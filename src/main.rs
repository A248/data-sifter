/*
 * data-sifter
 * Copyright © 2022 Anand Beh
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

mod database;
mod config;

use eyre::Result;
use async_std::path::PathBuf;
use async_std::{io, fs::{File, OpenOptions}};
use std::os::unix::ffi::OsStrExt;
use async_std::task::{self, JoinHandle};
use futures_lite::{AsyncBufReadExt, AsyncWriteExt};
use futures_util::{StreamExt, stream::FuturesUnordered};
use itertools::Itertools;
use sqlx::PgPool;
use crate::config::Config;
use crate::database::QueryOutput;

fn main() -> core::result::Result<(), eyre::Error> {
    use std::env;

    if let Err(env::VarError::NotPresent) = env::var("RUST_BACKTRACE") {
        env::set_var("RUST_BACKTRACE", "1");
        println!("Enabled RUST_BACKTRACE");
    }
    stable_eyre::install()?;

    let io = IO {
        input: io::BufReader::new(io::stdin()),
        output: io::stdout()
    };
    task::block_on(async_main(io))
}

async fn async_main<R>(mut io: IO<R>) -> Result<()> where R: io::BufRead + Unpin {
    let config_path = Config::default_path(&mut io).await?;
    let config = Config::load(&config_path).await?;
    let config = match config {
        None => {
            Config::default().write_to(&config_path).await?;
            io.write_output(
                "The default config has been created. Please configure and then restart data-sifter"
            ).await?;
            return Ok(());
        },
        Some(config) => config
    };
    let Config { postgres_url } = config;
    let mut app = App {
        io,
        connection_pool: sqlx::postgres::PgPool::connect_lazy(&postgres_url)?
    };
    app.run().await
}

pub struct IO<R> where R: io::BufRead + Unpin {
    input: R,
    output: io::Stdout
}

impl<R> IO<R> where R: io::BufRead + Unpin {

    async fn write_output(&mut self, line: &str) -> Result<()> {
        self.output.write(line.as_bytes()).await?;
        self.output.write(b"\n").await?;
        Ok(self.output.flush().await?)
    }

    async fn prompt(&mut self, question: &str) -> Result<String> {
        self.write_output(question).await?;

        let mut buffer = String::new();
        self.input.read_line(&mut buffer).await?;
        buffer.pop(); // Remove trailing \n
        Ok(buffer)
    }
}

struct App<R> where R: io::BufRead + Unpin {
    io: IO<R>,
    connection_pool: sqlx::postgres::PgPool
}

impl<R> App<R> where R: io::BufRead + Unpin {

    async fn run(&mut self) -> Result<()> {
        let csv_input = self.io.prompt("Enter CSV dataset file").await?;
        let csv_input = PathBuf::from(csv_input).canonicalize().await?;

        // Spawn a separate task so that the CSV is written in the background
        let csv_to_database: JoinHandle<Result<()>>= {
            let pool = self.connection_pool.clone();
            let csv_input = csv_input.clone();
            task::spawn(read_csv_then_write_to_database(pool, csv_input))
        };

        let query;
        let mut connection;
        let _results;
        let query = {
            query = self.io.prompt("Enter SQL query").await?;

            // Wait for the data to be ready before executing a query
            csv_to_database.await?;

            connection = self.connection_pool.acquire().await?;
            _results = sqlx::query(&query).fetch(&mut connection);
            QueryOutput {
                results: _results
            }
        };

        let next = self.io.prompt("
        What would you like to do with this query?
        'csv' - Query the dataset and output the results to CSV.
        'show' - Query the dataset and show the results here.
        ").await?;
        match next.as_str() {
            "csv" => {
                let csv_file = {
                    let mut csv_file = csv_input.into_os_string();
                    csv_file.push(".data-sifter-output.csv");
                    PathBuf::from(csv_file)
                };
                if csv_file.exists().await {
                    eyre::bail!("Delete existing file first")
                }
                let any_results = {
                    let csv_file = OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(&csv_file).await?;
                    query.output_query_results(csv_file).await?
                };
                if any_results {
                    let csv_file = csv_file.canonicalize().await?.into_os_string();
                    let stdout = &mut self.io.output;
                    stdout.write_all(b"Wrote output CSV to ").await?;
                    stdout.write_all(csv_file.as_bytes()).await?;
                    stdout.flush().await?;
                } else {
                    self.io.write_output("No results").await?;
                }
                Ok(())
            },
            "show" => {
                let any_results = query.output_query_results(&mut self.io.output).await?;
                if !any_results {
                    self.io.write_output("No results").await?;
                }
                Ok(())
            }
            unknown_option => eyre::bail!("Invalid option: {}", unknown_option)
        }
    }
}

async fn read_csv_then_write_to_database(pool: PgPool, csv_input: PathBuf) -> Result<()> {
    use crate::database::Schema;

    assert!(csv_input.exists().await, "Specified CSV file {:?} does not exist", csv_input);

    let csv_input = io::BufReader::new(File::open(csv_input).await?);
    let mut csv_input = csv_async::AsyncReader::from_reader(csv_input);

    let schema = &{
        let first_record = csv_input.headers().await?;
        let schema = Schema::from(first_record);

        let mut connection = pool.acquire().await?;
        schema.create_or_recreate_table(&mut connection).await?;

        schema
    };
    let column_names = schema.column_names_joined_by_commas();

    let futures = FuturesUnordered::new();
    let mut records = csv_input.records();
    while let Some(record) = records.next().await {
        let record = record?;
        assert_eq!(schema.len(), record.len(), "Field list must match");

        // Pool is an Arc
        let pool = pool.clone();
        let column_names = column_names.clone();

        futures.push(async move {
            let mut connection = pool.acquire().await?;
            // INSERT INTO data (col1, col2) VALUES ('val1', 'val2')
            let query = format!(
                "INSERT INTO data ({}) VALUES ({})",
                column_names,
                record.iter().map(|value| format!("'{}'", value)).join(", "));
            sqlx::query(&query).execute(&mut connection).await?;
            Ok::<_, eyre::Report>(())
        });
    }
    for result in futures.collect::<Vec<_>>().await {
        result?;
    }
    Ok(())
}


