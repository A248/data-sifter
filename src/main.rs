/*
 * Copyright © 2021 Anand Beh
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

use eyre::Result;
use std::path::{PathBuf, Path};
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use futures_util::{FutureExt, StreamExt, stream::FuturesUnordered};
use itertools::Itertools;
use futures_util::stream::BoxStream;
use sqlx::{Column, Error, Row, ValueRef, postgres::{PgRow, PgValue, Postgres}, Value};
use csv::StringRecord;
use std::iter::FromIterator;
use std::borrow::Cow;

fn main() -> core::result::Result<(), eyre::Error> {
    stable_eyre::install()?;

    let stdin = io::stdin();

    let mut io = IO {
        input: stdin.lock(),
        output: io::stdout()
    };
    let connection_url = io.prompt("Enter PostgreSQL connection URL")?;
    let mut app = App {
        io,
        connection_pool: sqlx::postgres::PgPool::connect_lazy(&connection_url)?
    };

    async_std::task::block_on(app.run())
}

struct IO<R> where R: io::BufRead {
    input: R,
    output: io::Stdout
}

impl<R> IO<R> where R: io::BufRead {

    fn write_output(&mut self, line: &str) -> Result<()> {
        self.output.write(line.as_bytes())?;
        self.output.write(b"\n")?;
        Ok(self.output.flush()?)
    }

    fn prompt(&mut self, question: &str) -> Result<String> {
        self.write_output(question)?;

        let mut buffer = String::new();
        self.input.read_line(&mut buffer)?;
        buffer.pop(); // Remove trailing \n
        Ok(buffer)
    }
}

struct App<R> where R: io::BufRead {
    io: IO<R>,
    connection_pool: sqlx::postgres::PgPool
}

impl<R> App<R> where R: io::BufRead {

    async fn run(&mut self) -> Result<()> {

        let csv_input = self.io.prompt("Enter CSV dataset file")?;
        let csv_input = PathBuf::from(csv_input).canonicalize()?;
        self.read_csv(&csv_input).await?;

        let csv_output = {
            let mut csv_output = csv_input.into_os_string();
            csv_output.push(".data-sifter-output.csv");
            PathBuf::from(csv_output)
        };
        if csv_output.exists() {
            eyre::bail!("Delete existing file first")
        }

        let query = self.io.prompt("Enter SQL query")?;

        let mut connection = self.connection_pool.acquire().await?;
        let query = sqlx::query(&query).fetch(&mut connection);

        let any_results = self.write_csv(query, &csv_output).await?;
        self.io.write_output(
            if any_results { "Wrote output CSV" } else { "Empty result set"})?;

        Ok(())
    }

    async fn read_csv(&mut self, csv_input: &Path) -> Result<()> {
        assert!(csv_input.exists(), "Specified CSV file {:?} does not exist", csv_input);

        let csv_input = io::BufReader::new(File::open(csv_input)?);
        let mut csv_input = csv::Reader::from_reader(csv_input);

        let schema = &{
            let first_record = csv_input.headers()?;
            let schema = Schema::from(first_record);

            let mut connection = self.connection_pool.acquire().await?;
            schema.create_table(&mut connection).await?;

            schema
        };

        let futures = FuturesUnordered::new();
        for record in csv_input.records() {
            let record = record?;
            assert_eq!(schema.len(), record.len(), "Field list must match");

            let connection = self.connection_pool.acquire();
            let query = connection.map(|connection| {
                async move {
                    let mut connection = connection?;
                    // INSERT INTO data (col1, col2) VALUES ('val1', 'val2')
                    let query = format!(
                        "INSERT INTO data ({}) VALUES ({})",
                        schema.column_names_joined_by_commas(),
                        record.iter().map(|value| format!("'{}'", value)).join(", "));
                    let query_result = sqlx::query(&query).execute(&mut connection).await?;
                    Ok::<_, eyre::Report>(query_result)
                }
            });
            futures.push(query.flatten());
        }
        for result in futures.collect::<Vec<_>>().await {
            result?;
        }
        Ok(())
    }

    async fn write_csv(&self,
                       mut query: BoxStream<'_, Result<PgRow, Error>>,
                       csv_output: &Path) -> Result<bool> {

        let csv_output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(csv_output);
        let mut csv_output = csv::Writer::from_writer(csv_output?);

        let first_row = match query.next().await {
            Some(row) => row?,
            None => return Ok(false)
        };

        // Write header first
        csv_output.write_record(
            first_row
                .columns()
                .iter()
                .map(|column| String::from(column.name()))
                .collect::<Vec<_>>()
        )?;

        // Write first row
        self.write_row_to_csv(&first_row, &mut csv_output)?;

        // Write remaining rows
        let results = query.map(|row| {
            let row = row?;
            self.write_row_to_csv(&row, &mut csv_output)
        });
        for result in results.collect::<Vec<_>>().await {
            result?;
        }
        Ok(true)
    }

    fn write_row_to_csv<'i, W>(&self,
                               row: &PgRow,
                               csv_writer: &mut csv::Writer<W>) -> Result<()>
        where W: io::Write {

        let mut raw_row_data = Vec::new();
        for index in 0..row.len() {

            let column_data = row.try_get_raw(index)?;
            let column_data: PgValue = ValueRef::to_owned(&column_data);
            raw_row_data.push(column_data);
        }

        // Use 2 loops so that PgValue's remain in scope
        let mut decoded_data = Vec::new();
        for column_data in raw_row_data.iter() {
            decoded_data.push(DecodedValue::from(column_data));
        }
        csv_writer.write_record(decoded_data)?;
        Ok(())
    }
}

struct DecodedValue<'v> {
    data: Cow<'v, str>
}

impl<'v> From<&'v PgValue> for DecodedValue<'v> {
    fn from(value: &'v PgValue) -> Self {
        let data = {
            if let Ok(decoded) = value.try_decode::<&str>() {
                Cow::Borrowed(decoded)
            } else if let Ok(decoded) = value.try_decode::<i32>() {
                Cow::Owned(decoded.to_string())
            } else if let Ok(decoded) = value.try_decode::<i64>() {
                Cow::Owned(decoded.to_string())
            } else if let Ok(decoded) = value.try_decode::<f32>() {
                Cow::Owned(decoded.to_string())
            } else if let Ok(decoded) = value.try_decode::<f64>() {
                Cow::Owned(decoded.to_string())
            } else if let Ok(decoded) = value.try_decode::<rust_decimal::Decimal>() {
                Cow::Owned(decoded.to_string())
            } else {
                panic!("No determinable value for type info {:?}", value.type_info())
            }
        };
        Self { data }
    }
}

impl<'v> AsRef<[u8]> for DecodedValue<'v> {
    fn as_ref(&self) -> &[u8] {
        match &self.data {
            Cow::Borrowed(borrowed) => borrowed.as_bytes(),
            Cow::Owned(owned) => owned.as_bytes()
        }
    }
}

#[derive(Debug)]
struct Schema {
    columns: Vec<Box<str>>
}

impl<'s> From<&'s StringRecord> for Schema {
    fn from(record: &'s StringRecord) -> Self {
        record.iter().collect()
    }
}

impl<'s> FromIterator<&'s str> for Schema {
    fn from_iter<T: IntoIterator<Item=&'s str>>(iter: T) -> Self {
        Self {
            columns: iter.into_iter().map(Box::from).collect()
        }
    }
}

impl Schema {

    fn len(&self) -> usize {
        self.columns.len()
    }

    async fn create_table(&self, connection: &mut sqlx::pool::PoolConnection<Postgres>) -> Result<()> {
        sqlx::query("DROP TABLE IF EXISTS data").execute(&mut *connection).await?;

        let mut create_table_query = String::from("CREATE TABLE data (");
        for (index, column_name) in self.columns.iter().enumerate() {
            if index != 0 { create_table_query.push_str(", "); }
            create_table_query.push_str(column_name);
            create_table_query.push_str(" VARCHAR(256) NOT NULL");
        }
        create_table_query.push_str(")");

        sqlx::query(&create_table_query).execute(&mut *connection).await?;
        Ok(())
    }

    fn column_names_joined_by_commas(&self) -> String {
        let mut output = String::new();
        for (index, column_name) in self.columns.iter().enumerate() {
            if index != 0 { output.push_str(", "); }
            output.push_str(column_name);
        }
        output
    }
}


