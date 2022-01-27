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

use std::borrow::Cow;
use csv_async::StringRecord;
use sqlx::{Column, Postgres, Row, Value, ValueRef};
use sqlx::postgres::{PgRow, PgValue};
use eyre::Result;
use futures_util::{StreamExt, stream::BoxStream};

pub(crate) type ResultSet<'r> = BoxStream<'r, Result<PgRow, sqlx::Error>>;

pub(crate) struct DecodedValue<'v> {
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
pub(crate) struct Schema {
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

    pub(crate) fn len(&self) -> usize {
        self.columns.len()
    }

    pub(crate) async fn create_or_recreate_table(&self, connection: &mut sqlx::pool::PoolConnection<Postgres>) -> Result<()> {
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

    pub(crate) fn column_names_joined_by_commas(&self) -> String {
        let mut output = String::new();
        for (index, column_name) in self.columns.iter().enumerate() {
            if index != 0 { output.push_str(", "); }
            output.push_str(column_name);
        }
        output
    }
}

pub(crate) struct QueryOutput<'r> {
    pub results: ResultSet<'r>
}

impl QueryOutput<'_> {
    pub async fn output_query_results<W>(mut self,
                                         csv_output: W) -> Result<bool>
        where W: async_std::io::Write + Unpin {

        let mut csv_output = csv_async::AsyncWriter::from_writer(csv_output);

        let first_row = match self.results.next().await {
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
        ).await?;

        // Write first row
        output_query_result_row(&first_row, &mut csv_output).await?;

        // Write remaining rows
        while let Some(row) = self.results.next().await {
            let row = row?;
            output_query_result_row(&row, &mut csv_output).await?;
        }
        Ok(true)
    }
}

async fn output_query_result_row<W>(row: &PgRow,
                                    csv_writer: &mut csv_async::AsyncWriter<W>) -> Result<()>
    where W: async_std::io::Write + Unpin {

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
    csv_writer.write_record(decoded_data).await?;
    Ok(())
}
