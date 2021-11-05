
# Data-Sifter

Rust program to re-organize data in CSV files using SQL.

### Problem

School assignments sometimes require using Excel or Google Sheets to create graphs of data. However, sometimes the data is not organized in a certain fashion, making it non-trivial to turn into the desired kind of graph.

Many people solve this problem by manipulating the data using Excel / Google Sheets functions. However, this requires learning

### Solution

Data-Sifter allows you to re-organize the data from Excel and Google Sheets. This way, you don't have to learn the niche Excel and Google Sheets functions.

Data-Sifter uses familiar SQL syntax, specifically from the PostgreSQL dialect. So, you can apply existing SQL knowledge without having to learn functions from Excel or Google Sheets.

## Usage

Export the data as CSV, run Data-Sifter, then re-import the CSV back to Excel / Google Sheets.

### Steps

1. Obtain a PostgreSQL connection URL. For this, you can create a free account with [CockroachDb](https://cockroachlabs.cloud/). The connection URL will look like `postgres://user:password@host:port/database`.
2. Enter the path to your CSV file.
3. Write a SQL query. It is the results of this query which will be re-exported to CSV.
4. The output CSV file is located in the same directory as the source CSV file.

### How it Works

1. The data in your CSV file is piped to the SQL database.
2. You write a SELECT query to select that data in the form you desire.
3. The result set from the query is written to the output CSV file.

## License

Licensed under the Apache License 2.0. See the license file for more details.
