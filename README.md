
# Data-Sifter

A CLI tool written in Rust to query data in CSV files using SQL.

### Use-cases

* Maybe you just want a generic way to view a CSV file on the command line.
* Perhaps an academic assignment requires using Excel or Google Sheets to create graphs of data. Why learn Excel or Sheets functions when you already know SQL?

### Solution

Data-Sifter takes a CSV file as data. It copies the data to a RDMS (currently PostgreSQL).

Then, it allows you to run a query against the data.

You have the option of displaying the query results either on the command line or written to another CSV file.

### Steps

1. Obtain a PostgreSQL connection URL. For this, you can create a free account with [CockroachDb](https://cockroachlabs.cloud/). The connection URL will look like `postgres://user:password@host:port/database`.
2. Enter the path to your CSV file.
3. Write a SQL query.
4. Decide whether you want the query results sent to STDOUT or written to another CSV file.

### License

Licensed under the Apache License 2.0. See the license file for more details.
