# Conduit Connector Firebolt

## General

The Firebolt connector is one of Conduit plugins. It provides both, a source and a destination Firebolt connector.

In order to work with Firebolt the connector needs access to an [engine](https://docs.firebolt.io/working-with-engines/).
Engines are computed clusters that run database workloads.

If the engine you specified in the connector configuration is not running, the connector will start it for you.
And it will periodically check the engine status until it starts.
The process of starting the engine may take some time, the connector at this moment will not be able to write or read data.

### Prerequisites

- [Go](https://go.dev/) 1.18
- (optional) [golangci-lint](https://github.com/golangci/golangci-lint) 1.45.2
- Firebolt ([May 31, 2022 version](https://docs.firebolt.io/general-reference/release-notes-archive.html#may-31-2022))

### How to build it

Run `make`

### Testing

Run `make test` to run all the unit and integration tests. The integration tests require `FIREBOLT_EMAIL`, `FIREBOLT_PASSWORD`, `FIREBOLT_DATABASE_ENGINE`, `FIREBOLT_DB` environment variables to be set.

## Destination

The Firebolt Destination takes a `sdk.Record` and parses it into a valid SQL query. When a SQL query is constructed the connector sends an HTTP request to your Firebolt engine endpoint. The Destination is designed to handle different payloads and keys.

### Table name

If a record contains a `table` property in it’s metadata it will be inserted into that table,
otherwise it will fall back to use the table configured in the connector.
This way the Destination can support multiple tables in the same connector, as long as the user has proper access to those tables.

### Known limitations

Firebolt ([May 31, 2022 version](https://docs.firebolt.io/general-reference/release-notes-archive.html#may-31-2022))) doesn't 
currently support deletes and updates.
This means that regardless of the value of the `action` property the connector will insert data. There's no upsert mechanism as well.

It also not possible to create `UNIQUE` constraint. There may be duplicates even if there's a primary key. 

### Configuration

| name          | description                                                                         | required | example              |
| ------------- | ----------------------------------------------------------------------------------- | -------- | -------------------- |
| `email`       | The email address of your Firebolt account.                                         | **true** | `email@test.com`     |
| `password`    | The password of your Firebolt account.                                              | **true** | `some_password`      |
| `accountName` | The account name of your Firebolt account.                                          | **true** | `super_organization` |
| `engineName`  | The engine name of your Firebolt engine.                                            | **true** | `my_super_engine`    |
| `db`          | The name of your database.                                                          | **true** | `some_database`      |
| `table`       | The name of a table in the database that the connector should write to, by default. | **true** | `some_table`         |

## Source

### Configuration

The config passed to `Configure` can contain the following fields.

| name             | description                                                                                                                                           | required  | example              |
| ---------------- |-------------------------------------------------------------------------------------------------------------------------------------------------------| --------- | -------------------- |
| `email`          | The email address of your Firebolt account.                                                                                                           | **true**  | email@test.com       |
| `password`       | The password of your Firebolt account.                                                                                                                | **true**  | password             |
| `accountName`    | The account name of your Firebolt account.                                                                                                            | **true**  | `super_organization` |
| `engineName`     | The engine name of your Firebolt engine.                                                                                                              | **true**  | `my_super_engine`    |
| `db`             | The name of your database.                                                                                                                            | **true**  | test                 |
| `table`          | The name of a table in the database that the connector should read from, by default.                                                                  | **true**  | clients              |
| `columns`        | Comma separated list of column names that should be included in the each Record's payload. By default: all columns.                                   | **false** | "id,name,age"        |
| `primaryKey`     | The name of the column that records should use for their `key` fields.                                                                                          | **true**  | "id"                 |
| `batchSize`      | Size of batch. By default is 100. <b>Important:</b> Please, don’t update this variable after running the pipeline, as this will cause position issues. | **false** | "100"                |

### Snapshot iterator

The snapshot iterator starts getting data from the table using post request with select query with limit and offset. 
For example `select * from {table} limit 20 offset 0`. Batch size
is configurable, offset value is zero for the first time. Iterator saves rows from table to `currentBatch` slice variable.
The Iterator `HasNext` method checks if the next element exists in the currentBatch using variable `index` and,
if necessary, changes the `offset` and runs a select query to get a new data with the new offset. Method `Next` 
gets next element and converts it to `Record` sets metadata variable table.
Set record metadata variable `action` - `insert`, increases `index`.

Example of position:

```json
{
  "IndexInBatch": 2,
  "BatchID": 10
}
```

If snapshot stops, it will parse a position from the last record. The position has fields: `IndexInBatch` - 
this is index of the element in the current batch, the last element that has been recorded; `BatchID` -
shows the last value offset that iterator used to get data query. Iterator runs a query to get the data
from the table with the `batchSize` and the offset that was taken from the position. The `Index` value will be
the Element plus one, as the iterator tries to find the next element in the current batch. If `index` > `batchSize` 
the iterator will change `BatchID` to the next one and set `index` to zero.

For example, we get snapshot position in `Open` function:

```json
{
  "IndexInBatch": 4,
  "BatchID": 20
}
```

The last recorded position has `BatchID` = 20, which means that the iterator last executed a query with an 
offset value of 20, the iterator will execute the same query with the same offset value.
The iterator gets a batch of rows from a table. `IndexInBatch` is the last element index in 
this batch that was processed. The iterator looks up the next element in the batch (with index = 5)
and converts it into a record.

### Known limitations

Firebolt ([May 31, 2022 version](https://docs.firebolt.io/general-reference/release-notes-archive.html#may-31-2022)) doesn't
currently support deletes and updates. Change Data Captured iterator not implemented.
