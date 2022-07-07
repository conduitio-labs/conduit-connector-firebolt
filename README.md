# Conduit Connector Firebolt

## General

The Firebolt connector is one of Conduit plugins. It provides both, a source and a destination Firebolt connector.

In order to work with Firebolt the connector needs access to an [engine](https://docs.firebolt.io/working-with-engines/). Engines are computed clusters that run database workloads.

If the engine you specified in the connector configuration is not running, the connector will start it for you. And it will periodically check the engine status until it starts. The process of starting the engine may take some time, the connector at this moment will not be able to write or read data.

### Prerequisites

- [Go](https://go.dev/) 1.18
- (optional) [golangci-lint](https://github.com/golangci/golangci-lint) 1.45.2
- Firebolt ([May 11, 2022 version](https://docs.firebolt.io/general-reference/release-notes-archive.html#may-11-2022))

### How to build it

Run `make`

### Testing

Run `make test` to run all the unit and integration tests. The integration tests require `FIREBOLT_EMAIL`, `FIREBOLT_PASSWORD`, `FIREBOLT_DATABASE_ENGINE`, `FIREBOLT_DB` environment variables to be set.

## Destination

The Firebolt Destination takes a `sdk.Record` and parses it into a valid SQL query. When a SQL query is constructed the connector sends an HTTP request to your Firebolt engine endpoint. The Destination is designed to handle different payloads and keys.

### Table name

If a record contains a `table` property in its metadata it will be inserted in that table, otherwise it will fall back to use the table configured in the connector. This way the Destination can support multiple tables in the same connector, provided the user has proper access to those tables.

### Known limitations

Firebolt ([May 11, 2022 version](https://docs.firebolt.io/general-reference/release-notes-archive.html#may-11-2022)) doesn't currently support deletes and updates. This means that regardless of the value of an `action` property the connector will insert data. There's no upsert mechanism as well.

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
| ---------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- | --------- | -------------------- |
| `email`          | The email address of your Firebolt account.                                                                                                           | **true**  | email@test.com       |
| `password`       | The password of your Firebolt account.                                                                                                                | **true**  | password             |
| `accountName`    | The account name of your Firebolt account.                                                                                                            | **true**  | `super_organization` |
| `engineName`     | The engine name of your Firebolt engine.                                                                                                              | **true**  | `my_super_engine`    |
| `db`             | The name of your database.                                                                                                                            | **true**  | test                 |
| `table`          | The name of a table in the database that the connector should read from, by default.                                                                  | **true**  | clients              |
| `orderingColumn` | Column which using for ordering in select query . Usually it is can be pk or timestamp column                                                         | **true**  | created_date         |
| `columns`        | Comma separated list of column names that should be included in each Record's payload. By default: all columns.                                       | **false** | "id,name,age"        |
| `primaryKey`     | Column name that records should use for their `Key` fields.                                                                                           | **true**  | "id"                 |
| `batchSize`      | Size of batch. By default is 100. <b>Important:</b> Please don't update this variable after the pipeline starts, it will cause problem with position. | **false** | "100"                |

### Snapshot iterator

The snapshot iterator starts getting data from the table using post request with select query with limit and offset and
ordering by ordering column. For example `select * from {table} order by {orderingColumn} limit 20 offset 0`. Batch size
is configurable, offset value is zero for the first time. Iterator save rows from table to `currentBatch` slice variable.
Iterator `HasNext` method checks if next element exist in currentBatch using variable index and if it is needed changes offset
and runs select query to get new data with new offset. Method `Next` gets next element and converts it to `Record` sets metadata variable table,
set metadata variable action - `insert`, increases index.

Example of position:

```json
{
  "IndexInBatch": 2,
  "BatchID": 10
}
```

If snapshot stops, it will parse position from last record. Position has fields: `IndexInBatch` - it is the index of element
in current batch, this last element what was recorded, `BatchID` - shows the last value offset what iterator used for
getting data query. Iterator runs query to get data from table with `batchSize` and `offset` which was got from
position. `index` value will be `Element` increased by one, because iterator tries to find next element in current batch.
If `index` > `batchSize` iterator will change `BatchID` to next and set `index` zero.

For example, we get snapshot position in `Open` function:

```json
{
  "IndexInBatch": 4,
  "BatchID": 20
}
```

Last recorded position has `BatchID` = 20, it is means iterator did last time query with `offset` value 20, iterator will
do the same query with the same `offset` value. Iterator gets batch with rows from table. `IndexInBatch` it is last
index for element in this batch what was processed. Iterator looks for next element in batch (with index = 5) and convert
it to record.

### Known limitations

Firebolt ([May 11, 2022 version](https://docs.firebolt.io/general-reference/release-notes-archive.html#may-11-2022)) doesn't
currently support deletes and updates. Change Data Captured iterator not implemented.
