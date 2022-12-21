# Conduit Connector Firebolt

## General

The Firebolt connector is one of Conduit plugins. It provides both, a source and a destination Firebolt connector.

In order to work with Firebolt the connector needs access to an [engine](https://docs.firebolt.io/working-with-engines/).
Engines are computed clusters that run database workloads.

If the engine you specified in the connector configuration is not running, the connector will start it for you.
And it will periodically check the engine status until it starts. If it takes more than 10 minutes connector will return
context cancelled error.
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

The Firebolt Destination takes a `sdk.Record` and parses it into a valid SQL query. 
When a SQL query is constructed the connector sends an HTTP request to your Firebolt engine endpoint.

### Table name

If a record contains a `firebolt.table` property in its metadata it will be inserted into that table,
otherwise it will fall back to use the table configured in the connector.
This way the Destination can support multiple tables in the same connector, as long as the user has proper access to those tables.

### Known limitations

Firebolt ([May 31, 2022 version](https://docs.firebolt.io/general-reference/release-notes-archive.html#may-31-2022))) doesn't 
currently support deletes and updates.
This means the destination supports only `OperationCreate` and `OperationSnapshot`operations types. 


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

| name              | description                                                                                                                                            | required  | example              |
|-------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|-----------|----------------------|
| `email`           | The email address of your Firebolt account.                                                                                                            | **true**  | email@test.com       |
| `password`        | The password of your Firebolt account.                                                                                                                 | **true**  | password             |
| `accountName`     | The account name of your Firebolt account.                                                                                                             | **true**  | `super_organization` |
| `engineName`      | The engine name of your Firebolt engine.                                                                                                               | **true**  | `my_super_engine`    |
| `db`              | The name of your database.                                                                                                                             | **true**  | test                 |
| `table`           | The name of a table in the database that the connector should read from, by default.                                                                   | **true**  | clients              |
| `orderingColumns` | Comma separated list of column names that records will use for ordering rows.                                                                          | **true**  | "id,name"            |
| `columns`         | Comma separated list of column names that should be included in the each Record's payload. By default: all columns.                                    | **false** | "id,name,age"        |
| `primaryKeys`     | Comma separated list of column names that records should use for their `key` fields.  See more: [Key handling](#key-handling).                         | **false** | "id,name"            |
| `batchSize`       | Size of batch. By default is 100. <b>Important:</b> Please, don’t update this variable after running the pipeline, as this will cause position issues. | **false** | "100"                |

### Snapshot iterator

The snapshot iterator starts getting data from the table using post request with select query with limit and offset 
ordering by `orderingColumn`.


If snapshot stops, it will continue works from last recorded row.

Example of position:

```json
{
  "RowNumber": 2
}
```

### Key handling

The connector builds `sdk.Record.Key` as `sdk.StructuredData`. The keys of this field consist of elements of
the `primaryKeys` configuration field. If `primaryKeys` is empty, the connector uses the primary keys of the specified
table; otherwise, if the table has no primary indexes, it uses the value of the `orderingColumns` field. The values
of `sdk.Record.Key` field are taken from `sdk.Payload.After` by the keys of this field.

### Known limitations

Firebolt ([May 31, 2022 version](https://docs.firebolt.io/general-reference/release-notes-archive.html#may-31-2022)) doesn't
currently support deletes and updates. Change Data Captured iterator not implemented.
