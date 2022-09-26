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
