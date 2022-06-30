# Conduit Connector Firebolt

## General

The Firebolt connector is one of Conduit plugins. It provides both, a source and a destination Firebolt connector.

### Prerequisites

- [Go](https://go.dev/) 1.18
- (optional) [golangci-lint](https://github.com/golangci/golangci-lint) 1.45.2
- Firebolt ([May 11, 2022 version](https://docs.firebolt.io/general-reference/release-notes-archive.html#may-11-2022))

### How to build it

Run `make`

### Testing

Run `make test` to run all the unit and integration tests. The integration tests require `FIREBOLT_EMAIL`, `FIREBOLT_PASSWORD`, `FIREBOLT_DATABASE_ENGINE`, `FIREBOLT_DB` environment variables to be set.

## Destination

The Firebolt Destination takes a `sdk.Record` and parses it into a valid SQL query. When a SQL query is constructed the connector sends an HTTP request to your preconfigured Firebolt engine endpoint. The Destination is designed to handle different payloads and keys.

### Table name

If a record contains a `table` property in its metadata it will be inserted in that table, otherwise it will fall back to use the table configured in the connector. This way the Destination can support multiple tables in the same connector, provided the user has proper access to those tables.

### Known limitations

Firebolt ([May 11, 2022 version](https://docs.firebolt.io/general-reference/release-notes-archive.html#may-11-2022)) doesn't currently support deletes and updates. This means that regardless of the value of an `action` property the connector will insert data. There's no upsert mechanism as well.

### Configuration

| name             | description                                                                         | required | example                                                  |
| ---------------- | ----------------------------------------------------------------------------------- | -------- | -------------------------------------------------------- |
| `email`          | The email address of your Firebolt account.                                         | **true** | `email@test.com`                                         |
| `password`       | The password of your Firebolt account.                                              | **true** | `some_password`                                          |
| `engineEndpoint` | The engine endpoint being used to query your database.                              | **true** | `test-1-general-purpose.myorg.us-east-1.app.firebolt.io` |
| `db`             | The name of your database.                                                          | **true** | `some_database`                                          |
| `table`          | The name of a table in the database that the connector should write to, by default. | **true** | `some_table`                                             |
