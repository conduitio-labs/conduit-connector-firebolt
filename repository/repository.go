// Copyright © 2022 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package repository

import (
	"context"
	"fmt"

	"github.com/huandu/go-sqlbuilder"

	"github.com/conduitio-labs/conduit-connector-firebolt/client"
)

// FireboltClient defines a Firebolt client interface needed for the Repository.
type FireboltClient interface {
	RunQuery(ctx context.Context, query string) (*client.RunQueryResponse, error)
	Close(ctx context.Context)
}

// Repository is a firebolt repository.
type Repository struct {
	client FireboltClient
}

// New creates new instance of the Repository.
func New(client FireboltClient) *Repository {
	return &Repository{client: client}
}

// GetRows get rows from table.
func (r *Repository) GetRows(
	ctx context.Context,
	table string,
	columns []string,
	limit, offset int,
) ([]map[string]any, error) {
	q := buildGetDataQuery(table, columns, offset, limit)
	resp, err := r.client.RunQuery(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("run query: %w", err)
	}

	if err := prepareRunQueryResponseData(resp); err != nil {
		return nil, fmt.Errorf("prepare run query response data: %w", err)
	}

	return resp.Data, nil
}

// InsertRow inserts a row into a table, with the provided columns and values.
func (r *Repository) InsertRow(ctx context.Context, table string, columns []string, values []any) error {
	if len(columns) != len(values) {
		return ErrColumnsValuesLenMismatch
	}

	query, err := buildInsertQuery(table, columns, values)
	if err != nil {
		return fmt.Errorf("build insert query: %w", err)
	}

	_, err = r.client.RunQuery(ctx, query)
	if err != nil {
		return fmt.Errorf("run query: %w", err)
	}

	return nil
}

// Close closes the underlying Firebolt client.
func (r *Repository) Close(ctx context.Context) error {
	r.client.Close(ctx)

	return nil
}

func buildGetDataQuery(table string, fields []string, offset, limit int) string {
	sb := sqlbuilder.NewSelectBuilder()

	if len(fields) == 0 {
		sb.Select("*")
	} else {
		sb.Select(fields...)
	}

	sb.From(table)
	sb.Offset(offset)
	sb.Limit(limit)

	return sb.String()
}

// buildInsertQuery generates an SQL INSERT statement query,
// based on the provided table, columns and values.
func buildInsertQuery(table string, columns []string, values []any) (string, error) {
	sb := sqlbuilder.NewInsertBuilder()

	sb.InsertInto(table)
	sb.Cols(columns...)
	sb.Values(values...)

	sql, args := sb.BuildWithFlavor(sqlbuilder.PostgreSQL)

	query, err := sqlbuilder.PostgreSQL.Interpolate(sql, args)
	if err != nil {
		return "", fmt.Errorf("interpolate arguments to SQL: %w", err)
	}

	return query, nil
}

// prepareRunQueryResponseData converts resp.Data values to the appropriate Go types.
// For example if we query a Firebolt's table containing boolean values,
// Firebolt will return them as UInt8, but other connectors such as Postgres and Materialize
// don't accept UInt8 as boolean.
func prepareRunQueryResponseData(resp *client.RunQueryResponse) error {
	mutations := make(map[string]func(any) (any, error))

	for _, meta := range resp.Meta {
		// UInt8 is a Firebolt's representation of a boolean type.
		if meta.Type == client.MetaTypeUInt8 {
			mutations[meta.Name] = func(value any) (any, error) {
				// use float64 here cause Go unmarshals JSON's integer type into
				// an empty interface as float64.
				parsed, ok := value.(float64)
				if !ok {
					return nil, ErrCannotCastValueToFloat64
				}

				return parsed != 0, nil
			}
		}
	}

	var err error
	for _, row := range resp.Data {
		for key, value := range row {
			if _, ok := mutations[key]; !ok {
				continue
			}

			row[key], err = mutations[key](value)
			if err != nil {
				return fmt.Errorf("mutate %q key: %w", key, err)
			}
		}
	}

	return nil
}
