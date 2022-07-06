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

	"github.com/conduitio-labs/conduit-connector-firebolt/client"
	"github.com/huandu/go-sqlbuilder"
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
	table, orderingColumn string,
	columns []string,
	limit, offset int,
) ([]map[string]any, error) {
	q := buildGetDataQuery(table, orderingColumn, columns, offset, limit)
	resp, err := r.client.RunQuery(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("run query: %w", err)
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

func buildGetDataQuery(table, orderingColumn string, fields []string, offset, limit int) string {
	sb := sqlbuilder.NewSelectBuilder()

	if len(fields) == 0 {
		sb.Select("*")
	} else {
		sb.Select(fields...)
	}

	sb.From(table)
	sb.Offset(offset)
	sb.Limit(limit)
	sb.OrderBy(orderingColumn)

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
