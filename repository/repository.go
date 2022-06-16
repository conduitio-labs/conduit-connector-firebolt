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
	"encoding/json"
	"fmt"

	"github.com/huandu/go-sqlbuilder"
)

// Firebolt - firebolt repository.
type Firebolt struct {
	client Client
}

func New(client Client) *Firebolt {
	return &Firebolt{client: client}
}

// GetRows get rows from table.
func (f *Firebolt) GetRows(
	ctx context.Context,
	table, orderingColumn string,
	columns []string,
	limit, offset int,
) ([]map[string]any, error) {
	q := buildGetDataQuery(table, orderingColumn, columns, offset, limit)
	body, err := f.client.RunQuery(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("run query: %w", err)
	}

	var resp selectQueryResponse
	if err = json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}

	return resp.Data, nil
}

func (f *Firebolt) Close(ctx context.Context) error {
	f.client.Close(ctx)

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
