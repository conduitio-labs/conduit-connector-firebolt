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

package writer

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/conduitio-labs/conduit-connector-firebolt/client"
	"github.com/conduitio-labs/conduit-connector-firebolt/config"
	"github.com/conduitio-labs/conduit-connector-firebolt/repository"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

const (
	// metadata related.
	metadataTable = "table"
)

// Repository defines a repository interface needed for the Writer.
type Repository interface {
	InsertRow(ctx context.Context, table string, columns []string, values []any) error
	Close(ctx context.Context) error
}

// Writer implements a write logic for Firebolt destination.
type Writer struct {
	repository Repository
	table      string
}

// NewWriter creates new instance of the Writer.
func NewWriter(ctx context.Context, config config.Destination) (*Writer, error) {
	fireboltClient := client.New(ctx, config.EngineEndpoint, config.DB)

	err := fireboltClient.Login(ctx, config.Email, config.Password)
	if err != nil {
		return nil, fmt.Errorf("client login: %w", err)
	}

	return &Writer{
		repository: repository.New(fireboltClient),
		table:      config.Table,
	}, nil
}

// InsertRecord inserts a record into a Destination.
func (w *Writer) InsertRecord(ctx context.Context, record sdk.Record) error {
	table := w.getTableName(record.Metadata)

	payload, err := w.structurizeData(record.Payload)
	if err != nil {
		return fmt.Errorf("structurize payload: %w", err)
	}

	// if payload is empty we don't need to insert anything
	if payload == nil {
		return ErrEmptyPayload
	}

	columns, values := w.extractColumnsAndValues(payload)

	if err := w.repository.InsertRow(ctx, table, columns, values); err != nil {
		return fmt.Errorf("insert row: %w", err)
	}

	return nil
}

// Close closes the firebolt connection.
func (w *Writer) Close(ctx context.Context) error {
	return w.repository.Close(ctx)
}

// getTableName returns either the records metadata value for table
// or the default configured value for table.
func (w *Writer) getTableName(metadata map[string]string) string {
	tableName, ok := metadata[metadataTable]
	if !ok {
		return w.table
	}

	return strings.ToLower(tableName)
}

// structurizeData converts sdk.Data to sdk.StructuredData.
func (w *Writer) structurizeData(data sdk.Data) (sdk.StructuredData, error) {
	if data == nil || len(data.Bytes()) == 0 {
		return nil, nil
	}

	structuredData := make(sdk.StructuredData)
	if err := json.Unmarshal(data.Bytes(), &structuredData); err != nil {
		return nil, fmt.Errorf("unmarshal data into structured data: %w", err)
	}

	// convert keys to lower case
	structuredDataLower := make(sdk.StructuredData)
	for key, value := range structuredData {
		if parsedValue, ok := value.(map[string]any); ok {
			jsonValue, err := json.Marshal(parsedValue)
			if err != nil {
				return nil, fmt.Errorf("marshal map into json: %w", err)
			}

			structuredDataLower[strings.ToLower(key)] = string(jsonValue)

			continue
		}

		structuredDataLower[strings.ToLower(key)] = value
	}

	return structuredDataLower, nil
}

// extractColumnsAndValues turns the payload into slices of
// columns and values for upserting into Firebolt.
func (w *Writer) extractColumnsAndValues(payload sdk.StructuredData) ([]string, []any) {
	var (
		columns []string
		values  []any
	)

	for key, value := range payload {
		columns = append(columns, key)
		values = append(values, value)
	}

	return columns, values
}
