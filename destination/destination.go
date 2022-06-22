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

package destination

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

// Repository defines a repository interface needed for the Destination.
type Repository interface {
	InsertRow(ctx context.Context, table string, columns []string, values []any) error
	Close(ctx context.Context) error
}

// Destination Firebolt Connector persists records to an Firebolt database.
type Destination struct {
	sdk.UnimplementedDestination

	config   config.Destination
	firebolt Repository
}

// New creates new instance of the Destination.
func New() sdk.Destination {
	return &Destination{}
}

// Configure parses and initializes the Destination config.
func (d *Destination) Configure(ctx context.Context, cfg map[string]string) error {
	configuration, err := config.ParseDestination(cfg)
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}

	d.config = configuration

	return nil
}

// Open makes sure everything is prepared to persists records.
func (d *Destination) Open(ctx context.Context) error {
	fireboltClient := client.New(ctx, d.config.EngineEndpoint, d.config.DB)

	err := fireboltClient.Login(ctx, d.config.Email, d.config.Password)
	if err != nil {
		return fmt.Errorf("client login: %w", err)
	}

	d.firebolt = repository.New(fireboltClient)

	return nil
}

// Write writes a record into a Destination.
func (d *Destination) Write(ctx context.Context, record sdk.Record) error {
	table := d.getTableName(record.Metadata)

	payload, err := d.structurizeData(record.Payload)
	if err != nil {
		return fmt.Errorf("structurize payload: %w", err)
	}

	// if payload is empty we don't need to insert anything
	if payload == nil {
		return ErrEmptyPayload
	}

	columns, values := d.extractColumnsAndValues(payload)

	if err := d.firebolt.InsertRow(ctx, table, columns, values); err != nil {
		return fmt.Errorf("insert row: %w", err)
	}

	return nil
}

// Teardown gracefully closes connections.
func (d *Destination) Teardown(ctx context.Context) error {
	return d.firebolt.Close(ctx)
}

// getTableName returns either the records metadata value for table
// or the default configured value for table.
func (d *Destination) getTableName(metadata map[string]string) string {
	tableName, ok := metadata[metadataTable]
	if !ok {
		return d.config.Table
	}

	return strings.ToLower(tableName)
}

// structurizeData converts sdk.Data to sdk.StructuredData.
func (d *Destination) structurizeData(data sdk.Data) (sdk.StructuredData, error) {
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
func (d *Destination) extractColumnsAndValues(payload sdk.StructuredData) ([]string, []any) {
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
