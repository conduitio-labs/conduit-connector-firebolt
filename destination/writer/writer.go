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
	"reflect"
	"strings"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"

	"github.com/conduitio-labs/conduit-connector-firebolt/client"
)

const (
	// metadata related.
	metadataTable = "firebolt.table"

	// column types.
	typeTimestamp = "timestamp"
	typeDate      = "date"
)

var (
	// time layouts.
	layouts = []string{time.RFC3339, time.RFC3339Nano, time.Layout, time.ANSIC, time.UnixDate, time.RubyDate,
		time.RFC822, time.RFC822Z, time.RFC850, time.RFC1123, time.RFC1123Z, time.RFC3339, time.RFC3339,
		time.RFC3339Nano, time.Kitchen, time.Stamp, time.StampMilli, time.StampMicro, time.StampNano}
)

// Writer implements write logic for Firebolt destination.
type Writer struct {
	client      *client.Client
	table       string
	columnTypes map[string]string
}

// NewWriter creates new instance of the Writer.
func NewWriter(client *client.Client, table string) (*Writer, error) {
	return &Writer{
		client: client,
		table:  table,
	}, nil
}

func (w *Writer) SetColumnTypes(cl map[string]string) {
	w.columnTypes = cl
}

// InsertRecord inserts a record into a Destination.
func (w *Writer) InsertRecord(ctx context.Context, record sdk.Record) error {
	table := w.getTableName(record.Metadata)

	payload, err := w.structurizeData(record.Payload.After)
	if err != nil {
		return fmt.Errorf("structurize payload: %w", err)
	}

	// if payload is empty we don't need to insert anything
	if payload == nil {
		return ErrEmptyPayload
	}

	payload, err = w.convertPayload(payload)
	if err != nil {
		return fmt.Errorf("convert payload: %w", err)
	}

	columns, values := w.extractColumnsAndValues(payload)

	if err = w.client.InsertRow(ctx, table, columns, values); err != nil {
		return fmt.Errorf("insert row: %w", err)
	}

	return nil
}

// Close closes the firebolt connection.
func (w *Writer) Close(ctx context.Context) error {
	w.client.Close(ctx)

	return nil
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

	// Firebolt API returns columns names as lower case, it is converts keys to lower case too.
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

// convertPayload converts a sdk.StructureData values to a proper database types.
func (w *Writer) convertPayload(data sdk.StructuredData) (sdk.StructuredData, error) {
	result := make(sdk.StructuredData, len(data))

	for key, value := range data {
		if value == nil {
			result[key] = value

			continue
		}

		switch reflect.TypeOf(value).Kind() {
		case reflect.Map, reflect.Slice:
			bs, err := json.Marshal(value)
			if err != nil {
				return nil, fmt.Errorf("marshal: %w", err)
			}

			result[key] = string(bs)

			continue
		}

		switch w.columnTypes[strings.ToLower(key)] {
		case typeDate:
			v, ok := value.(time.Time)
			if ok {
				result[key] = v.Format("2006-01-02")

				continue
			}

			valueStr, ok := value.(string)
			if ok {
				timeValue, err := w.parseToTime(valueStr)
				if err != nil {
					return nil, fmt.Errorf("convert value to time.Time: %w", err)
				}

				result[key] = timeValue.Format("2006-01-02")

				continue
			}

			return nil, ErrInvalidTypeForDateColumn
		case typeTimestamp:
			v, ok := value.(time.Time)
			if ok {
				// firebolt date type support this format
				// https://docs.firebolt.io/general-reference/data-types.html#date-and-time
				result[key] = v.Format("2006-01-02 15:04:05")

				continue
			}

			valueStr, ok := value.(string)
			if ok {
				timeValue, err := w.parseToTime(valueStr)
				if err != nil {
					return nil, fmt.Errorf("convert value to time.Time: %w", err)
				}

				// firebolt timestamp type support this format
				// https://docs.firebolt.io/general-reference/data-types.html#timestamp
				result[key] = timeValue.Format("2006-01-02 15:04:05")

				continue
			}

			return nil, ErrInvalidTypeForTimestampColumn
		default:
			result[key] = value
		}
	}

	return result, nil
}

func (w *Writer) parseToTime(val string) (time.Time, error) {
	for _, l := range layouts {
		timeValue, err := time.Parse(l, val)
		if err != nil {
			continue
		}

		return timeValue, nil
	}

	return time.Time{}, fmt.Errorf("%s - %w", val, ErrInvalidTimeLayout)
}
