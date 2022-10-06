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

package iterator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"

	"github.com/conduitio-labs/conduit-connector-firebolt/client"
	"github.com/conduitio-labs/conduit-connector-firebolt/source/position"
)

const (
	// metadata related.
	metadataTable = "firebolt.table"
)

var (
	ErrNoKey = errors.New("key doesn't exist")
)

// SnapshotIterator snapshot iterator.
type SnapshotIterator struct {
	// firebolt client.
	client *client.Client
	// rowNumber - current number of row which iterator converts to record.
	rowNumber int
	// batchSize size of batch.
	batchSize int
	// currentBatch - rows in current batch from table.
	currentBatch []map[string]any
	// name of columns what iterator use for setting key in record.
	primaryKeys []string
	// list of columns to reading from table.
	columns []string
	// table which iterator read.
	table string
}

func NewSnapshotIterator(
	client *client.Client,
	batchSize int,
	table string,
	columns, primaryKeys []string,
) *SnapshotIterator {
	return &SnapshotIterator{
		client:      client,
		batchSize:   batchSize,
		primaryKeys: primaryKeys,
		columns:     columns,
		table:       table}
}

// Setup iterator.
func (i *SnapshotIterator) Setup(ctx context.Context, p sdk.Position) error {
	if p != nil {
		pos, err := position.ParseSDKPosition(p)
		if err != nil {
			return err
		}

		i.rowNumber = pos.RowNumber + 1
	}

	rows, err := i.client.GetRows(ctx, i.table, i.primaryKeys, i.columns, i.batchSize, i.rowNumber)
	if err != nil {
		sdk.Logger(ctx).Debug().Str("table", i.table).Strs("primaryKeys", i.primaryKeys).
			Strs("columns", i.columns).Int("batchSize", i.batchSize).
			Int("rowNumber", i.rowNumber).Msg("get rows parameters")

		return fmt.Errorf("get rows: %w", err)
	}

	i.currentBatch = rows

	return nil
}

// HasNext check ability to get next record.
func (i *SnapshotIterator) HasNext(ctx context.Context) (bool, error) {
	var err error

	if len(i.currentBatch) > 0 {
		return true, nil
	}

	i.currentBatch, err = i.client.GetRows(ctx, i.table, i.primaryKeys, i.columns, i.batchSize, i.rowNumber)
	if err != nil {
		return false, err
	}

	return len(i.currentBatch) > 0, nil
}

// Next get new record.
func (i *SnapshotIterator) Next(ctx context.Context) (sdk.Record, error) {
	pos := position.NewPosition(i.rowNumber)

	payload, err := json.Marshal(i.currentBatch[0])
	if err != nil {
		return sdk.Record{}, fmt.Errorf("marshal error : %w", err)
	}

	keysMap := make(map[string]any)
	for _, val := range i.primaryKeys {
		if _, ok := i.currentBatch[0][val]; !ok {
			return sdk.Record{}, fmt.Errorf("key %v, %w", val, ErrNoKey)
		}

		keysMap[val] = i.currentBatch[0][val]
	}

	p, err := pos.ToSDKPosition()
	if err != nil {
		return sdk.Record{}, err
	}

	if len(i.currentBatch) > 1 {
		i.currentBatch = i.currentBatch[1:]
	} else {
		i.currentBatch = nil
	}

	i.rowNumber++

	metadata := sdk.Metadata{metadataTable: i.table}
	metadata.SetCreatedAt(time.Now())

	record := sdk.Util.Source.NewRecordSnapshot(p, metadata,
		sdk.StructuredData(keysMap), sdk.RawData(payload))

	return record, nil
}

// Stop shutdown iterator.
func (i *SnapshotIterator) Stop(ctx context.Context) error {
	i.client.Close(ctx)

	return nil
}

// Ack check if record with position was recorded.
func (i *SnapshotIterator) Ack(ctx context.Context, rp sdk.Position) error {
	sdk.Logger(ctx).Debug().Str("position", string(rp)).Msg("got ack")

	return nil
}
