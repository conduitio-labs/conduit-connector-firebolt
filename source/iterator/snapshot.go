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
	"fmt"
	"strings"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"

	"github.com/conduitio-labs/conduit-connector-firebolt/client"
	"github.com/conduitio-labs/conduit-connector-firebolt/source/position"
)

const (
	// metadata related.
	metadataTable = "firebolt.table"
)

// SnapshotIterator snapshot iterator.
type SnapshotIterator struct {
	// firebolt client.
	client *client.Client
	// index - current index of element in current batch which iterator converts to record.
	indexInBatch int
	// batchID - current batch id, show what batch iterator uses, using in query to get currentBatch.
	batchID int
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
			return nil
		}

		i.indexInBatch = pos.IndexInBatch + 1
		i.batchID = pos.BatchID
	}

	rows, err := i.client.GetRows(ctx, i.table, i.primaryKeys, i.columns, i.batchSize, i.batchID)
	if err != nil {
		sdk.Logger(ctx).Debug().Str("table", i.table).Str("primaryKey",
			strings.Join(i.primaryKeys, ",")).Str("columns", strings.Join(i.columns, ",")).
			Int("batchSize", i.batchSize).Int("batchID", i.batchID).Msg("get rows parameters")

		return fmt.Errorf("get rows: %w", err)
	}

	i.currentBatch = rows

	return nil
}

// HasNext check ability to get next record.
func (i *SnapshotIterator) HasNext(ctx context.Context) (bool, error) {
	var err error

	if i.indexInBatch < len(i.currentBatch) {
		return true, nil
	}

	if i.indexInBatch >= i.batchSize {
		i.batchID += i.batchSize
		i.indexInBatch = 0
	}

	i.currentBatch, err = i.client.GetRows(ctx, i.table, i.primaryKeys, i.columns, i.batchSize, i.batchID)
	if err != nil {
		return false, err
	}

	if len(i.currentBatch) == 0 || len(i.currentBatch) <= i.indexInBatch {
		return false, nil
	}

	return true, nil
}

// Next get new record.
func (i *SnapshotIterator) Next(ctx context.Context) (sdk.Record, error) {
	pos := position.NewPosition(i.indexInBatch, i.batchID)

	payload, err := json.Marshal(i.currentBatch[i.indexInBatch])
	if err != nil {
		return sdk.Record{}, fmt.Errorf("marshal error : %w", err)
	}

	keysMap := make(map[string]any)
	for _, val := range i.primaryKeys {
		if _, ok := i.currentBatch[i.indexInBatch][val]; !ok {
			return sdk.Record{}, fmt.Errorf("key %v, %w", val, ErrNoKey)
		}

		keysMap[val] = i.currentBatch[i.indexInBatch][val]
	}

	p, err := pos.ToSDKPosition()
	if err != nil {
		return sdk.Record{}, err
	}

	i.indexInBatch++

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
