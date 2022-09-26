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
	// name of column what iterator use for setting key in record.
	primaryKey string
	// list of columns to reading from table.
	columns []string
	// table which iterator read.
	table string
}

func NewSnapshotIterator(
	client *client.Client,
	batchSize int,
	columns []string,
	table, primaryKey string,
) *SnapshotIterator {
	return &SnapshotIterator{
		client:     client,
		batchSize:  batchSize,
		primaryKey: primaryKey,
		columns:    columns,
		table:      table}
}

// Setup iterator.
func (i *SnapshotIterator) Setup(ctx context.Context, p sdk.Position) error {
	var index, batchID int

	if p != nil {
		pos, err := position.ParseSDKPosition(p)
		if err != nil {
			return nil
		}

		index = pos.IndexInBatch + 1
		batchID = pos.BatchID
	}

	i.indexInBatch = index
	i.batchID = batchID

	rows, err := i.client.GetRows(ctx, i.table, i.primaryKey, i.columns, i.batchSize, i.batchID)
	if err != nil {
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

	i.currentBatch, err = i.client.GetRows(ctx, i.table, i.primaryKey, i.columns, i.batchSize, i.batchID)
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
	var (
		payload sdk.RawData
		err     error
	)

	pos := position.NewPosition(i.indexInBatch, i.batchID)

	payload, err = json.Marshal(i.currentBatch[i.indexInBatch])
	if err != nil {
		return sdk.Record{}, fmt.Errorf("marshal error : %w", err)
	}

	if _, ok := i.currentBatch[i.indexInBatch][i.primaryKey]; !ok {
		return sdk.Record{}, ErrKeyIsNotExist
	}

	key := i.currentBatch[i.indexInBatch][i.primaryKey]

	p, err := pos.ConvertToSDKPosition()
	if err != nil {
		return sdk.Record{}, err
	}

	i.indexInBatch++

	metadata := sdk.Metadata(map[string]string{metadataTable: i.table})
	metadata.SetCreatedAt(time.Now())

	record := sdk.Util.Source.NewRecordSnapshot(p, metadata,
		sdk.StructuredData{i.primaryKey: key}, payload)

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