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

	"github.com/conduitio-labs/conduit-connector-firebolt/source/position"
)

type actionType string

const (
	// metadata related.
	metadataTable  = "table"
	metadataAction = "action"

	// actionType names.
	actionInsert actionType = "insertValue"
)

type SnapshotIterator struct {
	// repository firebolt repository.
	firebolt Repository
	// dbInfo database info.
	dbInfo dbInfo
	// index - current index of element in current batch which iterator converts to record.
	index int
	// offset - current offset, show what batch iterator uses, using in query to get currentBatch.
	offset int
	// batchSize size of batch.
	batchSize int
	// currentBatch - rows in current batch from table.
	currentBatch []map[string]any
}

type dbInfo struct {
	// name  repository db name.
	name string
	// engine repository engine.
	engine string
	// table - table in snowflake for getting currentBatch.
	table string
	// columns list of table columns for record payload
	// if empty - will get all columns.
	columns []string
	// Name of column what iterator use for setting key in record.
	primaryKey string
	// orderingColumn column for ordering for select query.
	orderingColumn string
}

// HasNext check ability to get next record.
func (i *SnapshotIterator) HasNext(ctx context.Context) (bool, error) {
	var err error

	if i.index < len(i.currentBatch) {
		return true, nil
	}

	if i.index >= i.batchSize {
		i.offset += i.batchSize
		i.index = 0
	}

	i.currentBatch, err = i.firebolt.GetData(ctx, i.dbInfo.table, i.dbInfo.engine, i.dbInfo.orderingColumn,
		i.batchSize, i.offset)
	if err != nil {
		return false, err
	}

	if len(i.currentBatch) == 0 || len(i.currentBatch) <= i.index {
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

	pos := position.NewPosition(i.index, i.offset)

	payload, err = json.Marshal(i.currentBatch[i.index])
	if err != nil {
		return sdk.Record{}, fmt.Errorf("marshal error : %w", err)
	}

	if _, ok := i.currentBatch[i.index][i.dbInfo.primaryKey]; !ok {
		return sdk.Record{}, ErrKeyIsNotExist
	}

	key := i.currentBatch[i.index][i.dbInfo.primaryKey]

	p, err := pos.ConvertToSDKPosition()
	if err != nil {
		return sdk.Record{}, err
	}

	i.index++

	return sdk.Record{
		Position: p,
		Metadata: map[string]string{
			metadataTable:  i.dbInfo.table,
			metadataAction: string(actionInsert),
		},
		CreatedAt: time.Now(),
		Key: sdk.StructuredData{
			i.dbInfo.primaryKey: key,
		},
		Payload: payload,
	}, nil
}

// Stop shutdown iterator.
func (i *SnapshotIterator) Stop(ctx context.Context) error {
	return i.firebolt.Close(ctx)
}
