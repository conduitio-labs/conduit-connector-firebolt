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

package config

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/conduitio-labs/conduit-connector-firebolt/config/validator"
)

const (
	// KeyColumns is a config name for a list of columns.
	KeyColumns string = "columns"
	// KeyBatchSize is a config name for a batch size.
	KeyBatchSize string = "batchSize"
	// KeyPrimaryKey is a config name for a primary key.
	KeyPrimaryKey string = "primaryKey"

	// defaultBatchSize is a default batch size.
	defaultBatchSize = 100
)

// Source holds source-related configurable values.
type Source struct {
	General

	// List of columns from table, by default read all columns.
	Columns []string
	// BatchSize - size of batch.
	BatchSize int `validate:"gte=1,lte=100"`
	// PrimaryKeys - columns names that records should use for their `Key` fields.
	PrimaryKeys []string `validate:"required"`
}

// ParseSource attempts to parse plugins.Config into a Source struct.
func ParseSource(cfg map[string]string) (Source, error) {
	general, err := ParseGeneral(cfg)
	if err != nil {
		return Source{}, fmt.Errorf("parse general config: %w", err)
	}

	source := Source{
		General:   general,
		BatchSize: defaultBatchSize,
	}

	if colsRaw := cfg[KeyColumns]; colsRaw != "" {
		source.Columns = strings.Split(colsRaw, ",")
	}

	if colsRaw := cfg[KeyPrimaryKey]; colsRaw != "" {
		source.PrimaryKeys = strings.Split(colsRaw, ",")
	}

	if cfg[KeyBatchSize] != "" {
		batchSize, er := strconv.Atoi(cfg[KeyBatchSize])
		if er != nil {
			return Source{}, errors.New(`"batchSize" config value must be int`)
		}

		source.BatchSize = batchSize
	}

	if err = validator.Validate(source); err != nil {
		return Source{}, err
	}

	return source, nil
}
