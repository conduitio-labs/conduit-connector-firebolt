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
	"strconv"
	"strings"
)

const (
	KeyEmail          string = "email"
	KeyPassword       string = "password"
	KeyEngineEndpoint string = "engineEndpoint"
	KeyDB             string = "db"
	KeyTable          string = "table"
	KeyColumns        string = "columns"
	KeyPrimaryKey     string = "primaryKey"
	KeyBatchSize      string = "batchSize"

	defaultBatchSize = 100
)

// Config represents configuration needed for Firebolt.
type Config struct {
	// Email Firebolt account email.
	Email string `validate:"required,email"`

	// Password Firebolt account password.
	Password string `validate:"required"`

	// EngineEndpoint - engine endpoint.
	EngineEndpoint string `validate:"required"`

	// DB name.
	DB string `validate:"required"`

	// Table name.
	Table string `validate:"required"`

	// List of columns from table, by default read all columns.
	Columns []string

	// Key - Column name that records should use for their `Key` fields.
	PrimaryKey string `validate:"required"`

	// BatchSize - size of batch.
	BatchSize int `validate:"gte=1,lte=100"`
}

// Parse attempts to parse plugins.Config into a Config struct.
func Parse(cfg map[string]string) (Config, error) {
	config := Config{
		Email:          cfg[KeyEmail],
		Password:       cfg[KeyPassword],
		EngineEndpoint: cfg[KeyEngineEndpoint],
		DB:             cfg[KeyDB],
		Table:          cfg[KeyTable],
		PrimaryKey:     cfg[KeyPrimaryKey],
		BatchSize:      defaultBatchSize,
	}

	if colsRaw := cfg[KeyColumns]; colsRaw != "" {
		config.Columns = strings.Split(colsRaw, ",")
	}

	if cfg[KeyBatchSize] != "" {
		batchSize, err := strconv.Atoi(cfg[KeyBatchSize])
		if err != nil {
			return Config{}, errors.New(`"batchSize" config value must be int`)
		}

		config.BatchSize = batchSize
	}

	err := config.Validate()
	if err != nil {
		return Config{}, err
	}

	return config, nil
}
