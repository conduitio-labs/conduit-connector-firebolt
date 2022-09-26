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
	"strings"

	"github.com/conduitio-labs/conduit-connector-firebolt/config/validator"
)

const (
	// KeyEmail is a config name for an email.
	KeyEmail string = "email"
	// KeyPassword is a config name for a password.
	KeyPassword string = "password"
	// KeyAccountName is a config name for an account name.
	KeyAccountName string = "accountName"
	// KeyEngineName is a config name for an engine name.
	KeyEngineName string = "engineName"
	// KeyDB is a config name for a db.
	KeyDB string = "db"
	// KeyTable is a config name for a table.
	KeyTable string = "table"
)

// General represents configuration needed for Firebolt.
// This values are shared between source and destination.
type General struct {
	// Email Firebolt account email.
	Email string `validate:"required,email"`
	// Password Firebolt account password.
	Password string `validate:"required"`
	// AccountName is a Firebolt account name.
	AccountName string `validate:"required"`
	// EngineName is a Firebolt engine name.
	EngineName string `validate:"required"`
	// DB - database name.
	DB string `validate:"required"`
	// Table - database table name.
	Table string `validate:"required"`
}

// Parse attempts to parse plugins.Config into a General struct.
func ParseGeneral(cfg map[string]string) (General, error) {
	general := General{
		Email:       strings.ToLower(cfg[KeyEmail]),
		Password:    cfg[KeyPassword],
		AccountName: cfg[KeyAccountName],
		EngineName:  cfg[KeyEngineName],
		DB:          cfg[KeyDB],
		Table:       cfg[KeyTable],
	}

	if err := validator.Validate(general); err != nil {
		return General{}, err
	}

	return general, nil
}
