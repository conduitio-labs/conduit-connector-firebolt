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

import "github.com/conduitio-labs/conduit-connector-firebolt/config/validator"

const (
	// KeyEmail is a config name for a email.
	KeyEmail string = "email"
	// KeyPassword is a config name for a password.
	KeyPassword string = "password"
	// KeyEngineEndpoint is a config name for a engine endpoint.
	KeyEngineEndpoint string = "engineEndpoint"
	// KeyDB is a config name for a db.
	KeyDB string = "db"
	// KeyTable is a config name for a table.
	KeyTable string = "table"
)

// Common represents configuration needed for Firebolt.
// This values are shared between source and destination.
type Common struct {
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
}

// Parse attempts to parse plugins.Config into a Config struct.
func ParseCommon(cfg map[string]string) (Common, error) {
	common := Common{
		Email:          cfg[KeyEmail],
		Password:       cfg[KeyPassword],
		EngineEndpoint: cfg[KeyEngineEndpoint],
		DB:             cfg[KeyDB],
		Table:          cfg[KeyTable],
	}

	if err := validator.Validate(common); err != nil {
		return Common{}, err
	}

	return common, nil
}
