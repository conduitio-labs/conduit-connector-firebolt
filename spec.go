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

package firebolt

import (
	sdk "github.com/conduitio/conduit-connector-sdk"

	"github.com/conduitio-labs/conduit-connector-firebolt/config"
)

type Spec struct{}

// Specification returns the Plugin's Specification.
func Specification() sdk.Specification {
	return sdk.Specification{
		Name:    "firebolt",
		Summary: "The Firebolt plugin for Conduit, written in Go.",
		Description: "The Firebolt connector is one of Conduit plugins." +
			"It provides the source and destination snowflake connector.",
		Version: "v0.1.0",
		Author:  "Meroxa, Inc.",
		SourceParams: map[string]sdk.Parameter{
			config.KeyEmail: {
				Default:     "",
				Required:    true,
				Description: "The Firebolt email account.",
			},
			config.KeyPassword: {
				Default:     "",
				Required:    true,
				Description: "The Firebolt account password.",
			},
			config.KeyDB: {
				Default:     "",
				Required:    true,
				Description: "The Firebolt database name.",
			},
			config.KeyEngineEndpoint: {
				Default:     "",
				Required:    true,
				Description: "The Firebolt database engine.",
			},
			config.KeyTable: {
				Default:     "",
				Required:    true,
				Description: "The table name.",
			},
			config.KeyColumns: {
				Default:     "",
				Required:    false,
				Description: "Comma separated list of column names that should be included in each Record's payload.",
			},
			config.KeyPrimaryKey: {
				Default:     "",
				Required:    true,
				Description: "Column name that records should use for their `Key` fields.",
			},
			config.KeyOrderingColumn: {
				Default:     "",
				Required:    true,
				Description: "Column which using for ordering data",
			},
			config.KeyBatchSize: {
				Default:     "100",
				Required:    false,
				Description: "Size of batch",
			},
		},
		DestinationParams: map[string]sdk.Parameter{
			config.KeyEmail: {
				Default:     "",
				Required:    true,
				Description: "The Firebolt email account.",
			},
			config.KeyPassword: {
				Default:     "",
				Required:    true,
				Description: "The Firebolt account password.",
			},
			config.KeyEngineEndpoint: {
				Default:     "",
				Required:    true,
				Description: "The Firebolt database engine.",
			},
			config.KeyDB: {
				Default:     "",
				Required:    true,
				Description: "The Firebolt database name.",
			},
			config.KeyTable: {
				Default:     "",
				Required:    true,
				Description: "The Firebolt database table name.",
			},
		},
	}
}
