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

package destination

import (
	"context"
	"fmt"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"

	"github.com/conduitio-labs/conduit-connector-firebolt/client"
	"github.com/conduitio-labs/conduit-connector-firebolt/config"
	"github.com/conduitio-labs/conduit-connector-firebolt/destination/writer"
)

// Writer defines a writer interface needed for the Destination.
type Writer interface {
	InsertRecord(ctx context.Context, record sdk.Record) error
	SetColumnTypes(cl map[string]string)
	Close(ctx context.Context) error
}

// Destination Firebolt Connector persists records to an Firebolt database.
type Destination struct {
	sdk.UnimplementedDestination

	config config.Destination
	writer Writer
	client *client.Client
}

// New creates new instance of the Destination.
func New() sdk.Destination {
	return &Destination{}
}

// Parameters returns a map of named sdk.Parameters that describe how to configure the Destination.
func (d *Destination) Parameters() map[string]sdk.Parameter {
	return map[string]sdk.Parameter{
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
		config.KeyAccountName: {
			Default:     "",
			Required:    true,
			Description: "The Firebolt account name.",
		},
		config.KeyEngineName: {
			Default:     "",
			Required:    true,
			Description: "The Firebolt engine name.",
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
	}
}

// Configure parses and initializes the Destination config.
func (d *Destination) Configure(_ context.Context, cfg map[string]string) error {
	configuration, err := config.ParseDestination(cfg)
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}

	d.config = configuration

	return nil
}

// Open makes sure everything is prepared to persists records.
func (d *Destination) Open(ctx context.Context) error {
	d.client = client.New(ctx, d.config.DB)

	err := d.client.Login(ctx, client.LoginParams{
		Email:       d.config.Email,
		Password:    d.config.Password,
		AccountName: d.config.AccountName,
		EngineName:  d.config.EngineName,
	})
	if err != nil {
		return fmt.Errorf("client login: %w", err)
	}

	d.writer, err = writer.NewWriter(d.client, d.config.Table)
	if err != nil {
		return fmt.Errorf("create writer: %w", err)
	}

	isEngineStarted, err := d.client.StartEngine(ctx)
	if err != nil {
		return fmt.Errorf("start engine: %w", err)
	}

	if !isEngineStarted {
		ctxWithTimeOut, cancel := context.WithTimeout(ctx, 10*time.Minute)
		defer cancel()

		if err = d.client.WaitEngineStarted(ctxWithTimeOut); err != nil {
			return fmt.Errorf("wait engine started: %w", err)
		}
	}

	clTypes, err := d.client.GetColumnTypes(ctx, d.config.Table)
	if err != nil {
		return fmt.Errorf("get column types:%w", err)
	}

	d.writer.SetColumnTypes(clTypes)

	return nil
}

// Write writes a record into a Destination.
func (d *Destination) Write(ctx context.Context, records []sdk.Record) (int, error) {
	for i, record := range records {
		// Firebolt doesn't support update and delete operations.
		// Destination inserts record if operation value is snapshot or create
		err := sdk.Util.Destination.Route(ctx, record,
			d.writer.InsertRecord,
			emptyHandle,
			emptyHandle,
			d.writer.InsertRecord,
		)
		if err != nil {
			return i, fmt.Errorf("route %s: %w", record.Operation.String(), err)
		}
	}

	return len(records), nil
}

// Teardown gracefully closes connections.
func (d *Destination) Teardown(ctx context.Context) error {
	if d.writer != nil {
		return d.writer.Close(ctx)
	}

	return nil
}

// emptyHandle - default function to replacing update, delete functions for sdk.Route.
func emptyHandle(ctx context.Context, record sdk.Record) error {
	return nil
}
