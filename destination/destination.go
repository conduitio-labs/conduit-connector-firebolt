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

	"github.com/conduitio-labs/conduit-connector-firebolt/config"
	"github.com/conduitio-labs/conduit-connector-firebolt/destination/writer"
	"github.com/conduitio-labs/conduit-connector-firebolt/firebolt"
	"github.com/conduitio-labs/conduit-connector-firebolt/repository"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Writer defines a writer interface needed for the Destination.
type Writer interface {
	InsertRecord(ctx context.Context, record sdk.Record) error
	Close(ctx context.Context) error
}

// FireboltClient defines a FireboltClient interface needed for the Source.
type FireboltClient interface {
	Login(ctx context.Context, params firebolt.LoginParams) error
	StartEngine(ctx context.Context) error
	IsEngineStarted(ctx context.Context) (bool, error)
	RunQuery(ctx context.Context, query string) ([]byte, error)
	Close(ctx context.Context)
}

// Destination Firebolt Connector persists records to an Firebolt database.
type Destination struct {
	sdk.UnimplementedDestination

	config         config.Destination
	writer         Writer
	fireboltClient FireboltClient
}

// New creates new instance of the Destination.
func New() sdk.Destination {
	return &Destination{}
}

// Configure parses and initializes the Destination config.
func (d *Destination) Configure(ctx context.Context, cfg map[string]string) error {
	configuration, err := config.ParseDestination(cfg)
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}

	d.config = configuration

	return nil
}

// Open makes sure everything is prepared to persists records.
func (d *Destination) Open(ctx context.Context) error {
	d.fireboltClient = firebolt.NewClient(ctx, d.config.DB)

	err := d.fireboltClient.Login(ctx, firebolt.LoginParams{
		Email:       d.config.Email,
		Password:    d.config.Password,
		AccountName: d.config.AccountName,
		EngineName:  d.config.EngineName,
	})
	if err != nil {
		return fmt.Errorf("client login: %w", err)
	}

	repository := repository.New(d.fireboltClient)

	w, err := writer.NewWriter(ctx, repository, d.config.Table)
	if err != nil {
		return fmt.Errorf("create writer: %w", err)
	}

	d.writer = w

	if err := d.fireboltClient.StartEngine(ctx); err != nil {
		return fmt.Errorf("start engine: %w", err)
	}

	return nil
}

// Write writes a record into a Destination.
func (d *Destination) Write(ctx context.Context, record sdk.Record) error {
	isEngineStarted, err := d.fireboltClient.IsEngineStarted(ctx)
	if err != nil {
		return fmt.Errorf("is engine started: %w", err)
	}

	if !isEngineStarted {
		return nil
	}

	return d.writer.InsertRecord(ctx, record)
}

// Teardown gracefully closes connections.
func (d *Destination) Teardown(ctx context.Context) error {
	if d.writer != nil {
		return d.writer.Close(ctx)
	}

	return nil
}
