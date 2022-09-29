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

package source

import (
	"context"
	"fmt"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"

	"github.com/conduitio-labs/conduit-connector-firebolt/client"
	"github.com/conduitio-labs/conduit-connector-firebolt/config"
	"github.com/conduitio-labs/conduit-connector-firebolt/source/iterator"
)

// Iterator defines an Iterator interface needed for the Source.
type Iterator interface {
	Setup(ctx context.Context, p sdk.Position) error
	HasNext(ctx context.Context) (bool, error)
	Next(ctx context.Context) (sdk.Record, error)
	Stop(ctx context.Context) error
	Ack(ctx context.Context, p sdk.Position) error
}

// Source connector.
type Source struct {
	sdk.UnimplementedSource

	config   config.Source
	iterator Iterator
}

// New initialises a new source.
func New() sdk.Source {
	return &Source{}
}

// Parameters returns a map of named sdk.Parameters that describe how to configure the Source.
func (s *Source) Parameters() map[string]sdk.Parameter {
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
		config.KeyDB: {
			Default:     "",
			Required:    true,
			Description: "The Firebolt database name.",
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
			Description: "Columns names that records should use for their `Key` fields.",
		},
		config.KeyBatchSize: {
			Default:     "100",
			Required:    false,
			Description: "Size of batch",
		},
	}
}

// Configure parses and stores configurations, returns an error in case of invalid configuration.
func (s *Source) Configure(ctx context.Context, cfgRaw map[string]string) error {
	cfg, err := config.ParseSource(cfgRaw)
	if err != nil {
		return err
	}

	s.config = cfg

	return nil
}

// Open prepare the plugin to start sending records from the given position.
func (s *Source) Open(ctx context.Context, rp sdk.Position) error {
	fireboltClient := client.New(ctx, s.config.DB)

	err := fireboltClient.Login(ctx, client.LoginParams{
		Email:       s.config.Email,
		Password:    s.config.Password,
		AccountName: s.config.AccountName,
		EngineName:  s.config.EngineName,
	})
	if err != nil {
		return fmt.Errorf("client login: %w", err)
	}

	s.iterator = iterator.NewSnapshotIterator(fireboltClient, s.config.BatchSize, s.config.Table, s.config.Columns,
		s.config.PrimaryKeys)

	ctxWithTimeOut, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	if err = fireboltClient.WaitEngineStarted(ctxWithTimeOut); err != nil {
		return fmt.Errorf("wait engine started: %w", err)
	}

	if err = s.iterator.Setup(ctx, rp); err != nil {
		return fmt.Errorf("iterator setup: %w", err)
	}

	return nil
}

// Read gets the next object from the firebolt.
func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	hasNext, err := s.iterator.HasNext(ctx)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("has next: %w", err)
	}

	if !hasNext {
		return sdk.Record{}, sdk.ErrBackoffRetry
	}

	r, err := s.iterator.Next(ctx)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("next: %w", err)
	}

	return r, nil
}

// Teardown gracefully shutdown connector.
func (s *Source) Teardown(ctx context.Context) error {
	if s.iterator != nil {
		err := s.iterator.Stop(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

// Ack check if record with position was recorded.
func (s *Source) Ack(ctx context.Context, p sdk.Position) error {
	return s.iterator.Ack(ctx, p)
}

// SetIterator set iterator, using for testing purpose.
func (s *Source) SetIterator(it Iterator) {
	s.iterator = it
}
