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

	"github.com/conduitio-labs/conduit-connector-firebolt/config"
	"github.com/conduitio-labs/conduit-connector-firebolt/firebolt"
	"github.com/conduitio-labs/conduit-connector-firebolt/repository"
	"github.com/conduitio-labs/conduit-connector-firebolt/source/iterator"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Iterator defines an Iterator interface needed for the Source.
type Iterator interface {
	Setup(ctx context.Context, p sdk.Position) error
	HasNext(ctx context.Context) (bool, error)
	Next(ctx context.Context) (sdk.Record, error)
	Stop(ctx context.Context) error
	Ack(p sdk.Position) error
}

// FireboltClient defines a FireboltClient interface needed for the Source.
type FireboltClient interface {
	Login(ctx context.Context, params firebolt.LoginParams) error
	StartEngine(ctx context.Context) error
	IsEngineStarted(ctx context.Context) (bool, error)
	RunQuery(ctx context.Context, query string) ([]byte, error)
	Close(ctx context.Context)
}

// Source connector.
type Source struct {
	sdk.UnimplementedSource

	config          config.Source
	iterator        Iterator
	fireboltClient  FireboltClient
	isIteratorSetup bool
	initialPosition sdk.Position
}

// New initialises a new source.
func New() sdk.Source {
	return &Source{}
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
	s.fireboltClient = firebolt.NewClient(ctx, s.config.DB)

	err := s.fireboltClient.Login(ctx, firebolt.LoginParams{
		Email:       s.config.Email,
		Password:    s.config.Password,
		AccountName: s.config.AccountName,
		EngineName:  s.config.EngineName,
	})
	if err != nil {
		return fmt.Errorf("client login: %w", err)
	}

	rep := repository.New(s.fireboltClient)

	it := iterator.NewSnapshotIterator(rep, s.config.BatchSize, s.config.Columns, s.config.Table,
		s.config.PrimaryKey, s.config.OrderingColumn)

	s.iterator = it

	if err := s.fireboltClient.StartEngine(ctx); err != nil {
		return fmt.Errorf("start engine: %w", err)
	}

	isEngineStarted, err := s.fireboltClient.IsEngineStarted(ctx)
	if err != nil {
		return fmt.Errorf("is engine started: %w", err)
	}

	if isEngineStarted {
		if err := s.iterator.Setup(ctx, rp); err != nil {
			return fmt.Errorf("iterator setup: %w", err)
		}

		s.isIteratorSetup = true
	} else {
		s.initialPosition = rp
	}

	return nil
}

// Read gets the next object from the snowflake.
func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	isEngineStarted, err := s.fireboltClient.IsEngineStarted(ctx)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("is engine started: %w", err)
	}

	if !isEngineStarted {
		return sdk.Record{}, sdk.ErrBackoffRetry
	}

	if !s.isIteratorSetup {
		if err := s.iterator.Setup(ctx, s.initialPosition); err != nil {
			return sdk.Record{}, fmt.Errorf("iterator setup: %w", err)
		}

		s.isIteratorSetup = true
	}

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
	return s.iterator.Ack(p)
}
