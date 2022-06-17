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

	sdk "github.com/conduitio/conduit-connector-sdk"

	"github.com/conduitio-labs/conduit-connector-firebolt/client"
	"github.com/conduitio-labs/conduit-connector-firebolt/config"
	"github.com/conduitio-labs/conduit-connector-firebolt/repository"
	"github.com/conduitio-labs/conduit-connector-firebolt/source/iterator"
)

// Source connector.
type Source struct {
	sdk.UnimplementedSource

	config   config.Config
	iterator Iterator
}

// New initialises a new source.
func New() sdk.Source {
	return &Source{}
}

// Configure parses and stores configurations, returns an error in case of invalid configuration.
func (s *Source) Configure(ctx context.Context, cfgRaw map[string]string) error {
	cfg, err := config.Parse(cfgRaw)
	if err != nil {
		return err
	}

	s.config = cfg

	return nil
}

// Open prepare the plugin to start sending records from the given position.
func (s *Source) Open(ctx context.Context, rp sdk.Position) error {
	cl := client.New(ctx, s.config.EngineEndpoint, s.config.DB)

	err := cl.Login(ctx, s.config.Email, s.config.Password)
	if err != nil {
		return fmt.Errorf("client login: %w", err)
	}

	rep := repository.New(cl)

	it := iterator.NewSnapshotIterator(rep, s.config.BatchSize, s.config.Columns, s.config.Table,
		s.config.PrimaryKey, s.config.OrderingColumn)

	err = it.Setup(ctx, rp)
	if err != nil {
		return fmt.Errorf("setup iterator: %w", err)
	}

	s.iterator = it

	return nil
}

// Read gets the next object from the snowflake.
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
	return s.iterator.Ack(p)
}
