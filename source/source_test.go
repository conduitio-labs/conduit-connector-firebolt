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
	"errors"
	"reflect"
	"testing"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/golang/mock/gomock"

	"github.com/conduitio-labs/conduit-connector-firebolt/config"
	"github.com/conduitio-labs/conduit-connector-firebolt/source/mock"
)

func TestSource_Configure(t *testing.T) {
	s := New()

	tests := []struct {
		name    string
		cfg     map[string]string
		wantErr bool
	}{
		{
			name: "valid config",
			cfg: map[string]string{
				config.KeyEmail:           "test@test.com",
				config.KeyPassword:        "12345",
				config.KeyAccountName:     "super_account",
				config.KeyEngineName:      "super_engine",
				config.KeyDB:              "db",
				config.KeyTable:           "test",
				config.KeyPrimaryKeys:     "id",
				config.KeyBatchSize:       "100",
				config.KeyOrderingColumns: "id",
			},
			wantErr: false,
		},
		{
			name: "valid config, custom batch size",
			cfg: map[string]string{
				config.KeyEmail:           "test@test.com",
				config.KeyPassword:        "12345",
				config.KeyAccountName:     "super_account",
				config.KeyEngineName:      "super_engine",
				config.KeyDB:              "db",
				config.KeyTable:           "test",
				config.KeyPrimaryKeys:     "id",
				config.KeyBatchSize:       "20",
				config.KeyOrderingColumns: "id",
			},
			wantErr: false,
		},
		{
			name: "invalid config, missed email",
			cfg: map[string]string{
				config.KeyPassword:        "12345",
				config.KeyAccountName:     "super_account",
				config.KeyEngineName:      "super_engine",
				config.KeyDB:              "db",
				config.KeyTable:           "test",
				config.KeyBatchSize:       "20",
				config.KeyColumns:         "id,name",
				config.KeyOrderingColumns: "id",
			},
			wantErr: true,
		},
		{
			name: "invalid config, missed password",
			cfg: map[string]string{
				config.KeyEmail:           "test@test.com",
				config.KeyAccountName:     "super_account",
				config.KeyEngineName:      "super_engine",
				config.KeyDB:              "db",
				config.KeyTable:           "test",
				config.KeyBatchSize:       "20",
				config.KeyColumns:         "id,name",
				config.KeyOrderingColumns: "id",
			},
			wantErr: true,
		},
		{
			name: "invalid config, missed db",
			cfg: map[string]string{
				config.KeyEmail:           "test@test.com",
				config.KeyPassword:        "12345",
				config.KeyAccountName:     "super_account",
				config.KeyEngineName:      "super_engine",
				config.KeyTable:           "test",
				config.KeyBatchSize:       "20",
				config.KeyColumns:         "id,name",
				config.KeyOrderingColumns: "id",
			},
			wantErr: true,
		},
		{
			name: "invalid config, invalid email",
			cfg: map[string]string{
				config.KeyEmail:           "test",
				config.KeyPassword:        "12345",
				config.KeyAccountName:     "super_account",
				config.KeyEngineName:      "super_engine",
				config.KeyDB:              "db",
				config.KeyTable:           "test",
				config.KeyBatchSize:       "100",
				config.KeyOrderingColumns: "id",
			},
			wantErr: true,
		},
		{
			name: "invalid config, invalid batchSize",
			cfg: map[string]string{
				config.KeyEmail:           "test@test,com",
				config.KeyPassword:        "12345",
				config.KeyAccountName:     "super_account",
				config.KeyEngineName:      "super_engine",
				config.KeyDB:              "db",
				config.KeyTable:           "test",
				config.KeyBatchSize:       "test",
				config.KeyOrderingColumns: "id",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := s.Configure(context.Background(), tt.cfg)
			if err != nil && !tt.wantErr {
				t.Errorf("parse error = \"%s\", wantErr %t", err.Error(), tt.wantErr)

				return
			}
		})
	}
}

func TestSource_Read(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		ctx := context.Background()

		st := make(sdk.StructuredData)
		st["key"] = "value"

		record := sdk.Record{
			Position: sdk.Position("1.0"),
			Metadata: nil,
			Key:      st,
			Payload:  sdk.Change{After: st},
		}

		it := mock.NewMockIterator(ctrl)
		it.EXPECT().HasNext(ctx).Return(true, nil)
		it.EXPECT().Next(ctx).Return(record, nil)

		s := Source{
			iterator: it,
		}

		r, err := s.Read(ctx)
		if err != nil {
			t.Errorf("read error = \"%s\"", err.Error())
		}

		if !reflect.DeepEqual(r, record) {
			t.Errorf("got = %v, want %v", r, record)
		}
	})

	t.Run("failed_has_next", func(t *testing.T) {
		errGetData := errors.New("get data: failed")

		ctrl := gomock.NewController(t)
		ctx := context.Background()

		it := mock.NewMockIterator(ctrl)
		it.EXPECT().HasNext(ctx).Return(true, errGetData)

		s := Source{
			iterator: it,
		}

		_, err := s.Read(ctx)
		if !errors.Is(err, errGetData) {
			t.Errorf("want error: %v, got error: %v", errGetData, err)
		}
	})

	t.Run("failed_next", func(t *testing.T) {
		errNoKey := errors.New("key doesn't exist")

		ctrl := gomock.NewController(t)
		ctx := context.Background()

		it := mock.NewMockIterator(ctrl)
		it.EXPECT().HasNext(ctx).Return(true, nil)
		it.EXPECT().Next(ctx).Return(sdk.Record{}, errNoKey)

		s := Source{
			iterator: it,
		}

		_, err := s.Read(ctx)
		if !errors.Is(err, errNoKey) {
			t.Errorf("want error: %v, got error: %v", errNoKey, err)
		}
	})
}

func TestSource_Teardown(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		ctx := context.Background()

		it := mock.NewMockIterator(ctrl)
		it.EXPECT().Stop(ctx).Return(nil)

		s := Source{
			iterator: it,
		}
		err := s.Teardown(ctx)
		if err != nil {
			t.Errorf("teardown error = \"%s\"", err.Error())
		}
	})

	t.Run("failed", func(t *testing.T) {
		errTeardownFailed := errors.New("teardown failed")

		ctrl := gomock.NewController(t)
		ctx := context.Background()

		it := mock.NewMockIterator(ctrl)
		it.EXPECT().Stop(ctx).Return(errTeardownFailed)

		s := Source{
			iterator: it,
		}

		err := s.Teardown(ctx)
		if !errors.Is(err, errTeardownFailed) {
			t.Errorf("want error: %v, got error: %v", errTeardownFailed, err)
		}
	})
}
