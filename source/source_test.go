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
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/golang/mock/gomock"

	"github.com/conduitio-labs/conduit-connector-firebolt/config"
	"github.com/conduitio-labs/conduit-connector-firebolt/source/mock"
)

func TestSource_Configure(t *testing.T) {
	s := Source{}

	tests := []struct {
		name    string
		cfg     map[string]string
		wantErr bool
	}{
		{
			name: "valid config",
			cfg: map[string]string{
				config.KeyEmail:          "test@test.com",
				config.KeyPassword:       "12345",
				config.KeyEngineEndpoint: "endpoint",
				config.KeyDB:             "db",
				config.KeyTable:          "test",
				config.KeyPrimaryKey:     "id",
				config.KeyBatchSize:      "100",
				config.KeyOrderingColumn: "id",
			},
			wantErr: false,
		},
		{
			name: "valid config, custom batch size",
			cfg: map[string]string{
				config.KeyEmail:          "test@test.com",
				config.KeyPassword:       "12345",
				config.KeyEngineEndpoint: "endpoint",
				config.KeyDB:             "db",
				config.KeyTable:          "test",
				config.KeyPrimaryKey:     "id",
				config.KeyBatchSize:      "20",
				config.KeyOrderingColumn: "id",
			},
			wantErr: false,
		},
		{
			name: "invalid config, missed email",
			cfg: map[string]string{
				config.KeyPassword:       "12345",
				config.KeyEngineEndpoint: "endpoint",
				config.KeyDB:             "db",
				config.KeyTable:          "test",
				config.KeyPrimaryKey:     "id",
				config.KeyBatchSize:      "20",
				config.KeyColumns:        "id,name",
			},
			wantErr: true,
		},
		{
			name: "invalid config, missed password",
			cfg: map[string]string{
				config.KeyEmail:          "test@test.com",
				config.KeyEngineEndpoint: "endpoint",
				config.KeyDB:             "db",
				config.KeyTable:          "test",
				config.KeyPrimaryKey:     "id",
				config.KeyBatchSize:      "20",
				config.KeyColumns:        "id,name",
			},
			wantErr: true,
		},
		{
			name: "invalid config, missed db",
			cfg: map[string]string{
				config.KeyEmail:          "test@test.com",
				config.KeyPassword:       "12345",
				config.KeyEngineEndpoint: "endpoint",
				config.KeyTable:          "test",
				config.KeyPrimaryKey:     "id",
				config.KeyBatchSize:      "20",
				config.KeyColumns:        "id,name",
			},
			wantErr: true,
		},
		{
			name: "invalid config, invalid email",
			cfg: map[string]string{
				config.KeyEmail:          "test",
				config.KeyPassword:       "12345",
				config.KeyEngineEndpoint: "endpoint",
				config.KeyDB:             "db",
				config.KeyTable:          "test",
				config.KeyPrimaryKey:     "id",
				config.KeyBatchSize:      "100",
			},
			wantErr: true,
		},
		{
			name: "invalid config, invalid batchSize",
			cfg: map[string]string{
				config.KeyEmail:          "test@test,com",
				config.KeyPassword:       "12345",
				config.KeyEngineEndpoint: "endpoint",
				config.KeyDB:             "db",
				config.KeyTable:          "test",
				config.KeyPrimaryKey:     "id",
				config.KeyBatchSize:      "test",
			},
			wantErr: true,
		},
		{
			name: "invalid config, missed ordering column",
			cfg: map[string]string{
				config.KeyEmail:          "test@test.com",
				config.KeyPassword:       "12345",
				config.KeyEngineEndpoint: "endpoint",
				config.KeyDB:             "db",
				config.KeyTable:          "test",
				config.KeyPrimaryKey:     "id",
				config.KeyBatchSize:      "100",
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
			Position:  sdk.Position("1.0"),
			Metadata:  nil,
			CreatedAt: time.Time{},
			Key:       st,
			Payload:   st,
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
		ctrl := gomock.NewController(t)
		ctx := context.Background()

		it := mock.NewMockIterator(ctrl)
		it.EXPECT().HasNext(ctx).Return(true, errors.New("get data: failed"))

		s := Source{
			iterator: it,
		}

		_, err := s.Read(ctx)
		if err == nil {
			t.Errorf("want error")
		}
	})

	t.Run("failed_next", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		ctx := context.Background()

		it := mock.NewMockIterator(ctrl)
		it.EXPECT().HasNext(ctx).Return(true, nil)
		it.EXPECT().Next(ctx).Return(sdk.Record{}, errors.New("key is not exist"))

		s := Source{
			iterator: it,
		}

		_, err := s.Read(ctx)
		if err == nil {
			t.Errorf("want error")
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
		ctrl := gomock.NewController(t)
		ctx := context.Background()

		it := mock.NewMockIterator(ctrl)
		it.EXPECT().Stop(ctx).Return(errors.New("some error"))

		s := Source{
			iterator: it,
		}

		err := s.Teardown(ctx)
		if err == nil {
			t.Errorf("want error")
		}
	})
}
