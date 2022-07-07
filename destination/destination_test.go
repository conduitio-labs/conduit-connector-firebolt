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
	"errors"
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-firebolt/config"
	"github.com/conduitio-labs/conduit-connector-firebolt/destination/mock"
	"github.com/conduitio-labs/conduit-connector-firebolt/destination/writer"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/golang/mock/gomock"
)

func TestDestination_Configure(t *testing.T) {
	s := Destination{}

	tests := []struct {
		name    string
		cfg     map[string]string
		wantErr bool
	}{
		{
			name: "valid config",
			cfg: map[string]string{
				config.KeyEmail:       "test@test.com",
				config.KeyPassword:    "12345",
				config.KeyAccountName: "super_account",
				config.KeyEngineName:  "super_engine",
				config.KeyDB:          "db",
				config.KeyTable:       "test",
			},
			wantErr: false,
		},
		{
			name: "invalid config, missed email",
			cfg: map[string]string{
				config.KeyPassword:    "12345",
				config.KeyAccountName: "super_account",
				config.KeyEngineName:  "super_engine",
				config.KeyDB:          "db",
				config.KeyTable:       "test",
			},
			wantErr: true,
		},
		{
			name: "invalid config, missed password",
			cfg: map[string]string{
				config.KeyEmail:       "test@test.com",
				config.KeyAccountName: "super_account",
				config.KeyEngineName:  "super_engine",
				config.KeyDB:          "db",
				config.KeyTable:       "test",
			},
			wantErr: true,
		},
		{
			name: "invalid config, missed db",
			cfg: map[string]string{
				config.KeyEmail:       "test@test.com",
				config.KeyPassword:    "12345",
				config.KeyAccountName: "super_account",
				config.KeyEngineName:  "super_engine",
				config.KeyTable:       "test",
			},
			wantErr: true,
		},
		{
			name: "invalid config, invalid email",
			cfg: map[string]string{
				config.KeyEmail:       "test",
				config.KeyPassword:    "12345",
				config.KeyAccountName: "super_account",
				config.KeyEngineName:  "super_engine",
				config.KeyDB:          "db",
				config.KeyTable:       "test",
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

func TestDestination_Write(t *testing.T) {
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

		w := mock.NewMockWriter(ctrl)
		w.EXPECT().InsertRecord(ctx, record).Return(nil)

		d := Destination{
			writer: w,
		}

		err := d.Write(ctx, record)
		if err != nil {
			t.Errorf("read error = \"%s\"", err.Error())
		}
	})

	t.Run("failed_write", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		ctx := context.Background()

		w := mock.NewMockWriter(ctrl)
		w.EXPECT().InsertRecord(ctx, sdk.Record{}).Return(writer.ErrEmptyPayload)

		d := Destination{
			writer: w,
		}

		err := d.Write(ctx, sdk.Record{})
		if err == nil {
			t.Errorf("want error")
		}
	})
}

func TestDestination_Teardown(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		ctx := context.Background()

		w := mock.NewMockWriter(ctrl)
		w.EXPECT().Close(ctx).Return(nil)

		d := Destination{
			writer: w,
		}
		err := d.Teardown(ctx)
		if err != nil {
			t.Errorf("teardown error = \"%s\"", err.Error())
		}
	})

	t.Run("failed", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		ctx := context.Background()

		w := mock.NewMockWriter(ctrl)
		w.EXPECT().Close(ctx).Return(errors.New("some error"))

		d := Destination{
			writer: w,
		}

		err := d.Teardown(ctx)
		if err == nil {
			t.Errorf("want error")
		}
	})
}
