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

import (
	"reflect"
	"testing"
)

func TestParseSource(t *testing.T) {
	tests := []struct {
		name    string
		cfg     map[string]string
		want    Source
		wantErr bool
	}{
		{
			name: "valid config",
			cfg: map[string]string{
				KeyEmail:          "test@test.com",
				KeyPassword:       "12345",
				KeyEngineEndpoint: "endpoint",
				KeyDB:             "db",
				KeyTable:          "test",
				KeyPrimaryKey:     "id",
				KeyBatchSize:      "100",
				KeyOrderingColumn: "id",
			},
			want: Source{
				Common: Common{
					Email:          "test@test.com",
					Password:       "12345",
					EngineEndpoint: "endpoint",
					DB:             "db",
					Table:          "test",
				},
				BatchSize:      100,
				OrderingColumn: "id",
				PrimaryKey:     "id",
			},
			wantErr: false,
		},
		{
			name: "valid config, custom batch size",
			cfg: map[string]string{
				KeyEmail:          "test@test.com",
				KeyPassword:       "12345",
				KeyEngineEndpoint: "endpoint",
				KeyDB:             "db",
				KeyTable:          "test",
				KeyPrimaryKey:     "id",
				KeyBatchSize:      "20",
				KeyOrderingColumn: "id",
			},
			want: Source{
				Common: Common{
					Email:          "test@test.com",
					Password:       "12345",
					EngineEndpoint: "endpoint",
					DB:             "db",
					Table:          "test",
				},
				BatchSize:      20,
				OrderingColumn: "id",
				PrimaryKey:     "id",
			},
			wantErr: false,
		},
		{
			name: "valid config, custom columns",
			cfg: map[string]string{
				KeyEmail:          "test@test.com",
				KeyPassword:       "12345",
				KeyEngineEndpoint: "endpoint",
				KeyDB:             "db",
				KeyTable:          "test",
				KeyPrimaryKey:     "id",
				KeyBatchSize:      "20",
				KeyColumns:        "id,name",
				KeyOrderingColumn: "id",
			},
			want: Source{
				Common: Common{
					Email:          "test@test.com",
					Password:       "12345",
					EngineEndpoint: "endpoint",
					DB:             "db",
					Table:          "test",
				},
				BatchSize:      20,
				Columns:        []string{"id", "name"},
				OrderingColumn: "id",
				PrimaryKey:     "id",
			},
			wantErr: false,
		},
		{
			name: "invalid config, invalid batchSize",
			cfg: map[string]string{
				KeyEmail:          "test@test.com",
				KeyPassword:       "12345",
				KeyEngineEndpoint: "endpoint",
				KeyDB:             "db",
				KeyTable:          "test",
				KeyPrimaryKey:     "id",
				KeyBatchSize:      "984579579",
				KeyOrderingColumn: "id",
			},
			want:    Source{},
			wantErr: true,
		},
		{
			name: "invalid config, missed ordering column",
			cfg: map[string]string{
				KeyEmail:          "test@test.com",
				KeyPassword:       "12345",
				KeyEngineEndpoint: "endpoint",
				KeyDB:             "db",
				KeyTable:          "test",
				KeyPrimaryKey:     "id",
				KeyBatchSize:      "100",
			},
			want:    Source{},
			wantErr: true,
		},
		{
			name: "invalid config, missed primary key",
			cfg: map[string]string{
				KeyEmail:          "test@test.com",
				KeyPassword:       "12345",
				KeyEngineEndpoint: "endpoint",
				KeyDB:             "db",
				KeyTable:          "test",
				KeyBatchSize:      "20",
				KeyColumns:        "id,name",
				KeyOrderingColumn: "id",
			},
			want:    Source{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseSource(tt.cfg)
			if err != nil && !tt.wantErr {
				t.Errorf("parse error = \"%s\", wantErr %t", err.Error(), tt.wantErr)

				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parse = %v, want %v", got, tt.want)
			}
		})
	}
}
