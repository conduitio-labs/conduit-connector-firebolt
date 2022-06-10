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

func TestParse(t *testing.T) {
	tests := []struct {
		name    string
		cfg     map[string]string
		want    Config
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
			want: Config{
				Email:          "test@test.com",
				Password:       "12345",
				EngineEndpoint: "endpoint",
				DB:             "db",
				Table:          "test",
				PrimaryKey:     "id",
				BatchSize:      100,
				OrderingColumn: "id",
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
			want: Config{
				Email:          "test@test.com",
				Password:       "12345",
				EngineEndpoint: "endpoint",
				DB:             "db",
				Table:          "test",
				PrimaryKey:     "id",
				BatchSize:      20,
				OrderingColumn: "id",
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
			want: Config{
				Email:          "test@test.com",
				Password:       "12345",
				EngineEndpoint: "endpoint",
				DB:             "db",
				Table:          "test",
				PrimaryKey:     "id",
				BatchSize:      20,
				Columns:        []string{"id", "name"},
				OrderingColumn: "id",
			},
			wantErr: false,
		},
		{
			name: "invalid config, missed email",
			cfg: map[string]string{
				KeyPassword:       "12345",
				KeyEngineEndpoint: "endpoint",
				KeyDB:             "db",
				KeyTable:          "test",
				KeyPrimaryKey:     "id",
				KeyBatchSize:      "20",
				KeyColumns:        "id,name",
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "invalid config, missed password",
			cfg: map[string]string{
				KeyEmail:          "test@test.com",
				KeyEngineEndpoint: "endpoint",
				KeyDB:             "db",
				KeyTable:          "test",
				KeyPrimaryKey:     "id",
				KeyBatchSize:      "20",
				KeyColumns:        "id,name",
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "invalid config, missed db",
			cfg: map[string]string{
				KeyEmail:          "test@test.com",
				KeyPassword:       "12345",
				KeyEngineEndpoint: "endpoint",
				KeyTable:          "test",
				KeyPrimaryKey:     "id",
				KeyBatchSize:      "20",
				KeyColumns:        "id,name",
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "invalid config, invalid email",
			cfg: map[string]string{
				KeyEmail:          "test",
				KeyPassword:       "12345",
				KeyEngineEndpoint: "endpoint",
				KeyDB:             "db",
				KeyTable:          "test",
				KeyPrimaryKey:     "id",
				KeyBatchSize:      "100",
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "invalid config, invalid batchSize",
			cfg: map[string]string{
				KeyEmail:          "test@test,com",
				KeyPassword:       "12345",
				KeyEngineEndpoint: "endpoint",
				KeyDB:             "db",
				KeyTable:          "test",
				KeyPrimaryKey:     "id",
				KeyBatchSize:      "test",
			},
			want:    Config{},
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
			want:    Config{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.cfg)
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
