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

func TestParseGeneral(t *testing.T) {
	tests := []struct {
		name    string
		cfg     map[string]string
		want    General
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
			},
			want: General{
				Email:          "test@test.com",
				Password:       "12345",
				EngineEndpoint: "endpoint",
				DB:             "db",
				Table:          "test",
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
			want:    General{},
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
			want:    General{},
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
			want:    General{},
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
			want:    General{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseGeneral(tt.cfg)
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
