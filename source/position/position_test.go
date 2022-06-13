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

package position

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

func TestParseSDKPosition(t *testing.T) {
	pos := Position{
		IndexInBatch: 10,
		BatchID:      100,
	}

	poBytes, _ := json.Marshal(pos)

	wrongPosBytes, _ := json.Marshal(map[string]string{"BatchID": "test"})

	tests := []struct {
		name        string
		in          sdk.Position
		want        Position
		wantErr     bool
		expectedErr string
	}{
		{
			name: "valid position",
			in:   sdk.Position(poBytes),
			want: pos,
		},
		{
			name:        "invalid struct",
			in:          sdk.Position(wrongPosBytes),
			wantErr:     true,
			expectedErr: errors.New("json: cannot unmarshal string into Go struct field Position.BatchID of type int").Error(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseSDKPosition(tt.in)
			if err != nil {
				if !tt.wantErr {
					t.Errorf("parse error = \"%s\", wantErr %t", err.Error(), tt.wantErr)

					return
				}

				if err.Error() != tt.expectedErr {
					t.Errorf("expected error \"%s\", got \"%s\"", tt.expectedErr, err.Error())

					return
				}

				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parse = %v, want %v", got, tt.want)
			}
		})
	}
}
