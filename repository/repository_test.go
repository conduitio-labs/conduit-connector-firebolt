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

package repository

import (
	"encoding/json"
	"testing"

	"github.com/conduitio-labs/conduit-connector-firebolt/client"
	"github.com/matryer/is"
)

func Test_prepareRunQueryResponseData(t *testing.T) {
	type args struct {
		resp *client.RunQueryResponse
	}
	tests := []struct {
		name    string
		raw     []byte
		args    args
		want    []map[string]any
		wantErr bool
	}{
		{
			name: "success",
			raw: []byte(`{
				"meta": [
				  {
					"name": "id",
					"type": "Int32"
				  },
				  {
					"name": "isOkay",
					"type": "UInt8"
				  }
				],
				"data": [
				  {
					"id": 2,
					"isOkay": 1
				  }
				]
			  }`),
			want: []map[string]any{
				{
					"id":     float64(2),
					"isOkay": true,
				},
			},
			wantErr: false,
		},
		{
			name: "fail",
			raw: []byte(`{
				"meta": [
				  {
					"name": "id",
					"type": "Int32"
				  },
				  {
					"name": "isOkay",
					"type": "UInt8"
				  }
				],
				"data": [
				  {
					"id": 2,
					"isOkay": "1"
				  }
				]
			  }`),
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is := is.New(t)

			err := json.Unmarshal(tt.raw, &tt.args.resp)
			is.NoErr(err)

			err = prepareRunQueryResponseData(tt.args.resp)
			if (err != nil) != tt.wantErr {
				t.Errorf("prepareRunQueryResponseData() error = %v, wantErr %v", err, tt.wantErr)

				return
			}

			if tt.wantErr && err != nil {
				return
			}

			is.Equal(tt.args.resp.Data, tt.want)
		})
	}
}
