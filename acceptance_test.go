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

package firebolt

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit"
	"github.com/conduitio-labs/conduit-connector-firebolt/client"
	"github.com/conduitio-labs/conduit-connector-firebolt/config"
	"github.com/conduitio-labs/conduit-connector-firebolt/destination"
	"github.com/conduitio-labs/conduit-connector-firebolt/source"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
	"go.uber.org/goleak"
)

const (
	queryCreateTable = "CREATE DIMENSION TABLE %s (id INT, name TEXT)"
	queryDropTable   = "DROP TABLE IF EXISTS %s"

	metadataAction = "action"
	actionInsert   = "insertValue"
)

type driver struct {
	sdk.ConfigurableAcceptanceTestDriver

	counter int64
}

// GenerateRecord generates a random sdk.Record.
func (d *driver) GenerateRecord(t *testing.T) sdk.Record {
	atomic.AddInt64(&d.counter, 1)

	return sdk.Record{
		Position: nil,
		Metadata: map[string]string{
			metadataAction:  actionInsert,
			config.KeyTable: d.Config.DestinationConfig[config.KeyTable],
		},
		Key: sdk.StructuredData{
			// convert to float64, since the connector will unmarhsal the value into "any" as float64
			// see https://pkg.go.dev/encoding/json#Unmarshal
			"id": float64(d.counter),
		},
		Payload: sdk.RawData(
			fmt.Sprintf(
				`{"id":%d,"name":"%s"}`, d.counter, gofakeit.Name(),
			),
		),
	}
}

func TestAcceptance(t *testing.T) {
	cfg := prepareConfig(t)

	sdk.AcceptanceTest(t, &driver{
		ConfigurableAcceptanceTestDriver: sdk.ConfigurableAcceptanceTestDriver{
			Config: sdk.ConfigurableAcceptanceTestDriverConfig{
				Connector: sdk.Connector{
					NewSpecification: Specification,
					NewSource:        source.New,
					NewDestination:   destination.New,
				},
				SourceConfig:      cfg,
				DestinationConfig: cfg,
				BeforeTest:        beforeTest(t, cfg),
				GoleakOptions: []goleak.Option{
					goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
				},
			},
		},
	})
}

// beforeTest creates new table before each test.
func beforeTest(t *testing.T, cfg map[string]string) func(t *testing.T) {
	return func(t *testing.T) {
		is := is.New(t)

		err := prepareData(context.Background(), t, cfg)
		is.NoErr(err)
	}
}

func prepareConfig(t *testing.T) map[string]string {
	email := os.Getenv("FIREBOLT_EMAIL")
	if email == "" {
		t.Skip("FIREBOLT_EMAIL env var must be set")
	}

	password := os.Getenv("FIREBOLT_PASSWORD")
	if password == "" {
		t.Skip("FIREBOLT_PASSWORD env var must be set")
	}

	databaseEngine := os.Getenv("FIREBOLT_DATABASE_ENGINE")
	if databaseEngine == "" {
		t.Skip("FIREBOLT_DATABASE_ENGINE env var must be set")
	}

	db := os.Getenv("FIREBOLT_DB")
	if db == "" {
		t.Skip("FIREBOLT_DB env var must be set")
	}

	cfg := map[string]string{
		config.KeyEmail:          email,
		config.KeyPassword:       password,
		config.KeyEngineEndpoint: databaseEngine,
		config.KeyDB:             db,
		config.KeyPrimaryKey:     "id",
		config.KeyBatchSize:      "100",
		config.KeyOrderingColumn: "id",
	}

	return cfg
}

func prepareData(ctx context.Context, t *testing.T, cfg map[string]string) error {
	cl := client.New(ctx, cfg[config.KeyEngineEndpoint], cfg[config.KeyDB])

	err := cl.Login(ctx, cfg[config.KeyEmail], cfg[config.KeyPassword])
	if err != nil {
		return fmt.Errorf("client login: %w", err)
	}

	tableName := randomIdentifier(t)
	t.Logf("testing on table %s", tableName)
	queryCreateTable := fmt.Sprintf(queryCreateTable, tableName)

	// create table.
	_, err = cl.RunQuery(ctx, queryCreateTable)
	if err != nil {
		return err
	}

	cfg[config.KeyTable] = tableName

	// drop table
	t.Cleanup(func() {
		queryDropTable := fmt.Sprintf(queryDropTable, tableName)

		_, err = cl.RunQuery(ctx, queryDropTable)
		if err != nil {
			t.Errorf("drop test table: %v", err)
		}
	})

	return nil
}

func randomIdentifier(t *testing.T) string {
	return fmt.Sprintf("conduit_%v_%d",
		strings.ReplaceAll(strings.ToLower(t.Name()), "/", "_"),
		time.Now().UnixMicro()%1000)
}
