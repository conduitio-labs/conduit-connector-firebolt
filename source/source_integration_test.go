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
	"fmt"
	"os"
	"reflect"
	"testing"

	sdk "github.com/conduitio/conduit-connector-sdk"

	"github.com/conduitio-labs/conduit-connector-firebolt/client"
	"github.com/conduitio-labs/conduit-connector-firebolt/config"
)

const (
	testTable = "CONDUIT_INTEGRATION_TEST_TABLE"

	queryCreateTable = "CREATE DIMENSION TABLE CONDUIT_INTEGRATION_TEST_TABLE (id TEXT, test TEXT)"

	queryDropTable = "DROP TABLE IF EXISTS CONDUIT_INTEGRATION_TEST_TABLE"

	queryInsertTestValues = "INSERT INTO CONDUIT_INTEGRATION_TEST_TABLE VALUES ('1', 'foo'), ('2', 'bar'), ('3', 'test')"
)

func TestSource_Snapshot(t *testing.T) {
	cfg, err := prepareConfig()
	if err != nil {
		t.Skip()
	}

	ctx := context.Background()

	err = prepareData(ctx, cfg)
	if err != nil {
		t.Error(err)
	}

	s := new(Source)

	defer clearData(ctx, cfg) // nolint:errcheck,nolintlint

	err = s.Configure(ctx, cfg)
	if err != nil {
		t.Error(err)
	}

	// Start first time with nil position.
	err = s.Open(ctx, nil)
	if err != nil {
		t.Error(err)
	}

	// Check first read.
	r, err := s.Read(ctx)
	if err != nil {
		t.Error(err)
	}

	var wantedKey sdk.StructuredData
	wantedKey = map[string]interface{}{"id": "1"}

	if !reflect.DeepEqual(r.Key, wantedKey) {
		t.Error(errors.New("wrong record key"))
	}

	// Check teardown.
	err = s.Teardown(ctx)
	if err != nil {
		t.Error(err)
	}

	// Start from previous position.
	err = s.Open(ctx, r.Position)
	if err != nil {
		t.Error(err)
	}

	// Check read after teardown.
	r, err = s.Read(ctx)
	if err != nil {
		t.Error(err)
	}

	wantedKey = map[string]interface{}{"id": "2"}

	if !reflect.DeepEqual(r.Key, wantedKey) {
		t.Error(errors.New("wrong record key"))
	}

	err = s.Teardown(ctx)
	if err != nil {
		t.Error(err)
	}
}

func prepareConfig() (map[string]string, error) {
	email := os.Getenv("FIREBOLT_EMAIL")
	password := os.Getenv("FIREBOLT_PASSWORD")
	databaseEngine := os.Getenv("FIREBOLT_DATABASE_ENGINE")
	db := os.Getenv("FIREBOLT_DB")

	if email == "" || password == "" || databaseEngine == "" || db == "" {
		return map[string]string{}, errors.New("missed env variable")
	}

	return map[string]string{
		config.KeyEmail:          email,
		config.KeyPassword:       password,
		config.KeyEngineEndpoint: databaseEngine,
		config.KeyDB:             db,
		config.KeyTable:          testTable,
		config.KeyPrimaryKey:     "id",
		config.KeyBatchSize:      "100",
		config.KeyOrderingColumn: "id",
	}, nil
}

func prepareData(ctx context.Context, cfg map[string]string) error {
	cl := client.New(ctx, cfg[config.KeyEngineEndpoint], cfg[config.KeyDB])

	err := cl.Login(ctx, cfg[config.KeyEmail], cfg[config.KeyPassword])
	if err != nil {
		return fmt.Errorf("client login: %w", err)
	}

	// create table.
	_, err = cl.RunQuery(ctx, queryCreateTable)
	if err != nil {
		return err
	}

	// insert test data to table.
	_, err = cl.RunQuery(ctx, queryInsertTestValues)
	if err != nil {
		return err
	}

	return nil
}

func clearData(ctx context.Context, cfg map[string]string) error {
	cl := client.New(ctx, cfg[config.KeyEngineEndpoint], cfg[config.KeyDB])

	err := cl.Login(ctx, cfg[config.KeyEmail], cfg[config.KeyPassword])
	if err != nil {
		return fmt.Errorf("client login: %w", err)
	}

	// drop table.
	_, err = cl.RunQuery(ctx, queryDropTable)
	if err != nil {
		return err
	}

	return nil
}
