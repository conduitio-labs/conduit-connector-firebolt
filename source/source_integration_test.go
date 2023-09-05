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

	queryCreateDimensionTable = "CREATE DIMENSION TABLE CONDUIT_INTEGRATION_TEST_TABLE (id TEXT, test TEXT)"

	queryCreateTableFact = "CREATE FACT TABLE CONDUIT_INTEGRATION_TEST_TABLE (id TEXT, test TEXT) PRIMARY INDEX test"

	queryDropTable = "DROP TABLE IF EXISTS CONDUIT_INTEGRATION_TEST_TABLE"

	queryInsertTestValues = `INSERT INTO CONDUIT_INTEGRATION_TEST_TABLE VALUES 
        ('1', 'test1', '1999-01-31', '1999-01-31 20:33:14'), 
        ('2', 'test2', '2011-04-31', '2011-12-31 12:27:54'), 
		('3', 'test3',  NULL, NULL)`
)

func TestSource_Snapshot(t *testing.T) {
	cfg, err := prepareConfig()
	if err != nil {
		t.Log(err)
		t.Skip()
	}

	ctx := context.Background()

	cl, err := login(ctx, cfg)
	if err != nil {
		t.Error(err)
	}

	err = prepareTableDemensial(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}

	s := New()

	defer clearData(ctx, cl) // nolint:errcheck,nolintlint

	err = s.Configure(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Start first time with nil position.
	err = s.Open(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Check first read.
	r, err := s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	var wantedKey sdk.StructuredData
	wantedKey = map[string]any{"id": "1", "test": "test1"}

	if !reflect.DeepEqual(r.Key, wantedKey) {
		t.Fatal(errors.New("wrong record key"))
	}

	// Check teardown.
	err = s.Teardown(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Start from previous position.
	err = s.Open(ctx, r.Position)
	if err != nil {
		t.Fatal(err)
	}

	// Check read after teardown.
	r, err = s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	wantedKey = map[string]any{"id": "2", "test": "test2"}

	if !reflect.DeepEqual(r.Key, wantedKey) {
		t.Fatal(errors.New("wrong record key"))
	}

	// Check third row.
	r, err = s.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}

	wantedKey = map[string]any{"id": "3", "test": "test3"}

	if !reflect.DeepEqual(r.Key, wantedKey) {
		t.Fatal(errors.New("wrong record key"))
	}

	err = s.Teardown(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSource_Snapshot_Empty_Table(t *testing.T) {
	cfg, err := prepareConfig()
	if err != nil {
		t.Log(err)
		t.Skip()
	}

	ctx := context.Background()

	cl, err := login(ctx, cfg)
	if err != nil {
		t.Error(err)
	}

	err = prepareEmptyTable(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}

	defer clearData(ctx, cl) // nolint:errcheck,nolintlint

	s := New()

	err = s.Configure(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Start first time with nil position.
	err = s.Open(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Check read from empty table.
	_, err = s.Read(ctx)
	// expect ErrBackoffRetry error because table is empty.
	if err != sdk.ErrBackoffRetry {
		t.Error(err)
	}

	err = s.Teardown(ctx)
	if err != nil {
		t.Error(err)
	}
}

func TestSource_primaryKeysFromConfig(t *testing.T) {
	cfg, err := prepareConfig()
	if err != nil {
		t.Log(err)
		t.Skip()
	}

	cfg[config.KeyPrimaryKeys] = "id"

	ctx := context.Background()

	cl, err := login(ctx, cfg)
	if err != nil {
		t.Error(err)
	}

	err = prepareTableFact(ctx, cl)
	if err != nil {
		t.Error(err)
	}

	s := New()

	defer clearData(ctx, cl) // nolint:errcheck,nolintlint

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

	wantedKey := sdk.StructuredData{"id": "1"}

	if !reflect.DeepEqual(r.Key, wantedKey) {
		t.Error(errors.New("wrong record key"))
	}

	// Check teardown.
	err = s.Teardown(ctx)
	if err != nil {
		t.Error(err)
	}
}

func TestSource_primaryKeysFromTableMetadata(t *testing.T) {
	cfg, err := prepareConfig()
	if err != nil {
		t.Log(err)
		t.Skip()
	}

	ctx := context.Background()

	cl, err := login(ctx, cfg)
	if err != nil {
		t.Error(err)
	}

	err = prepareTableFact(ctx, cl)
	if err != nil {
		t.Error(err)
	}

	s := New()

	defer clearData(ctx, cl) // nolint:errcheck,nolintlint

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

	wantedKey := sdk.StructuredData{"test": "test1"}

	if !reflect.DeepEqual(r.Key, wantedKey) {
		t.Error(errors.New("wrong record key"))
	}

	// Check teardown.
	err = s.Teardown(ctx)
	if err != nil {
		t.Error(err)
	}
}

func TestSource_primaryKeysFromOrderingColumns(t *testing.T) {
	cfg, err := prepareConfig()
	if err != nil {
		t.Log(err)
		t.Skip()
	}

	cfg[config.KeyOrderingColumns] = "test"

	ctx := context.Background()

	cl, err := login(ctx, cfg)
	if err != nil {
		t.Error(err)
	}

	err = prepareTableFact(ctx, cl)
	if err != nil {
		t.Error(err)
	}

	s := New()

	defer clearData(ctx, cl) // nolint:errcheck,nolintlint

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

	wantedKey := sdk.StructuredData{"test": "test1"}

	if !reflect.DeepEqual(r.Key, wantedKey) {
		t.Error(errors.New("wrong record key"))
	}

	// Check teardown.
	err = s.Teardown(ctx)
	if err != nil {
		t.Error(err)
	}
}

func prepareConfig() (map[string]string, error) {
	email := os.Getenv("FIREBOLT_EMAIL")
	password := os.Getenv("FIREBOLT_PASSWORD")
	accountName := os.Getenv("FIREBOLT_ACCOUNT_NAME")
	engineName := os.Getenv("FIREBOLT_ENGINE_NAME")
	db := os.Getenv("FIREBOLT_DB")

	if email == "" || password == "" || accountName == "" || engineName == "" || db == "" {
		return map[string]string{}, errors.New("missed env variable")
	}

	return map[string]string{
		config.KeyEmail:           email,
		config.KeyPassword:        password,
		config.KeyAccountName:     accountName,
		config.KeyEngineName:      engineName,
		config.KeyDB:              db,
		config.KeyTable:           testTable,
		config.KeyOrderingColumns: "id,test",
		config.KeyBatchSize:       "100",
	}, nil
}

func login(ctx context.Context, cfg map[string]string) (*client.Client, error) {
	cl := client.New(ctx, cfg[config.KeyDB])

	err := cl.Login(ctx, client.LoginParams{
		Email:       cfg[config.KeyEmail],
		Password:    cfg[config.KeyPassword],
		AccountName: cfg[config.KeyAccountName],
		EngineName:  cfg[config.KeyEngineName],
	})
	if err != nil {
		return nil, fmt.Errorf("client login: %w", err)
	}

	return cl, nil
}

func prepareTableDemensial(ctx context.Context, cl *client.Client) error {
	// drop table for case it wasn't removed previous time.
	_, err := cl.RunQuery(ctx, queryDropTable)
	if err != nil {
		return err
	}

	// create table.
	_, err = cl.RunQuery(ctx, queryCreateDimensionTable)
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

func prepareTableFact(ctx context.Context, cl *client.Client) error {
	// drop table for case it wasn't removed previous time.
	_, err := cl.RunQuery(ctx, queryDropTable)
	if err != nil {
		return err
	}

	// create table.
	_, err = cl.RunQuery(ctx, queryCreateTableFact)
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

func prepareEmptyTable(ctx context.Context, cl *client.Client) error {
	// create table.
	_, err := cl.RunQuery(ctx, queryCreateDimensionTable)
	if err != nil {
		return err
	}

	return nil
}

func clearData(ctx context.Context, cl *client.Client) error {
	// drop table.
	_, err := cl.RunQuery(ctx, queryDropTable)
	if err != nil {
		return err
	}

	return nil
}
