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
	"fmt"
	"os"
	"testing"

	"github.com/conduitio-labs/conduit-connector-firebolt/client"
	"github.com/conduitio-labs/conduit-connector-firebolt/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

const (
	testTable        = "CONDUIT_INTEGRATION_TEST_DESTINATION_TABLE"
	queryCreateTable = "CREATE DIMENSION TABLE CONDUIT_INTEGRATION_TEST_DESTINATION_TABLE (id TEXT, test TEXT)"
	queryDropTable   = "DROP TABLE IF EXISTS CONDUIT_INTEGRATION_TEST_DESTINATION_TABLE"
)

func TestDestination_Write_Success(t *testing.T) {
	is := is.New(t)

	cfg, err := prepareConfig()
	if err != nil {
		t.Skip()
	}

	ctx := context.Background()

	err = prepareTable(ctx, cfg)
	is.NoErr(err)

	d := new(Destination)

	t.Cleanup(func() {
		if err := clearData(ctx, cfg); err != nil {
			t.Error(err)
		}
	})

	err = d.Configure(ctx, cfg)
	is.NoErr(err)

	err = d.Open(ctx)
	is.NoErr(err)

	err = d.Write(ctx, sdk.Record{
		Payload: sdk.StructuredData{
			"id":   "1",
			"test": "hellp",
		},
	})
	is.NoErr(err)

	err = d.Teardown(ctx)
	is.NoErr(err)
}

func TestDestination_Write_Failed(t *testing.T) {
	is := is.New(t)

	cfg, err := prepareConfig()
	if err != nil {
		t.Skip()
	}

	ctx := context.Background()

	err = prepareTable(ctx, cfg)
	is.NoErr(err)

	d := new(Destination)

	t.Cleanup(func() {
		err := clearData(ctx, cfg)
		is.NoErr(err)
	})

	err = d.Configure(ctx, cfg)
	is.NoErr(err)

	err = d.Open(ctx)
	is.NoErr(err)

	err = d.Write(ctx, sdk.Record{
		Payload: sdk.StructuredData{
			// non-existent column "name"
			"name": "bob",
			"test": "hellp",
		},
	})
	is.Equal(err != nil, true)

	err = d.Teardown(ctx)
	is.NoErr(err)
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
		config.KeyEmail:       email,
		config.KeyPassword:    password,
		config.KeyAccountName: accountName,
		config.KeyEngineName:  engineName,
		config.KeyDB:          db,
		config.KeyTable:       testTable,
	}, nil
}

func prepareTable(ctx context.Context, cfg map[string]string) error {
	cl := client.NewClient(ctx, cfg[config.KeyDB])

	err := cl.Login(ctx, client.LoginParams{
		Email:       cfg[config.KeyEmail],
		Password:    cfg[config.KeyPassword],
		AccountName: cfg[config.KeyAccountName],
		EngineName:  cfg[config.KeyEngineName],
	})
	if err != nil {
		return fmt.Errorf("client login: %w", err)
	}

	// create table.
	_, err = cl.RunQuery(ctx, queryCreateTable)
	if err != nil {
		return err
	}

	return nil
}

func clearData(ctx context.Context, cfg map[string]string) error {
	cl := client.NewClient(ctx, cfg[config.KeyDB])

	err := cl.Login(ctx, client.LoginParams{
		Email:       cfg[config.KeyEmail],
		Password:    cfg[config.KeyPassword],
		AccountName: cfg[config.KeyAccountName],
		EngineName:  cfg[config.KeyEngineName],
	})
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
