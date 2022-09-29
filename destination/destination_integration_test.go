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

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"

	"github.com/conduitio-labs/conduit-connector-firebolt/client"
	"github.com/conduitio-labs/conduit-connector-firebolt/config"
)

const (
	testTable        = "CONDUIT_INTEGRATION_TEST_DESTINATION_TABLE"
	queryCreateTable = "CREATE DIMENSION TABLE CONDUIT_INTEGRATION_TEST_DESTINATION_TABLE" +
		" (id string, name TEXT)"
	queryDropTable = "DROP TABLE IF EXISTS CONDUIT_INTEGRATION_TEST_DESTINATION_TABLE"
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
		if err = clearData(ctx, cfg); err != nil {
			t.Error(err)
		}
	})

	err = d.Configure(ctx, cfg)
	is.NoErr(err)

	err = d.Open(ctx)
	is.NoErr(err)

	rc1 := map[string]any{
		"id":   "1",
		"name": "vasyl",
	}

	rc2 := map[string]any{
		"id":   "2",
		"name": "petro",
	}

	count, err := d.Write(ctx, []sdk.Record{
		{Payload: sdk.Change{After: sdk.StructuredData(rc1)},
			Operation: sdk.OperationSnapshot,
		},
		{Payload: sdk.Change{After: sdk.StructuredData(rc2)},
			Operation: sdk.OperationCreate,
		},
	},
	)

	is.NoErr(err)

	is.Equal(count, 2)

	err = d.Teardown(ctx)
	is.NoErr(err)

	// check data in firebolt
	data, err := d.client.GetRows(ctx, cfg[config.KeyTable], []string{cfg[config.KeyPrimaryKey]}, nil, 2, 0)

	is.Equal(data[0], rc1)
	is.Equal(data[1], rc2)
}

func TestDestination_Write_Failed_Wrong_Column_Name(t *testing.T) {
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
		err = clearData(ctx, cfg)
		is.NoErr(err)
	})

	err = d.Configure(ctx, cfg)
	is.NoErr(err)

	err = d.Open(ctx)
	is.NoErr(err)

	_, err = d.Write(ctx, []sdk.Record{
		{Payload: sdk.Change{After: sdk.StructuredData{
			"id":   "1",
			"test": "test",
		}},
			Operation: sdk.OperationSnapshot},
		{Payload: sdk.Change{After: sdk.StructuredData{
			"id":   "2",
			"test": "test2",
		}},
			Operation: sdk.OperationSnapshot},
	},
	)

	is.True(err != nil)

	err = d.Teardown(ctx)
	is.NoErr(err)
}

func prepareConfig() (map[string]string, error) {
	email := os.Getenv("FIREBOLT_EMAIL")
	if email == "" {
		return map[string]string{}, errors.New("missed env variable 'FIREBOLT_EMAIL'")
	}

	password := os.Getenv("FIREBOLT_PASSWORD")
	if password == "" {
		return map[string]string{}, errors.New("missed env variable 'FIREBOLT_PASSWORD'")
	}

	accountName := os.Getenv("FIREBOLT_ACCOUNT_NAME")
	if accountName == "" {
		return map[string]string{}, errors.New("missed env variable 'FIREBOLT_ACCOUNT_NAME'")
	}

	engineName := os.Getenv("FIREBOLT_ENGINE_NAME")
	if engineName == "" {
		return map[string]string{}, errors.New("missed env variable 'FIREBOLT_ENGINE_NAME'")
	}

	db := os.Getenv("FIREBOLT_DB")
	if db == "" {
		return map[string]string{}, errors.New("missed env variable 'FIREBOLT_DB'")
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
	cl := client.New(ctx, cfg[config.KeyDB])

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
	cl := client.New(ctx, cfg[config.KeyDB])

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
