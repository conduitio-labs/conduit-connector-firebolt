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

package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/hashicorp/go-retryablehttp"
)

const (
	loginURL = "https://api.app.firebolt.io/auth/v1/login"
)

// HTTPClient client for calls to firebolt.
type HTTPClient struct {
	accessToken    string
	refreshToken   string
	databaseEngine string
	dbName         string

	httpClient *http.Client
}

func New(ctx context.Context, databaseEngine, dbName string) *HTTPClient {
	retryClient := retryablehttp.NewClient()
	retryClient.RetryMax = 3
	retryClient.Logger = sdk.Logger(ctx)

	return &HTTPClient{
		databaseEngine: databaseEngine,
		dbName:         dbName,
		httpClient:     retryClient.StandardClient(),
	}
}

// Login login to firebolt.
func (h *HTTPClient) Login(ctx context.Context, email, password string) error {
	accountData, err := json.Marshal(map[string]string{
		"username": email,
		"password": password,
	})
	if err != nil {
		return fmt.Errorf("marshal body: %w", err)
	}

	b := bytes.NewBuffer(accountData)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, loginURL, b)
	if err != nil {
		return fmt.Errorf("login request: %w", err)
	}

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("do request")
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%w, %d", errInValidHTTPStatusCode, resp.StatusCode)
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read body: %w", err)
	}

	var loginResponse loginResponse

	if err = json.Unmarshal(body, &loginResponse); err != nil {
		return fmt.Errorf("unmarshal login response")
	}

	h.accessToken = loginResponse.AccessToken
	h.refreshToken = loginResponse.RefreshToken

	return nil
}

// RunQuery - run query.
func (h *HTTPClient) RunQuery(ctx context.Context, query string) ([]byte, error) {
	b := bytes.NewBuffer([]byte(query))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("https://%s/?database=%s",
		h.databaseEngine, h.dbName), b)
	if err != nil {
		return nil, fmt.Errorf("new request: %w", err)
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", h.accessToken))

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w, %d", errInValidHTTPStatusCode, resp.StatusCode)
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	return body, nil
}

func (h *HTTPClient) Close(ctx context.Context) {
	h.httpClient.CloseIdleConnections()
}
