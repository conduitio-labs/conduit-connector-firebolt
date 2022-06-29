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
	"io"
	"net/http"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/hashicorp/go-retryablehttp"
)

const (
	loginURL = "https://api.app.firebolt.io/auth/v1/login"
	//nolint:gosec // refreshTokenURL is a public url, we don't need to hide it
	refreshTokenURL = "https://api.app.firebolt.io/auth/v1/refresh"

	// retryMax is the maximum number of retries.
	retryMax = 3
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
	client := &HTTPClient{
		databaseEngine: databaseEngine,
		dbName:         dbName,
	}

	retryClient := retryablehttp.NewClient()
	retryClient.RetryMax = retryMax
	retryClient.Logger = sdk.Logger(ctx)
	retryClient.CheckRetry = client.checkRetry
	client.httpClient = retryClient.StandardClient()

	return client
}

// Login login to firebolt.
func (h *HTTPClient) Login(ctx context.Context, email, password string) error {
	accountData, err := json.Marshal(loginRequest{
		Username: email,
		Password: password,
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

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read body: %w", err)
	}

	var loginResponse loginResponse
	if err = json.Unmarshal(body, &loginResponse); err != nil {
		return fmt.Errorf("unmarshal login response: %w", err)
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

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	return body, nil
}

// RefreshToken performs a refresh token request.
// The method set the *HTTPClient.accessToken field to the new access token.
func (h *HTTPClient) RefreshToken(ctx context.Context) error {
	refreshTokenRequest := refreshTokenRequest{
		RefreshToken: h.refreshToken,
	}

	buffer := bytes.NewBuffer(nil)
	if err := json.NewEncoder(buffer).Encode(&refreshTokenRequest); err != nil {
		return fmt.Errorf("encode refresh token request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, refreshTokenURL, buffer)
	if err != nil {
		return fmt.Errorf("create refresh token request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json;charset=UTF-8")

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("do refresh token request: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%w, %d", errInValidHTTPStatusCode, resp.StatusCode)
	}

	defer resp.Body.Close()

	var loginResp loginResponse
	if err := json.NewDecoder(resp.Body).Decode(&resp); err != nil {
		return fmt.Errorf("decode refresh token response: %w", err)
	}

	h.accessToken = loginResp.AccessToken

	return nil
}

// checkRetry specifies the policy for handling retries, and is called after each request.
// This is a custom check retry function for the retryablehttp client.
func (h *HTTPClient) checkRetry(ctx context.Context, resp *http.Response, err error) (bool, error) {
	// do not retry on context.Canceled or context.DeadlineExceeded
	if ctx.Err() != nil {
		return false, ctx.Err()
	}

	// don't propagate other errors
	shouldRetry, _ := retryablehttp.DefaultRetryPolicy(ctx, resp, err)
	if shouldRetry {
		return true, nil
	}

	if resp != nil && resp.StatusCode == http.StatusUnauthorized {
		if err := h.RefreshToken(ctx); err != nil {
			return true, fmt.Errorf("refresh token: %w", err)
		}

		// set Authorization header to the newly created access token
		resp.Request.Header.Set("Authorization", fmt.Sprintf("Bearer %s", h.accessToken))

		// shouldRetry is true cause we need to retry one more time with the new access token
		return true, nil
	}

	return false, nil
}

func (h *HTTPClient) Close(ctx context.Context) {
	h.httpClient.CloseIdleConnections()
}
