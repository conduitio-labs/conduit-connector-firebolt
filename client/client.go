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
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/hashicorp/go-retryablehttp"
)

const (
	baseURL            = "https://api.app.firebolt.io"
	loginURL           = baseURL + "/auth/v1/login"
	refreshTokenURL    = baseURL + "/auth/v1/refresh"
	accountIDByNameURL = baseURL + "/iam/v2/accounts:getIdByName?account_name=%s"
	engineIDByNameURL  = baseURL + "/core/v1/accounts/%s/engines:getIdByName?engine_name=%s"
	engineURLByNameURL = baseURL + "/core/v1/accounts/%s/engines?filter.name_contains=%s"
	engineByIDURL      = baseURL + "/core/v1/accounts/%s/engines/%s"
	startEngineURL     = baseURL + "/core/v1/accounts/%s/engines/%s:start"

	// retryMax is the maximum number of retries.
	retryMax = 3
	// engineStatusCheckTimeout is a timeout for checking engine status.
	engineStatusCheckTimeout = time.Second * 5
)

// Client client for calls to firebolt.
type Client struct {
	accessToken    string
	refreshToken   string
	accountID      string
	accountName    string
	engineID       string
	engineName     string
	engineEndpoint string
	dbName         string

	httpClient *http.Client
}

// New creates new instance of the Client.
func New(ctx context.Context, dbName string) *Client {
	client := &Client{
		dbName: dbName,
	}

	retryClient := retryablehttp.NewClient()
	retryClient.RetryMax = retryMax
	retryClient.Logger = sdk.Logger(ctx)
	retryClient.CheckRetry = client.checkRetry
	client.httpClient = retryClient.StandardClient()

	return client
}

// LoginParams is an incoming params for the Login method.
type LoginParams struct {
	Email       string
	Password    string
	AccountName string
	EngineName  string
}

// Login logins to firebolt.
func (c *Client) Login(ctx context.Context, params LoginParams) error {
	loginRequest := loginRequest{
		Username: params.Email,
		Password: params.Password,
	}

	req, err := c.newRequest(ctx, http.MethodPost, loginURL, &loginRequest)
	if err != nil {
		return fmt.Errorf("create login request: %w", err)
	}

	var loginResponse loginResponse
	_, err = c.do(ctx, req, &loginResponse)
	if err != nil {
		return fmt.Errorf("do login request: %w", err)
	}

	c.accessToken = loginResponse.AccessToken
	c.refreshToken = loginResponse.RefreshToken
	c.accountName = params.AccountName
	c.engineName = params.EngineName

	c.accountID, err = c.getAccountIDByName(ctx)
	if err != nil {
		return fmt.Errorf("get account id by name: %w", err)
	}

	c.engineID, err = c.getEngineIDByName(ctx)
	if err != nil {
		return fmt.Errorf("get engine id by name: %w", err)
	}

	c.engineEndpoint, err = c.getEngineURLByName(ctx)
	if err != nil {
		return fmt.Errorf("get engine url by name: %w", err)
	}

	return nil
}

// StartEngine starts a Firebolt engine and returns
// a bool indicating whether the engine is started or not.
func (c *Client) StartEngine(ctx context.Context) (bool, error) {
	if c.accountID == "" || c.engineID == "" {
		return false, errAccountIDOrEngineIDIsEmpty
	}

	req, err := c.newRequest(ctx, http.MethodPost, fmt.Sprintf(startEngineURL, c.accountID, c.engineID), nil)
	if err != nil {
		return false, fmt.Errorf("create start engine request: %w", err)
	}

	var engineResponse engineResponse
	_, err = c.do(ctx, req, &engineResponse)
	if err != nil {
		return false, fmt.Errorf("do start engine request: %w", err)
	}

	isEngineStarted := engineResponse.Engine.CurrentStatus == EngineStartedStatus

	return isEngineStarted, nil
}

// RunQuery runs an SQL query.
func (c *Client) RunQuery(ctx context.Context, query string) ([]byte, error) {
	b := bytes.NewBuffer([]byte(query))

	req, err := c.newRequest(ctx, http.MethodPost, fmt.Sprintf("https://%s/?database=%s",
		c.engineEndpoint, c.dbName), b)
	if err != nil {
		return nil, fmt.Errorf("create run query request: %w", err)
	}

	buff := bytes.NewBuffer(nil)
	_, err = c.do(ctx, req, buff)
	if err != nil {
		return nil, fmt.Errorf("do run query request: %w", err)
	}

	return buff.Bytes(), nil
}

// GetEngineStatus returns the current status of the underlying engine.
func (c *Client) GetEngineStatus(ctx context.Context) (string, error) {
	if c.accountID == "" || c.engineID == "" {
		return "", errAccountIDOrEngineIDIsEmpty
	}

	engineResponse, err := c.getEngineByID(ctx)
	if err != nil {
		return "", fmt.Errorf("get engine by id: %w", err)
	}

	return engineResponse.Engine.CurrentStatus, nil
}

// RefreshToken performs a refresh token request.
// The method set the *Client.accessToken field to the new access token.
func (c *Client) RefreshToken(ctx context.Context) error {
	refreshTokenRequest := refreshTokenRequest{
		RefreshToken: c.refreshToken,
	}

	req, err := c.newRequest(ctx, http.MethodPost, refreshTokenURL, &refreshTokenRequest)
	if err != nil {
		return fmt.Errorf("create refresh token request: %w", err)
	}

	var loginResp loginResponse
	_, err = c.do(ctx, req, &loginResp)
	if err != nil {
		return fmt.Errorf("do refresh token request: %w", err)
	}

	c.accessToken = loginResp.AccessToken

	return nil
}

// WaitEngineStarted periodically checks the engine status,
// and if the status is equal to ENGINE_STATUS_RUNNING_REVISION_SERVING or ctx is canceled returns.
// It's a blocking method.
func (c *Client) WaitEngineStarted(ctx context.Context) error {
	ticker := time.NewTicker(engineStatusCheckTimeout)

	for {
		select {
		case <-ctx.Done():
			return nil

		case <-ticker.C:
			engineStatus, err := c.GetEngineStatus(ctx)
			if err != nil {
				return fmt.Errorf("get engine status: %w", err)
			}

			sdk.Logger(ctx).Info().Fields(map[string]any{
				"engine_status": engineStatus,
			}).Msgf("checking firebolt engine status")

			if engineStatus == EngineStartedStatus {
				return nil
			}
		}
	}
}

// Close closes the HTTP client connections.
func (c *Client) Close(ctx context.Context) {
	c.httpClient.CloseIdleConnections()
}

// getAccountIDByName returns an account id by its name.
func (c *Client) getAccountIDByName(ctx context.Context) (string, error) {
	req, err := c.newRequest(ctx, http.MethodGet, fmt.Sprintf(accountIDByNameURL, c.accountName), nil)
	if err != nil {
		return "", fmt.Errorf("create get account id request: %w", err)
	}

	var getAccountByIDResponse getAccountIDByNameResponse
	_, err = c.do(ctx, req, &getAccountByIDResponse)
	if err != nil {
		return "", fmt.Errorf("do get account id request: %w", err)
	}

	return getAccountByIDResponse.AccountID, nil
}

// getEngineURLByName returns an engine URL by its name.
func (c *Client) getEngineURLByName(ctx context.Context) (string, error) {
	req, err := c.newRequest(ctx, http.MethodGet, fmt.Sprintf(engineURLByNameURL, c.accountID, c.engineName), nil)
	if err != nil {
		return "", fmt.Errorf("create get engine id request: %w", err)
	}

	var getEngineURLByNameResponse getEngineURLByNameResponse
	_, err = c.do(ctx, req, &getEngineURLByNameResponse)
	if err != nil {
		return "", fmt.Errorf("do get engine id request: %w", err)
	}

	if len(getEngineURLByNameResponse.Edges) == 0 {
		return "", errCannotDetermineEngineURL
	}

	// get the first edge and returns its URL
	return getEngineURLByNameResponse.Edges[0].Node.Endpoint, nil
}

// getEngineIDByName returns an engine id by its name.
func (c *Client) getEngineIDByName(ctx context.Context) (string, error) {
	req, err := c.newRequest(ctx, http.MethodGet, fmt.Sprintf(engineIDByNameURL, c.accountID, c.engineName), nil)
	if err != nil {
		return "", fmt.Errorf("create get engine id request: %w", err)
	}

	var getEngineIDByNameResponse getEngineIDByNameResponse
	_, err = c.do(ctx, req, &getEngineIDByNameResponse)
	if err != nil {
		return "", fmt.Errorf("do get engine id request: %w", err)
	}

	return getEngineIDByNameResponse.EngineID.EngineID, nil
}

// getEngineByID returns engineResponse.
func (c *Client) getEngineByID(ctx context.Context) (*engineResponse, error) {
	req, err := c.newRequest(ctx, http.MethodGet, fmt.Sprintf(engineByIDURL, c.accountID, c.engineID), nil)
	if err != nil {
		return nil, fmt.Errorf("create get engine id request: %w", err)
	}

	var engineResponse engineResponse
	_, err = c.do(ctx, req, &engineResponse)
	if err != nil {
		return nil, fmt.Errorf("do get engine id request: %w", err)
	}

	return &engineResponse, nil
}

// NewRequest creates an API request.
func (c *Client) newRequest(ctx context.Context, method, url string, body interface{}) (*http.Request, error) {
	var (
		buf        io.ReadWriter
		bodyIsJSON bool
	)

	if body != nil {
		switch body := body.(type) {
		case nil:
		case io.ReadWriter:
			buf = body

		default:
			buf = &bytes.Buffer{}
			if err := json.NewEncoder(buf).Encode(body); err != nil {
				return nil, fmt.Errorf("encode request body: %w", err)
			}

			bodyIsJSON = true
		}
	}

	req, err := http.NewRequestWithContext(ctx, method, url, buf)
	if err != nil {
		return nil, fmt.Errorf("create new request: %w", err)
	}

	if body != nil && bodyIsJSON {
		req.Header.Set("Content-Type", "application/json;charset=UTF-8")
	}

	if c.accessToken != "" {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.accessToken))
	}

	return req, nil
}

// do sends an API request and returns the API response. The API response is
// JSON decoded and stored in the value pointed to by out, or returned as an
// error if an API error has occurred.
func (c *Client) do(_ context.Context, req *http.Request, out interface{}) (*http.Response, error) {
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return resp, err
	}

	if resp.StatusCode != http.StatusOK {
		return resp, fmt.Errorf("%w, %d", errInValidHTTPStatusCode, resp.StatusCode)
	}

	defer resp.Body.Close()

	switch out := out.(type) {
	case nil:
	case io.Writer:
		_, err = io.Copy(out, resp.Body)
		if err != nil {
			return resp, fmt.Errorf("decode response body: %w", err)
		}

	default:
		err := json.NewDecoder(resp.Body).Decode(out)
		if err != nil && !errors.Is(err, io.EOF) {
			return resp, fmt.Errorf("decode response body: %w", err)
		}
	}

	return resp, nil
}

// checkRetry specifies the policy for handling retries, and is called after each request.
// This is a custom check retry function for the retryablehttp client.
func (c *Client) checkRetry(ctx context.Context, resp *http.Response, err error) (bool, error) {
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
		if err := c.RefreshToken(ctx); err != nil {
			return true, fmt.Errorf("refresh token: %w", err)
		}

		// set Authorization header to the newly created access token
		resp.Request.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.accessToken))

		// shouldRetry is true cause we need to retry one more time with the new access token
		return true, nil
	}

	return false, nil
}
