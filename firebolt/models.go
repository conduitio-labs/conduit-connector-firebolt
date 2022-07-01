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

const (
	// EngineStartedStatus represents a status of a running engine.
	EngineStartedStatus = "ENGINE_STATUS_RUNNING_REVISION_SERVING"
)

// loginRequest is a request model for the login route.
type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// firebolt response after login request.
type loginResponse struct {
	AccessToken  string `json:"access_token"`
	ExpiresIn    int    `json:"expires_in"`
	RefreshToken string `json:"refresh_token"`
	Scope        string `json:"scope"`
	TokenType    string `json:"token_type"`
}

// refreshTokenRequest is a request model for the refresh token route.
type refreshTokenRequest struct {
	RefreshToken string `json:"refresh_token"`
}

// getAccountByIDResponse is a response model for the get account id by name route.
type getAccountIDByNameResponse struct {
	AccountID string `json:"account_id"`
}

// getEngineIDByNameResponse is a response model for the get engine id by name route.
type getEngineIDByNameResponse struct {
	EngineID engineID `json:"engine_id"`
}

// engineID is a little wrapper for engine id.
type engineID struct {
	EngineID string `json:"engine_id"`
}

// engineResponse is a response model for get engine by id and start engine routes.
type engineResponse struct {
	Engine engine `json:"engine"`
}

// engine holds all fields of the Firebolt engine model.
type engine struct {
	CurrentStatus string `json:"current_status"`
}

// getEngineURLByNameResponse is a response model for get engine url by name route.
type getEngineURLByNameResponse struct {
	Edges []edge `json:"edges"`
}

// edge represents an Edge model used for getEngineURLByNameResponse.
type edge struct {
	Node node `json:"node"`
}

// node represents an Edge's node.
type node struct {
	Endpoint string `json:"endpoint"`
}
