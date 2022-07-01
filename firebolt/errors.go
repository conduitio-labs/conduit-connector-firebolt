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
	"errors"
)

var (
	// errInValidHTTPStatusCode occurs when the client gets invalid HTTP status code.
	errInValidHTTPStatusCode = errors.New("invalid http status code")
	// errAccountIDOrEngineIDIsEmpty occurs when the client has empty account id or engine id.
	errAccountIDOrEngineIDIsEmpty = errors.New("account id or engine id is empty, please do login first")
	// errCannotDetermineEngineURL occurs when it's impossible to determine an engine's URL.
	errCannotDetermineEngineURL = errors.New("cannot determine engine url")
)
