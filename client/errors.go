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
	"errors"
)

var (
	// errInValidHTTPStatusCode occurs when the client gets invalid HTTP status code.
	errInValidHTTPStatusCode = errors.New("invalid http status code")
	// errAccountIDOrEngineIDIsEmpty occurs when the client has empty account id or engine id.
	errAccountIDOrEngineIDIsEmpty = errors.New("account id or engine id is empty, login wasn't successful")
	// errCannotDetermineEngineURL occurs when it's impossible to determine an engine's URL.
	errCannotDetermineEngineURL = errors.New("cannot determine engine url")
	// ErrColumnsValuesLenMismatch occurs when trying to insert a row with a different column and value lengths.
	ErrColumnsValuesLenMismatch = errors.New("number of columns must be equal to number of values")
	// ErrCannotCastValueToFloat64 occurs when trying to cast any to float64 but it failed.
	ErrCannotCastValueToFloat64 = errors.New("cannot cast value to float64")
	// ErrCannotCastValueToString occurs when trying to cast any to string but it failed.
	ErrCannotCastValueToString = errors.New("cannot cast value to string")
	// ErrCannotParseTime occurs when trying to cast any to string but it failed.
	ErrCannotParseTime = errors.New("parse time error")
)
