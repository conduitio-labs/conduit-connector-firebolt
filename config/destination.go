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

package config

import (
	"fmt"
)

// Destination holds destination-related configurable values.
type Destination struct {
	Common
}

// ParseDestination attempts to parse plugins.Config into a Destination struct.
func ParseDestination(cfg map[string]string) (Destination, error) {
	common, err := ParseCommon(cfg)
	if err != nil {
		return Destination{}, fmt.Errorf("parse common config: %w", err)
	}

	destination := Destination{common}

	return destination, nil
}
