/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package manifests

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/go-containerregistry/pkg/authn"
)

// ParseDockerConfig resolves registry credentials from docker config
// content ({"auths": {"<registry>": {"auth": "<base64 user:password>"}}}),
// the format kubernetes.io/dockerconfigjson secrets, `docker login` and
// `oc registry login` use. Only basic-auth entries are supported;
// credential helpers and identity tokens are not.
func ParseDockerConfig(raw []byte) (authn.Keychain, error) {
	var cfg struct {
		Auths map[string]struct {
			Auth string `json:"auth"`
		} `json:"auths"`
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("parsing docker config: %w", err)
	}

	creds := fileKeychain{}
	for registry, entry := range cfg.Auths {
		decoded, err := base64.StdEncoding.DecodeString(entry.Auth)
		if err != nil {
			return nil, fmt.Errorf("decoding auth for registry %s: %w", registry, err)
		}
		username, password, ok := strings.Cut(string(decoded), ":")
		if !ok {
			return nil, fmt.Errorf("auth for registry %s is not user:password", registry)
		}
		registry = strings.TrimPrefix(strings.TrimPrefix(registry, "https://"), "http://")
		creds[registry] = authn.AuthConfig{Username: username, Password: password}
	}
	return creds, nil
}

// fileKeychain maps registry host[:port] to credentials, falling back to
// anonymous for unknown registries.
type fileKeychain map[string]authn.AuthConfig

func (f fileKeychain) Resolve(res authn.Resource) (authn.Authenticator, error) {
	if cfg, ok := f[res.RegistryStr()]; ok {
		return authn.FromConfig(cfg), nil
	}
	return authn.Anonymous, nil
}
