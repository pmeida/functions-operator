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
	"testing"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
)

func TestParseDockerConfig(t *testing.T) {
	// kubernetes.io/dockerconfigjson secret payload: {"auths": ...}
	raw := []byte(`{"auths":{
		"image-registry.openshift-image-registry.svc:5000": {"auth": "dW51c2VkOnMzY3JldA=="},
		"https://quay.io": {"auth": "dW51c2VkOnMzY3JldA=="}
	}}`)
	keychain, err := ParseDockerConfig(raw)
	if err != nil {
		t.Fatalf("ParseDockerConfig: %v", err)
	}

	cases := []struct {
		registry string
		wantAuth bool
	}{
		{"image-registry.openshift-image-registry.svc:5000", true},
		{"quay.io", true}, // https:// prefix is stripped
		{"registry.ci.openshift.org", false},
	}
	for _, tc := range cases {
		ref, err := name.ParseReference(tc.registry + "/my-repo/image:tag")
		if err != nil {
			t.Fatal(err)
		}
		auth, err := keychain.Resolve(ref.Context())
		if err != nil {
			t.Fatalf("resolve %s: %v", tc.registry, err)
		}
		if tc.wantAuth {
			cfg, err := auth.Authorization()
			if err != nil {
				t.Fatalf("authorization %s: %v", tc.registry, err)
			}
			if cfg.Username != "unused" || cfg.Password != "s3cret" {
				t.Errorf("%s: got credentials %q:%q, want unused:s3cret", tc.registry, cfg.Username, cfg.Password)
			}
		} else if auth != authn.Anonymous {
			t.Errorf("%s: expected anonymous authenticator", tc.registry)
		}
	}
}

func TestParseDockerConfigRejectsMalformed(t *testing.T) {
	if _, err := ParseDockerConfig([]byte(`{"auths": {"reg": {"auth": "!!!"}}}`)); err == nil {
		t.Error("expected error for non-base64 auth")
	}
}
