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

// Package manifests implements image-sourced plugin resource definitions
// (SRVOCF-1095, Option B): the plugin image carries its resource definitions
// as Go text/template YAML under /manifests, and the operator pulls that
// image in-process - the same approach OLM v1 uses for bundles - renders the
// templates with the values computed during reconciliation, validates the
// result, and applies it.
//
// The design goal is that adding or changing plugin resources, including new
// backend flags, requires changes only in faas-console-plugin: feature knobs
// are read from the operator's controller-config ConfigMap via Values.Config,
// so the operator grows no per-feature code. The operator changes only for
// new cluster facts or for new resource kinds, where the kind allowlist is a
// deliberate security gate rather than boilerplate.
//
// Security boundaries: the plugin image is untrusted data. It is pulled and
// read, never executed. Its rendered output must pass validate before
// anything is applied, and the operator's own RBAC - which is defined by the
// operator's deployment, not by the image - bounds what can be applied
// regardless. Image-provided manifests must therefore not be able to extend
// the operator ServiceAccount's permissions; a follow-up hardening step is
// rejecting RoleBindings whose subjects reference the operator's own
// ServiceAccount.
package manifests

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/crane"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/yaml"
)

const (
	// DirName is the directory inside the plugin image that carries the
	// resource definitions.
	DirName = "manifests"
	// CompatibilityFileName declares the manifest format contract. The
	// operator refuses to apply manifests with a formatVersion it does not
	// understand, so an image from the future fails cleanly instead of
	// half-applying.
	CompatibilityFileName = "compatibility.yaml"
	// SupportedFormatVersion is the manifest format this operator understands.
	SupportedFormatVersion = "1"
)

// Source describes where a plugin's resource definitions come from.
type Source struct {
	// ImageRef is the plugin image reference (tag or digest). Definitions
	// are read from the same image the operator deploys, so definitions and
	// running code always come from one build.
	ImageRef string
	// Insecure allows plain-HTTP registries and skips TLS verification:
	// the internal registry's certificate is signed by the service CA,
	// which the operator pod does not trust. Development and tests only;
	// never set in production.
	Insecure bool
	// PullSecretName names a kubernetes.io/dockerconfigjson secret in the
	// operator's namespace with credentials for pulling ImageRef from
	// authenticated registries - the internal registry always requires
	// them, and the operator's in-process pull cannot use the kubelet's
	// credentials. The reconciler resolves it into Keychain. Empty means
	// anonymous pulls, which is correct for public registries.
	PullSecretName string
	// Keychain authenticates registry pulls; resolved from PullSecretName
	// by the reconciler.
	Keychain authn.Keychain
}

// Values are the template render inputs. The typed fields are stable
// deployment coordinates and cluster facts; Config and Env are generic bags
// so new plugin knobs do not require operator changes.
type Values struct {
	// Deployment coordinates.
	Namespace string
	Name      string
	Image     string
	// Cluster facts read from the Infrastructure and ClusterVersion CRs.
	APIServerURL string
	ClusterID    string
	// CertSecret is the serving-cert secret name annotated onto the plugin
	// Service.
	CertSecret string
	// Config is the operator's controller-config ConfigMap data, verbatim.
	// This is the user-facing knob path (OLM populates it from the
	// Subscription's spec.config.env); templates read feature knobs from it,
	// e.g. {{ .Config.ghApiUrl }}. Missing keys render as empty strings.
	Config map[string]string
	// Env is a deliberately small allowlist of operator environment
	// variables that templates may fall back to, e.g.
	// {{ or .Config.ghApiUrl .Env.GH_API_URL }}. It is never the full
	// environment: image-provided templates must not be able to exfiltrate
	// operator secrets into rendered resources.
	Env map[string]string
}

// Resource is a single rendered and validated manifest document.
type Resource struct {
	// Path names the file inside the image the resource came from, for logs
	// and error messages.
	Path string
	// Obj is the rendered document.
	Obj *unstructured.Unstructured
}

// Cache pulls plugin images in-process and caches their extracted /manifests
// directory by image digest: an unchanged image is never pulled twice, so
// ordinary reconciles perform no layer downloads. This mirrors OLM v1's
// digest-keyed image cache (operator-controller's
// internal/shared/util/image). A Cache is safe for concurrent use.
type Cache struct {
	// BaseDir is the root directory for extracted manifests. When empty it
	// defaults to os.TempDir()/console-plugin-manifests.
	BaseDir string

	mu    sync.Mutex
	dirs  map[string]string // image digest -> extracted manifests directory
	pulls int
}

// NewCache returns a Cache extracting under baseDir.
func NewCache(baseDir string) *Cache {
	return &Cache{BaseDir: baseDir, dirs: map[string]string{}}
}

// Pulls returns the number of image pulls performed, for diagnostics.
func (c *Cache) Pulls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pulls
}

// Fetch resolves src.ImageRef, extracts /manifests (once per digest), checks
// the compatibility contract, renders the templates with vals and validates
// the result. Callers apply the returned resources; this package never talks
// to the Kubernetes API.
func (c *Cache) Fetch(ctx context.Context, src Source, vals Values) ([]Resource, error) {
	dir, err := c.dirFor(ctx, src)
	if err != nil {
		return nil, err
	}

	if err := checkCompatibility(dir); err != nil {
		return nil, err
	}

	resources, err := renderDir(dir, vals)
	if err != nil {
		return nil, err
	}
	for _, res := range resources {
		if err := validate(res, vals); err != nil {
			return nil, err
		}
	}
	return resources, nil
}

// dirFor returns the extracted manifests directory for src's image,
// extracting it first when the digest is not cached.
func (c *Cache) dirFor(ctx context.Context, src Source) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	opts := []crane.Option{crane.WithContext(ctx)}
	if src.Insecure {
		opts = append(opts, crane.Insecure)
	}
	if src.Keychain != nil {
		opts = append(opts, crane.WithAuthFromKeychain(src.Keychain))
	}

	digest, err := crane.Digest(src.ImageRef, opts...)
	if err != nil {
		return "", fmt.Errorf("resolving image digest for %q: %w", src.ImageRef, err)
	}
	if dir, ok := c.dirs[digest]; ok {
		return dir, nil
	}

	img, err := crane.Pull(src.ImageRef, opts...)
	if err != nil {
		return "", fmt.Errorf("pulling image %q: %w", src.ImageRef, err)
	}

	base := c.BaseDir
	if base == "" {
		base = filepath.Join(os.TempDir(), "console-plugin-manifests")
	}
	if err := os.MkdirAll(base, 0o700); err != nil {
		return "", fmt.Errorf("creating manifests cache dir: %w", err)
	}
	extractDir, err := os.MkdirTemp(base, "digest-")
	if err != nil {
		return "", fmt.Errorf("creating manifests extraction dir: %w", err)
	}
	if err := extractLayers(img, extractDir); err != nil {
		_ = os.RemoveAll(extractDir)
		return "", fmt.Errorf("extracting /%s from %q: %w", DirName, src.ImageRef, err)
	}
	dir := filepath.Join(extractDir, DirName)
	if _, err := os.Stat(dir); err != nil {
		_ = os.RemoveAll(extractDir)
		return "", fmt.Errorf("image %q carries no /%s directory: %w", src.ImageRef, DirName, err)
	}

	c.dirs[digest] = dir
	c.pulls++
	// Keep only the newest digest's directory: one plugin image is in play
	// at a time and this bounds disk usage across image changes.
	for d, p := range c.dirs {
		if d != digest {
			_ = os.RemoveAll(p)
			delete(c.dirs, d)
		}
	}
	return dir, nil
}

// checkCompatibility refuses manifests whose declared formatVersion the
// operator does not understand.
func checkCompatibility(dir string) error {
	raw, err := os.ReadFile(filepath.Join(dir, CompatibilityFileName))
	if err != nil {
		return fmt.Errorf("image carries no %s under /%s: %w", CompatibilityFileName, DirName, err)
	}
	var comp struct {
		FormatVersion string `yaml:"formatVersion"`
	}
	if err := yaml.Unmarshal(raw, &comp); err != nil {
		return fmt.Errorf("parsing %s: %w", CompatibilityFileName, err)
	}
	if comp.FormatVersion != SupportedFormatVersion {
		return fmt.Errorf("image uses manifest formatVersion %q, this operator supports %q: refusing to apply",
			comp.FormatVersion, SupportedFormatVersion)
	}
	return nil
}
