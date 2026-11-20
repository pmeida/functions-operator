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
	"archive/tar"
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/go-containerregistry/pkg/crane"
	"github.com/google/go-containerregistry/pkg/registry"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// pluginManifests mirrors the templates shipped in faas-console-plugin's
// manifests/ directory, which is the source this package exists to consume.
var pluginManifests = map[string][]byte{
	"manifests/compatibility.yaml": []byte(`formatVersion: "1"
`),
	"manifests/serviceaccount.yaml": []byte(`apiVersion: v1
kind: ServiceAccount
metadata:
  name: {{ .Name }}
  namespace: {{ .Namespace }}
`),
	"manifests/role.yaml": []byte(`apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: {{ .Name }}
  namespace: {{ .Namespace }}
rules:
  - apiGroups: [""]
    resources: ["secrets"]
    verbs: ["create", "get", "update", "delete"]
`),
	"manifests/rolebinding.yaml": []byte(`apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: {{ .Name }}
  namespace: {{ .Namespace }}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: {{ .Name }}
subjects:
  - kind: ServiceAccount
    name: {{ .Name }}
    namespace: {{ .Namespace }}
`),
	"manifests/service.yaml": []byte(`apiVersion: v1
kind: Service
metadata:
  name: {{ .Name }}
  namespace: {{ .Namespace }}
  annotations:
    service.alpha.openshift.io/serving-cert-secret-name: {{ .CertSecret }}
spec:
  ports:
    - port: 9443
`),
	"manifests/deployment.yaml": []byte(`apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ .Name }}
  namespace: {{ .Namespace }}
spec:
  replicas: 2
  template:
    spec:
      containers:
        - name: {{ .Name }}
          image: {{ .Image }}
          args:
            - "--external-api-server-url={{ .APIServerURL }}"
            - "--cluster-id={{ .ClusterID }}"
            {{- if or .Config.ghApiUrl .Env.GH_API_URL }}
            - "--gh-api-url={{ or .Config.ghApiUrl .Env.GH_API_URL }}"
            {{- end }}
            {{- if .Config.workflowPollingInterval }}
            - "--workflow-polling-interval={{ .Config.workflowPollingInterval }}"
            {{- end }}
          livenessProbe:
            initialDelaySeconds: 5
`),
	"manifests/consoleplugin.yaml": []byte(`apiVersion: console.openshift.io/v1
kind: ConsolePlugin
metadata:
  name: {{ .Name }}
spec:
  backend:
    type: Service
`),
}

func testValues() Values {
	return Values{
		Namespace:    "serverless-functions",
		Name:         "console-functions-plugin",
		Image:        "registry.test/faas-console-plugin:v1",
		APIServerURL: "https://api.test-cluster.example.com:6443",
		ClusterID:    "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
		CertSecret:   "faas-console-plugin-cert",
		Config: map[string]string{
			"ghApiUrl": "https://fake-gh.test.example.com",
		},
		Env: map[string]string{"GH_API_URL": ""},
	}
}

// countingRegistry counts blob downloads so tests can assert that a cached
// digest triggers no registry traffic beyond tag resolution.
type countingRegistry struct {
	http.Handler
	blobGets atomic.Int32
}

func (c *countingRegistry) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/blobs/") {
		c.blobGets.Add(1)
	}
	c.Handler.ServeHTTP(w, r)
}

func startRegistry(t *testing.T) (*countingRegistry, *httptest.Server) {
	t.Helper()
	cr := &countingRegistry{Handler: registry.New()}
	server := httptest.NewServer(cr)
	t.Cleanup(server.Close)
	return cr, server
}

// pushImage builds an image from files (a map of in-image paths to content)
// and pushes it to the test registry under tag.
func pushImage(t *testing.T, server *httptest.Server, files map[string][]byte, tag string) string {
	t.Helper()

	layer, err := crane.Layer(files)
	if err != nil {
		t.Fatalf("building layer: %v", err)
	}
	img, err := mutate.Append(empty.Image, mutate.Addendum{Layer: layer})
	if err != nil {
		t.Fatalf("building image: %v", err)
	}

	ref := strings.TrimPrefix(server.URL, "http://") + "/faas-console-plugin:" + tag
	if err := crane.Push(img, ref, crane.Insecure); err != nil {
		t.Fatalf("pushing image: %v", err)
	}
	return ref
}

func findByKind(resources []Resource, kind string) *Resource {
	for i := range resources {
		if resources[i].Obj.GetKind() == kind {
			return &resources[i]
		}
	}
	return nil
}

// deploymentContainer returns the first container of the rendered Deployment.
func deploymentContainer(t *testing.T, res *Resource) map[string]any {
	t.Helper()
	containers, found, err := unstructured.NestedSlice(res.Obj.Object, "spec", "template", "spec", "containers")
	if err != nil || !found || len(containers) == 0 {
		t.Fatalf("reading deployment containers: %v found=%v", err, found)
	}
	container, ok := containers[0].(map[string]any)
	if !ok {
		t.Fatal("deployment container is not a map")
	}
	return container
}

func TestFetchRendersAndValidates(t *testing.T) {
	_, server := startRegistry(t)
	ref := pushImage(t, server, pluginManifests, "v1")

	cache := NewCache(t.TempDir())
	resources, err := cache.Fetch(context.Background(), Source{ImageRef: ref, Insecure: true}, testValues())
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	wantKinds := map[string]bool{
		"ServiceAccount": false, "Role": false, "RoleBinding": false,
		"Service": false, "Deployment": false, "ConsolePlugin": false,
	}
	for _, res := range resources {
		if _, ok := wantKinds[res.Obj.GetKind()]; !ok {
			t.Errorf("unexpected kind %q from %s", res.Obj.GetKind(), res.Path)
			continue
		}
		wantKinds[res.Obj.GetKind()] = true

		if res.Obj.GetKind() != "ConsolePlugin" && res.Obj.GetNamespace() != "serverless-functions" {
			t.Errorf("%s: namespace = %q, want serverless-functions", res.Path, res.Obj.GetNamespace())
		}
		if res.Obj.GetName() != "console-functions-plugin" {
			t.Errorf("%s: name = %q, want console-functions-plugin", res.Path, res.Obj.GetName())
		}
	}
	for kind, seen := range wantKinds {
		if !seen {
			t.Errorf("no %s rendered", kind)
		}
	}

	// Rendered values flow through: cluster facts, image, and config knobs.
	container := deploymentContainer(t, findByKind(resources, "Deployment"))
	var renderedArgs strings.Builder
	for _, a := range container["args"].([]any) {
		renderedArgs.WriteString(a.(string))
		renderedArgs.WriteString(" ")
	}
	for _, want := range []string{
		"--external-api-server-url=https://api.test-cluster.example.com:6443",
		"--cluster-id=a1b2c3d4-e5f6-7890-abcd-ef1234567890",
		"--gh-api-url=https://fake-gh.test.example.com",
	} {
		if !strings.Contains(renderedArgs.String(), want) {
			t.Errorf("deployment args missing %q, got: %s", want, renderedArgs.String())
		}
	}
	if strings.Contains(renderedArgs.String(), "--workflow-polling-interval") {
		t.Errorf("workflow polling interval arg rendered without config value: %s", renderedArgs.String())
	}
	if got := container["image"]; got != "registry.test/faas-console-plugin:v1" {
		t.Errorf("deployment image = %v, want registry.test/faas-console-plugin:v1", got)
	}

	// The serving-cert annotation is rendered onto the Service.
	svc := findByKind(resources, "Service")
	if got := svc.Obj.GetAnnotations()["service.alpha.openshift.io/serving-cert-secret-name"]; got != "faas-console-plugin-cert" {
		t.Errorf("serving cert annotation = %q, want faas-console-plugin-cert", got)
	}
}

func TestFetchRendersEnvFallback(t *testing.T) {
	_, server := startRegistry(t)
	ref := pushImage(t, server, pluginManifests, "v1")

	vals := testValues()
	vals.Config = nil
	vals.Env = map[string]string{"GH_API_URL": "https://env-gh.test.example.com"}

	cache := NewCache(t.TempDir())
	resources, err := cache.Fetch(context.Background(), Source{ImageRef: ref, Insecure: true}, vals)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	container := deploymentContainer(t, findByKind(resources, "Deployment"))
	var renderedArgs strings.Builder
	for _, a := range container["args"].([]any) {
		renderedArgs.WriteString(a.(string))
	}
	if !strings.Contains(renderedArgs.String(), "--gh-api-url=https://env-gh.test.example.com") {
		t.Errorf("env fallback not rendered, args: %s", renderedArgs.String())
	}
}

func TestFetchCachesByDigest(t *testing.T) {
	cr, server := startRegistry(t)
	ref := pushImage(t, server, pluginManifests, "v1")

	cache := NewCache(t.TempDir())
	vals := testValues()
	if _, err := cache.Fetch(context.Background(), Source{ImageRef: ref, Insecure: true}, vals); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	blobGetsAfterFirst := cr.blobGets.Load()
	if blobGetsAfterFirst < 1 {
		t.Fatalf("expected at least one blob download on first fetch, got %d", blobGetsAfterFirst)
	}

	for range 2 {
		if _, err := cache.Fetch(context.Background(), Source{ImageRef: ref, Insecure: true}, vals); err != nil {
			t.Fatalf("Fetch: %v", err)
		}
	}
	if got := cache.Pulls(); got != 1 {
		t.Errorf("pulls = %d, want 1 (unchanged digest must be served from cache)", got)
	}
	if got := cr.blobGets.Load(); got != blobGetsAfterFirst {
		t.Errorf("blob downloads grew from %d to %d on cached digest", blobGetsAfterFirst, cr.blobGets.Load())
	}
}

func TestFetchPullsAgainForNewDigest(t *testing.T) {
	_, server := startRegistry(t)
	ref := pushImage(t, server, pluginManifests, "v1")

	changed := map[string][]byte{}
	for k, v := range pluginManifests {
		changed[k] = v
	}
	changed["manifests/deployment.yaml"] = bytes.Replace(
		pluginManifests["manifests/deployment.yaml"],
		[]byte("initialDelaySeconds: 5"), []byte("initialDelaySeconds: 3"), 1)

	cache := NewCache(t.TempDir())
	vals := testValues()
	if _, err := cache.Fetch(context.Background(), Source{ImageRef: ref, Insecure: true}, vals); err != nil {
		t.Fatalf("Fetch v1: %v", err)
	}

	refV2 := pushImage(t, server, changed, "v2")
	resources, err := cache.Fetch(context.Background(), Source{ImageRef: refV2, Insecure: true}, vals)
	if err != nil {
		t.Fatalf("Fetch v2: %v", err)
	}
	if got := cache.Pulls(); got != 2 {
		t.Errorf("pulls = %d, want 2 (new digest must be pulled)", got)
	}

	container := deploymentContainer(t, findByKind(resources, "Deployment"))
	liveness, found, err := unstructured.NestedMap(container, "livenessProbe")
	if err != nil || !found {
		t.Fatalf("reading livenessProbe: %v found=%v", err, found)
	}
	if got, ok := liveness["initialDelaySeconds"].(float64); !ok || got != 3 {
		t.Errorf("initialDelaySeconds = %v (%T), want 3 (definitions must come from the new image)", liveness["initialDelaySeconds"], liveness["initialDelaySeconds"])
	}
}

func TestFetchRejectsUnknownFormatVersion(t *testing.T) {
	_, server := startRegistry(t)
	ref := pushImage(t, server, map[string][]byte{
		"manifests/compatibility.yaml": []byte(`formatVersion: "2"
`),
	}, "v1")

	cache := NewCache(t.TempDir())
	_, err := cache.Fetch(context.Background(), Source{ImageRef: ref, Insecure: true}, testValues())
	if err == nil || !strings.Contains(err.Error(), "formatVersion") {
		t.Errorf("expected formatVersion rejection, got: %v", err)
	}
}

func TestFetchRequiresCompatibilityFile(t *testing.T) {
	_, server := startRegistry(t)
	ref := pushImage(t, server, map[string][]byte{
		"manifests/service.yaml": pluginManifests["manifests/service.yaml"],
	}, "v1")

	cache := NewCache(t.TempDir())
	_, err := cache.Fetch(context.Background(), Source{ImageRef: ref, Insecure: true}, testValues())
	if err == nil || !strings.Contains(err.Error(), CompatibilityFileName) {
		t.Errorf("expected missing compatibility file rejection, got: %v", err)
	}
}

func TestFetchRejectsDisallowedKind(t *testing.T) {
	_, server := startRegistry(t)
	ref := pushImage(t, server, map[string][]byte{
		"manifests/compatibility.yaml": pluginManifests["manifests/compatibility.yaml"],
		"manifests/evil.yaml": []byte(`apiVersion: batch/v1
kind: CronJob
metadata:
  name: {{ .Name }}
  namespace: {{ .Namespace }}
`),
	}, "v1")

	cache := NewCache(t.TempDir())
	_, err := cache.Fetch(context.Background(), Source{ImageRef: ref, Insecure: true}, testValues())
	if err == nil || !strings.Contains(err.Error(), "allowlist") {
		t.Errorf("expected allowlist rejection, got: %v", err)
	}
}

func TestFetchRejectsForeignNamespace(t *testing.T) {
	_, server := startRegistry(t)
	ref := pushImage(t, server, map[string][]byte{
		"manifests/compatibility.yaml": pluginManifests["manifests/compatibility.yaml"],
		"manifests/service.yaml": []byte(`apiVersion: v1
kind: Service
metadata:
  name: {{ .Name }}
  namespace: kube-system
`),
	}, "v1")

	cache := NewCache(t.TempDir())
	_, err := cache.Fetch(context.Background(), Source{ImageRef: ref, Insecure: true}, testValues())
	if err == nil || !strings.Contains(err.Error(), "kube-system") {
		t.Errorf("expected namespace rejection, got: %v", err)
	}
}

func TestExtractLayerRejectsPathTraversal(t *testing.T) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	content := []byte("evil")
	if err := tw.WriteHeader(&tar.Header{
		Name: "../../evil.txt", Mode: 0o644, Size: int64(len(content)), Typeflag: tar.TypeReg,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}

	// The tar entry resolves outside dest; extraction must contain it.
	dest := t.TempDir()
	if err := extractLayer(&buf, dest); err != nil {
		t.Fatalf("extractLayer: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dest, "evil.txt")); !os.IsNotExist(err) {
		t.Errorf("traversal written inside dest: %v", err)
	}
	if escaped := filepath.Join(dest, "..", "..", "evil.txt"); fileExists(t, escaped) {
		t.Errorf("traversal escaped to %s", escaped)
	}
}

func fileExists(t *testing.T, path string) bool {
	t.Helper()
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false
	}
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return true
}
