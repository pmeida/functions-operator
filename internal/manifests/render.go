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
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"text/template"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/yaml"
)

// renderDir renders every *.yaml file in dir, except the compatibility
// contract, with vals. Files are processed in sorted order for deterministic
// output. Each file must contain exactly one YAML document.
func renderDir(dir string, vals Values) ([]Resource, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("reading manifests directory: %w", err)
	}

	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".yaml" {
			continue
		}
		if entry.Name() == CompatibilityFileName {
			continue
		}
		names = append(names, entry.Name())
	}
	sort.Strings(names)

	resources := make([]Resource, 0, len(names))
	for _, name := range names {
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return nil, err
		}

		tmpl, err := template.New(name).Parse(string(raw))
		if err != nil {
			return nil, fmt.Errorf("parsing template %s: %w", name, err)
		}
		var rendered bytes.Buffer
		if err := tmpl.Execute(&rendered, vals); err != nil {
			return nil, fmt.Errorf("rendering %s: %w", name, err)
		}

		obj := &unstructured.Unstructured{}
		if err := yaml.Unmarshal(rendered.Bytes(), &obj.Object); err != nil {
			return nil, fmt.Errorf("rendered %s is not valid YAML: %w", name, err)
		}
		resources = append(resources, Resource{Path: name, Obj: obj})
	}
	return resources, nil
}
