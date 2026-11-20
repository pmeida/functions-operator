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

import "fmt"

// allowedKinds bounds what image-provided manifests may create. The
// operator's own RBAC covers exactly these kinds today; extending the set is
// a deliberate operator change (allowlist plus RBAC plus review), not
// boilerplate.
var allowedKinds = map[string]bool{
	"ServiceAccount": true,
	"Role":           true,
	"RoleBinding":    true,
	"Service":        true,
	"ConfigMap":      true,
	"Deployment":     true,
	"ConsolePlugin":  true,
}

// clusterScopedKinds are applied without a namespace.
var clusterScopedKinds = map[string]bool{
	"ConsolePlugin": true,
}

// validate rejects rendered resources the operator is not willing to apply:
// unknown kinds, missing identities, and anything namespaced that targets a
// namespace other than the operator's own.
func validate(res Resource, vals Values) error {
	obj := res.Obj

	if obj.GetAPIVersion() == "" {
		return fmt.Errorf("%s: rendered document has no apiVersion", res.Path)
	}
	kind := obj.GetKind()
	if kind == "" {
		return fmt.Errorf("%s: rendered document has no kind", res.Path)
	}
	if !allowedKinds[kind] {
		return fmt.Errorf("%s: kind %q is not in the operator allowlist, refusing to apply", res.Path, kind)
	}
	if obj.GetName() == "" {
		return fmt.Errorf("%s: rendered %s has no metadata.name", res.Path, kind)
	}

	ns := obj.GetNamespace()
	if clusterScopedKinds[kind] {
		if ns != "" {
			return fmt.Errorf("%s: %s is cluster-scoped but has namespace %q set", res.Path, kind, ns)
		}
		return nil
	}
	if ns != vals.Namespace {
		return fmt.Errorf("%s: rendered %s targets namespace %q, operator only manages %q",
			res.Path, kind, ns, vals.Namespace)
	}
	return nil
}
