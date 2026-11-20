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

// manifests-poc renders the console plugin resources carried by a plugin
// image, using the exact package the operator uses in production
// (internal/manifests). SRVOCF-1095 Option B demo:
//
//	go run ./hack/manifests-poc \
//	  -image localhost:5000/faas-console-plugin:poc -insecure \
//	  -namespace serverless-functions \
//	  -api-server-url https://api.cluster.example.com:6443 \
//	  -cluster-id my-cluster-id
//
// The output can be piped into `oc apply -f -` on a real cluster.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/functions-dev/func-operator/internal/manifests"
	"sigs.k8s.io/yaml"
)

func main() {
	image := flag.String("image", "", "plugin image reference carrying /manifests")
	insecure := flag.Bool("insecure", false, "allow plain-HTTP registries (local dev only)")
	namespace := flag.String("namespace", "serverless-functions", "target namespace")
	name := flag.String("name", "console-functions-plugin", "plugin resource name")
	apiServerURL := flag.String("api-server-url", "", "cluster API server URL")
	clusterID := flag.String("cluster-id", "", "cluster ID")
	ghAPIURL := flag.String("gh-api-url", "", "GitHub API URL (optional)")
	workflowPolling := flag.String("workflow-polling-interval", "", "workflow polling interval (optional)")
	flag.Parse()

	if *image == "" || *apiServerURL == "" || *clusterID == "" {
		fmt.Fprintln(os.Stderr, "-image, -api-server-url and -cluster-id are required")
		os.Exit(1)
	}

	vals := manifests.Values{
		Namespace:    *namespace,
		Name:         *name,
		Image:        *image,
		APIServerURL: *apiServerURL,
		ClusterID:    *clusterID,
		CertSecret:   "faas-console-plugin-cert",
		Config:       map[string]string{},
		Env:          map[string]string{},
	}
	if *ghAPIURL != "" {
		vals.Config["ghApiUrl"] = *ghAPIURL
	}
	if *workflowPolling != "" {
		vals.Config["workflowPollingInterval"] = *workflowPolling
	}

	cache := manifests.NewCache("")
	resources, err := cache.Fetch(context.Background(), manifests.Source{ImageRef: *image, Insecure: *insecure}, vals)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fetching manifests: %v\n", err)
		os.Exit(1)
	}

	for _, res := range resources {
		raw, err := yaml.Marshal(res.Obj.Object)
		if err != nil {
			fmt.Fprintf(os.Stderr, "marshalling %s: %v\n", res.Path, err)
			os.Exit(1)
		}
		fmt.Printf("---\n# from %s\n%s\n", res.Path, raw)
	}

	fmt.Fprintf(os.Stderr, "[manifests-poc] rendered %d resources from %s (%d pull(s))\n",
		len(resources), *image, cache.Pulls())
}
