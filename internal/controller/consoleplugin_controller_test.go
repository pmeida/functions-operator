/*
Copyright 2025.

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

package controller

import (
	"context"
	"net/http/httptest"
	"strings"
	"sync"

	"github.com/google/go-containerregistry/pkg/crane"
	"github.com/google/go-containerregistry/pkg/registry"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/functions-dev/func-operator/internal/manifests"
)

// imageRegistryServer serves an in-memory OCI registry the image-manifests
// tests push synthetic plugin images to.
var (
	imageRegistryServer *httptest.Server
	imageRegistryOnce   sync.Once
)

func startImageRegistry() {
	imageRegistryOnce.Do(func() {
		imageRegistryServer = httptest.NewServer(registry.New())
	})
}

// ensureInfrastructureStatus creates or updates the singleton Infrastructure
// CR's apiServerURL. Both ConsolePlugin Describes share one envtest API
// server and Ginkgo randomizes spec order, so each BeforeEach must be able
// to (re)assert its own value.
func ensureInfrastructureStatus(ctx context.Context, apiServerURL string) {
	infra := &unstructured.Unstructured{}
	infra.SetAPIVersion(infrastructureAPIVersion)
	infra.SetKind(infrastructureKind)
	infra.SetName(infrastructureName)

	err := k8sClient.Get(ctx, types.NamespacedName{Name: infrastructureName}, infra)
	if apierrors.IsNotFound(err) {
		infra.Object["status"] = map[string]interface{}{"apiServerURL": apiServerURL}
		Expect(k8sClient.Create(ctx, infra)).To(Succeed())
		return
	}
	Expect(err).NotTo(HaveOccurred())
	infra.Object["status"] = map[string]interface{}{"apiServerURL": apiServerURL}
	Expect(k8sClient.Update(ctx, infra)).To(Succeed())
}

// ensureClusterVersionID creates or updates the singleton ClusterVersion
// CR's clusterID.
func ensureClusterVersionID(ctx context.Context, clusterID string) {
	cv := &unstructured.Unstructured{}
	cv.SetAPIVersion(clusterVersionAPIVersion)
	cv.SetKind(clusterVersionKind)
	cv.SetName(clusterVersionName)

	err := k8sClient.Get(ctx, types.NamespacedName{Name: clusterVersionName}, cv)
	if apierrors.IsNotFound(err) {
		cv.Object["spec"] = map[string]interface{}{"clusterID": clusterID}
		Expect(k8sClient.Create(ctx, cv)).To(Succeed())
		return
	}
	Expect(err).NotTo(HaveOccurred())
	cv.Object["spec"] = map[string]interface{}{"clusterID": clusterID}
	Expect(k8sClient.Update(ctx, cv)).To(Succeed())
}

var _ = Describe("ConsolePlugin Controller", func() {
	const (
		operatorNamespace = "test-consoleplugin-ns"
		testImage         = "quay.io/test/faas-console-plugin:latest"
		testAPIServerURL  = "https://api.test-cluster.example.com:6443"
		testClusterID     = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
	)

	var (
		reconciler *ConsolePluginReconciler
		ctx        context.Context
	)

	BeforeEach(func() {
		ctx = context.Background()

		ns := &v1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: operatorNamespace},
		}
		err := k8sClient.Create(ctx, ns)
		if err != nil && !apierrors.IsAlreadyExists(err) {
			Expect(err).NotTo(HaveOccurred())
		}

		reconciler = &ConsolePluginReconciler{
			Client:             k8sClient,
			OperatorNamespace:  operatorNamespace,
			ConsolePluginImage: testImage,
		}

		// Create or update the cluster-facts CRs; the image-manifests
		// Describe shares this envtest API server and asserts different
		// values.
		ensureInfrastructureStatus(ctx, testAPIServerURL)
		ensureClusterVersionID(ctx, testClusterID)
	})

	AfterEach(func() {
		cm := &v1.ConfigMap{}
		err := k8sClient.Get(ctx, types.NamespacedName{
			Name:      controllerConfigName,
			Namespace: operatorNamespace,
		}, cm)
		if err == nil {
			_ = k8sClient.Delete(ctx, cm)
		}

		cp := &unstructured.Unstructured{}
		cp.SetAPIVersion(consolePluginAPIVersion)
		cp.SetKind(consolePluginKind)
		cp.SetName(consolePluginName)
		_ = k8sClient.Delete(ctx, cp)

		deploy := &appsv1.Deployment{}
		if k8sClient.Get(ctx, types.NamespacedName{Name: consolePluginName, Namespace: operatorNamespace}, deploy) == nil {
			_ = k8sClient.Delete(ctx, deploy)
		}

		svc := &v1.Service{}
		if k8sClient.Get(ctx, types.NamespacedName{Name: consolePluginName, Namespace: operatorNamespace}, svc) == nil {
			_ = k8sClient.Delete(ctx, svc)
		}

		sa := &v1.ServiceAccount{}
		if k8sClient.Get(ctx, types.NamespacedName{Name: consolePluginName, Namespace: operatorNamespace}, sa) == nil {
			_ = k8sClient.Delete(ctx, sa)
		}
	})

	createConfigMap := func(data map[string]string) {
		cm := &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      controllerConfigName,
				Namespace: operatorNamespace,
			},
			Data: data,
		}
		Expect(k8sClient.Create(ctx, cm)).To(Succeed())
	}

	getConsolePlugin := func() (*unstructured.Unstructured, error) {
		cp := &unstructured.Unstructured{}
		cp.SetAPIVersion(consolePluginAPIVersion)
		cp.SetKind(consolePluginKind)
		err := k8sClient.Get(ctx, types.NamespacedName{Name: consolePluginName}, cp)
		return cp, err
	}

	getDeployment := func() (*appsv1.Deployment, error) {
		deploy := &appsv1.Deployment{}
		err := k8sClient.Get(ctx, types.NamespacedName{Name: consolePluginName, Namespace: operatorNamespace}, deploy)
		return deploy, err
	}

	getService := func() (*v1.Service, error) {
		svc := &v1.Service{}
		err := k8sClient.Get(ctx, types.NamespacedName{Name: consolePluginName, Namespace: operatorNamespace}, svc)
		return svc, err
	}

	getServiceAccount := func() (*v1.ServiceAccount, error) {
		sa := &v1.ServiceAccount{}
		err := k8sClient.Get(ctx, types.NamespacedName{Name: consolePluginName, Namespace: operatorNamespace}, sa)
		return sa, err
	}

	getRole := func() (*rbacv1.Role, error) {
		role := &rbacv1.Role{}
		err := k8sClient.Get(ctx, types.NamespacedName{Name: consolePluginName, Namespace: operatorNamespace}, role)
		return role, err
	}

	getRoleBinding := func() (*rbacv1.RoleBinding, error) {
		rb := &rbacv1.RoleBinding{}
		err := k8sClient.Get(ctx, types.NamespacedName{Name: consolePluginName, Namespace: operatorNamespace}, rb)
		return rb, err
	}

	doReconcile := func() (reconcile.Result, error) {
		return reconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: consolePluginName},
		})
	}

	Context("isConsolePluginEnabled", func() {
		It("should return false when key is missing from configmap", func() {
			createConfigMap(map[string]string{"autoUpdateMiddleware": "true"})
			enabled, err := reconciler.isConsolePluginEnabled(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(enabled).To(BeFalse())
		})

		It("should return true when consolePluginEnabled is true", func() {
			createConfigMap(map[string]string{consolePluginConfigKey: "true"})
			enabled, err := reconciler.isConsolePluginEnabled(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(enabled).To(BeTrue())
		})

		It("should return false when consolePluginEnabled is false", func() {
			createConfigMap(map[string]string{consolePluginConfigKey: "false"})
			enabled, err := reconciler.isConsolePluginEnabled(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(enabled).To(BeFalse())
		})

		It("should return error when configmap does not exist", func() {
			_, err := reconciler.isConsolePluginEnabled(ctx)
			Expect(err).To(HaveOccurred())
		})

		It("should return error when value is not a valid bool", func() {
			createConfigMap(map[string]string{consolePluginConfigKey: "notabool"})
			_, err := reconciler.isConsolePluginEnabled(ctx)
			Expect(err).To(HaveOccurred())
		})
	})

	Context("getAPIServerURL", func() {
		It("should return the API server URL from Infrastructure CR", func() {
			url, err := reconciler.getAPIServerURL(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(url).To(Equal(testAPIServerURL))
		})
	})

	Context("getClusterID", func() {
		It("should return the cluster ID from ClusterVersion CR", func() {
			id, err := reconciler.getClusterID(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(id).To(Equal(testClusterID))
		})
	})

	Context("Reconcile", func() {
		It("should delete all resources when disabled", func() {
			// The resources are created directly: the enabled path renders
			// them from the plugin image, which the image-sourced manifests
			// Describe covers.
			pluginLabels := map[string]string{"app": consolePluginName}
			meta := metav1.ObjectMeta{Name: consolePluginName, Namespace: operatorNamespace, Labels: pluginLabels}
			Expect(k8sClient.Create(ctx, &v1.ServiceAccount{ObjectMeta: meta})).To(Succeed())
			Expect(k8sClient.Create(ctx, &rbacv1.Role{ObjectMeta: meta})).To(Succeed())
			Expect(k8sClient.Create(ctx, &rbacv1.RoleBinding{
				ObjectMeta: meta,
				RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: consolePluginName},
			})).To(Succeed())
			Expect(k8sClient.Create(ctx, &v1.Service{ObjectMeta: meta, Spec: v1.ServiceSpec{
				Ports: []v1.ServicePort{{Port: 9443}},
			}})).To(Succeed())
			deploy := &appsv1.Deployment{ObjectMeta: meta, Spec: appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{MatchLabels: pluginLabels},
				Template: v1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: pluginLabels},
					Spec:       v1.PodSpec{Containers: []v1.Container{{Name: consolePluginName, Image: "fixture"}}},
				},
			}}
			Expect(k8sClient.Create(ctx, deploy)).To(Succeed())
			cp := &unstructured.Unstructured{}
			cp.SetAPIVersion(consolePluginAPIVersion)
			cp.SetKind(consolePluginKind)
			cp.SetName(consolePluginName)
			Expect(k8sClient.Create(ctx, cp)).To(Succeed())

			createConfigMap(map[string]string{consolePluginConfigKey: "true"})

			cm := &v1.ConfigMap{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name:      controllerConfigName,
				Namespace: operatorNamespace,
			}, cm)).To(Succeed())
			cm.Data[consolePluginConfigKey] = "false"
			Expect(k8sClient.Update(ctx, cm)).To(Succeed())

			_, err := doReconcile()
			Expect(err).NotTo(HaveOccurred())

			_, err = getConsolePlugin()
			Expect(apierrors.IsNotFound(err)).To(BeTrue())

			_, err = getDeployment()
			Expect(apierrors.IsNotFound(err)).To(BeTrue())

			_, err = getService()
			Expect(apierrors.IsNotFound(err)).To(BeTrue())

			_, err = getServiceAccount()
			Expect(apierrors.IsNotFound(err)).To(BeTrue())

			_, err = getRole()
			Expect(apierrors.IsNotFound(err)).To(BeTrue())

			_, err = getRoleBinding()
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})

		It("should not error when deleting non-existent resources", func() {
			createConfigMap(map[string]string{consolePluginConfigKey: "false"})

			result, err := doReconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(reconcile.Result{}))
		})

		It("should not create resources when key is absent", func() {
			createConfigMap(map[string]string{"autoUpdateMiddleware": "true"})

			result, err := doReconcile()
			Expect(err).NotTo(HaveOccurred())
			Expect(result).To(Equal(reconcile.Result{}))

			_, err = getConsolePlugin()
			Expect(apierrors.IsNotFound(err)).To(BeTrue())

			_, err = getDeployment()
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})
	})
})

// imagePluginManifests mirrors the templates shipped in faas-console-plugin's
// manifests/ directory (see internal/manifests for the package under test
// here).
var imagePluginManifests = map[string][]byte{
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
  type: ClusterIP
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
  selector:
    matchLabels:
      app: {{ .Name }}
  template:
    metadata:
      labels:
        app: {{ .Name }}
    spec:
      containers:
        - name: {{ .Name }}
          image: {{ .Image }}
          args:
            - "--https-port=9443"
            - "--external-api-server-url={{ .APIServerURL }}"
            - "--cluster-id={{ .ClusterID }}"
            {{- if or .Config.ghApiUrl .Env.GH_API_URL }}
            - "--gh-api-url={{ or .Config.ghApiUrl .Env.GH_API_URL }}"
            {{- end }}
`),
	"manifests/consoleplugin.yaml": []byte(`apiVersion: console.openshift.io/v1
kind: ConsolePlugin
metadata:
  name: {{ .Name }}
spec:
  backend:
    type: Service
    service:
      name: {{ .Name }}
      namespace: {{ .Namespace }}
      port: 9443
`),
}

var _ = Describe("ConsolePlugin Controller with image-sourced manifests", func() {
	const (
		imageNamespace  = "test-consoleplugin-image-ns"
		imageTestRefTag = "quay.io/test/faas-console-plugin:latest"
		imageAPIServer  = "https://api.image-test.example.com:6443"
		imageClusterID  = "b2c3d4e5-f6a7-8901-bcde-f12345678901"
	)

	var (
		reconciler *ConsolePluginReconciler
		ctx        context.Context
	)

	pushTestImage := func(files map[string][]byte, tag string) string {
		layer, err := crane.Layer(files)
		Expect(err).NotTo(HaveOccurred())
		img, err := mutate.Append(empty.Image, mutate.Addendum{Layer: layer})
		Expect(err).NotTo(HaveOccurred())
		ref := strings.TrimPrefix(imageRegistryServer.URL, "http://") + "/faas-console-plugin:" + tag
		Expect(crane.Push(img, ref, crane.Insecure)).To(Succeed())
		return ref
	}

	BeforeEach(func() {
		ctx = context.Background()
		startImageRegistry()

		ns := &v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: imageNamespace}}
		err := k8sClient.Create(ctx, ns)
		if err != nil && !apierrors.IsAlreadyExists(err) {
			Expect(err).NotTo(HaveOccurred())
		}

		ensureInfrastructureStatus(ctx, imageAPIServer)
		ensureClusterVersionID(ctx, imageClusterID)

		imageRef := pushTestImage(imagePluginManifests, "v1")
		reconciler = &ConsolePluginReconciler{
			Client:             k8sClient,
			OperatorNamespace:  imageNamespace,
			ConsolePluginImage: imageTestRefTag,
			ManifestSource: &manifests.Source{
				ImageRef: imageRef,
				Insecure: true,
				// The dev flow resolves a dockerconfigjson pull secret by
				// name for the internal registry; exercise the read+parse
				// path here (the test registry is anonymous, so the
				// credentials simply go unused).
				PullSecretName: "plugin-registry-auth",
			},
		}

		pullSecret := &v1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "plugin-registry-auth", Namespace: imageNamespace},
			Type:       v1.SecretTypeDockerConfigJson,
			Data: map[string][]byte{
				".dockerconfigjson": []byte(`{"auths":{"image-registry.example.com:5000":{"auth":"dW51c2VkOnMzY3JldA=="}}}`),
			},
		}
		err = k8sClient.Create(ctx, pullSecret)
		if apierrors.IsAlreadyExists(err) {
			Expect(k8sClient.Update(ctx, pullSecret)).To(Succeed())
		} else {
			Expect(err).NotTo(HaveOccurred())
		}
	})

	doReconcile := func() (reconcile.Result, error) {
		return reconciler.Reconcile(ctx, reconcile.Request{
			NamespacedName: types.NamespacedName{Name: consolePluginName},
		})
	}

	It("should render and apply all plugin resources from the image", func() {
		cm := &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: controllerConfigName, Namespace: imageNamespace},
			Data: map[string]string{
				consolePluginConfigKey: "true",
				"ghApiUrl":             "https://fake-gh.test.example.com",
			},
		}
		Expect(k8sClient.Create(ctx, cm)).To(Succeed())
		GinkgoT().Setenv("GH_API_URL", "")

		result, err := doReconcile()
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(reconcile.Result{}))

		By("creating the ConsolePlugin")
		cp := &unstructured.Unstructured{}
		cp.SetAPIVersion(consolePluginAPIVersion)
		cp.SetKind(consolePluginKind)
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: consolePluginName}, cp)).To(Succeed())

		By("creating the Deployment with rendered values")
		deploy := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: consolePluginName, Namespace: imageNamespace}, deploy)).To(Succeed())
		Expect(deploy.Spec.Template.Spec.Containers[0].Image).To(Equal(imageTestRefTag))
		Expect(deploy.Spec.Template.Spec.Containers[0].Args).To(ContainElement("--external-api-server-url=" + imageAPIServer))
		Expect(deploy.Spec.Template.Spec.Containers[0].Args).To(ContainElement("--cluster-id=" + imageClusterID))
		Expect(deploy.Spec.Template.Spec.Containers[0].Args).To(ContainElement("--gh-api-url=https://fake-gh.test.example.com"))
		Expect(*deploy.Spec.Replicas).To(Equal(int32(2)))

		By("creating the Service with the serving-cert annotation")
		svc := &v1.Service{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: consolePluginName, Namespace: imageNamespace}, svc)).To(Succeed())
		Expect(svc.Annotations).To(HaveKeyWithValue("service.alpha.openshift.io/serving-cert-secret-name", consolePluginCertSecret))

		By("creating the RBAC and ServiceAccount")
		sa := &v1.ServiceAccount{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: consolePluginName, Namespace: imageNamespace}, sa)).To(Succeed())
		role := &rbacv1.Role{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: consolePluginName, Namespace: imageNamespace}, role)).To(Succeed())
		Expect(role.Rules).To(HaveLen(1))
		rb := &rbacv1.RoleBinding{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: consolePluginName, Namespace: imageNamespace}, rb)).To(Succeed())
		Expect(rb.RoleRef.Name).To(Equal(consolePluginName))
	})

	AfterEach(func() {
		// Both specs create the controller-config ConfigMap and the plugin
		// resources in the shared envtest API server; spec order is
		// randomized, so each spec must clean up after itself.
		deleteAll := func(obj client.Object) {
			_ = k8sClient.Delete(ctx, obj)
		}
		deleteAll(&v1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: controllerConfigName, Namespace: imageNamespace}})

		cp := &unstructured.Unstructured{}
		cp.SetAPIVersion(consolePluginAPIVersion)
		cp.SetKind(consolePluginKind)
		cp.SetName(consolePluginName)
		deleteAll(cp)
		deleteAll(&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: consolePluginName, Namespace: imageNamespace}})
		deleteAll(&v1.Service{ObjectMeta: metav1.ObjectMeta{Name: consolePluginName, Namespace: imageNamespace}})
		deleteAll(&v1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: consolePluginName, Namespace: imageNamespace}})
		deleteAll(&rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: consolePluginName, Namespace: imageNamespace}})
		deleteAll(&rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: consolePluginName, Namespace: imageNamespace}})
	})

	It("should be idempotent on a cached digest and adopt new definitions from a new image", func() {
		cm := &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: controllerConfigName, Namespace: imageNamespace},
			Data:       map[string]string{consolePluginConfigKey: "true"},
		}
		Expect(k8sClient.Create(ctx, cm)).To(Succeed())
		GinkgoT().Setenv("GH_API_URL", "")

		_, err := doReconcile()
		Expect(err).NotTo(HaveOccurred())

		By("reconciling again without error (digest cache hit)")
		_, err = doReconcile()
		Expect(err).NotTo(HaveOccurred())

		By("adopting new definitions from a new image digest")
		changed := map[string][]byte{}
		for k, v := range imagePluginManifests {
			changed[k] = v
		}
		changed["manifests/service.yaml"] = []byte(`apiVersion: v1
kind: Service
metadata:
  name: {{ .Name }}
  namespace: {{ .Namespace }}
  annotations:
    service.alpha.openshift.io/serving-cert-secret-name: {{ .CertSecret }}
spec:
  type: NodePort
  ports:
    - port: 9443
`)
		reconciler.ManifestSource = &manifests.Source{
			ImageRef: pushTestImage(changed, "v2"),
			Insecure: true,
		}

		_, err = doReconcile()
		Expect(err).NotTo(HaveOccurred())

		svc := &v1.Service{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: consolePluginName, Namespace: imageNamespace}, svc)).To(Succeed())
		Expect(svc.Spec.Type).To(Equal(v1.ServiceTypeNodePort))
	})
})
