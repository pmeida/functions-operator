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
	"fmt"
	"os"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/functions-dev/func-operator/internal/manifests"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	consolePluginName       = "console-functions-plugin"
	consolePluginConfigKey  = "consolePluginEnabled"
	consolePluginPort       = int64(9443)
	consolePluginPortInt32  = int32(9443)
	consolePluginBasePath   = "/"
	consolePluginAPIVersion = "console.openshift.io/v1"
	consolePluginKind       = "ConsolePlugin"
	consolePluginCertSecret = "faas-console-plugin-cert"
	consolePluginCertMount  = "/var/cert"

	infrastructureAPIVersion = "config.openshift.io/v1"
	infrastructureKind       = "Infrastructure"
	infrastructureName       = "cluster"

	clusterVersionAPIVersion = "config.openshift.io/v1"
	clusterVersionKind       = "ClusterVersion"
	clusterVersionName       = "version"
)

// ConsolePluginReconciler reconciles the faas-console-plugin resources
// (ConsolePlugin, Deployment, Service, ServiceAccount) based on the
// operator's controller-config ConfigMap.
type ConsolePluginReconciler struct {
	client.Client
	DirectReader       client.Reader
	OperatorNamespace  string
	ConsolePluginImage string
	PodName            string

	// ManifestSource, when set, renders the plugin resources from the
	// plugin image instead of the Go builders below (SRVOCF-1095, Option
	// B). The image is pulled in-process and cached by digest; adding or
	// changing plugin resources is then a faas-console-plugin change only.
	// Nil keeps the builder path.
	ManifestSource *manifests.Source

	manifestsCache *manifests.Cache

	ownerInfoResolved  bool
	deploymentOwnerRef *metav1.OwnerReference
	clusterOwnerRef    *metav1.OwnerReference
}

func (r *ConsolePluginReconciler) Reconcile(ctx context.Context, _ ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if err := r.resolveOLMOwnership(ctx); err != nil {
		logger.Error(err, "Failed to resolve OLM ownership")
		return ctrl.Result{}, err
	}

	enabled, err := r.isConsolePluginEnabled(ctx)
	if err != nil {
		logger.Error(err, "Failed to read console plugin config")
		return ctrl.Result{}, err
	}

	if enabled {
		if r.ManifestSource == nil {
			// Unreachable when deployed: main.go always constructs the
			// source and refuses to start without CONSOLE_PLUGIN_IMAGE.
			err := fmt.Errorf("console plugin manifest source is not configured (CONSOLE_PLUGIN_IMAGE)")
			logger.Error(err, "Cannot reconcile ConsolePlugin resources")
			return ctrl.Result{}, err
		}

		apiServerURL, err := r.getAPIServerURL(ctx)
		if err != nil {
			logger.Error(err, "Failed to get API server URL from Infrastructure CR")
			return ctrl.Result{}, err
		}

		clusterID, err := r.getClusterID(ctx)
		if err != nil {
			logger.Error(err, "Failed to get cluster ID from ClusterVersion CR")
			return ctrl.Result{}, err
		}

		if err := r.ensureImageManifests(ctx, apiServerURL, clusterID); err != nil {
			logger.Error(err, "Failed to ensure image-sourced ConsolePlugin resources")
			return ctrl.Result{}, err
		}
		logger.Info("ConsolePlugin resources are reconciled")
	} else {
		if err := r.deleteConsolePlugin(ctx); err != nil {
			logger.Error(err, "Failed to delete ConsolePlugin")
			return ctrl.Result{}, err
		}
		if err := r.deleteDeployment(ctx); err != nil {
			logger.Error(err, "Failed to delete Deployment")
			return ctrl.Result{}, err
		}
		if err := r.deleteService(ctx); err != nil {
			logger.Error(err, "Failed to delete Service")
			return ctrl.Result{}, err
		}
		if err := r.deleteRoleBinding(ctx); err != nil {
			logger.Error(err, "Failed to delete RoleBinding")
			return ctrl.Result{}, err
		}
		if err := r.deleteRole(ctx); err != nil {
			logger.Error(err, "Failed to delete Role")
			return ctrl.Result{}, err
		}
		if err := r.deleteServiceAccount(ctx); err != nil {
			logger.Error(err, "Failed to delete ServiceAccount")
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

func (r *ConsolePluginReconciler) isConsolePluginEnabled(ctx context.Context) (bool, error) {
	cm := &v1.ConfigMap{}
	err := r.Get(ctx, types.NamespacedName{Namespace: r.OperatorNamespace, Name: controllerConfigName}, cm)
	if err != nil {
		return false, fmt.Errorf("failed to get operator config configmap: %w", err)
	}

	val, ok := cm.Data[consolePluginConfigKey]
	if !ok {
		return false, nil
	}

	boolVal, err := strconv.ParseBool(val)
	if err != nil {
		return false, fmt.Errorf("failed to parse %s value from configmap: %w", consolePluginConfigKey, err)
	}

	return boolVal, nil
}

func (r *ConsolePluginReconciler) getAPIServerURL(ctx context.Context) (string, error) {
	infra := &unstructured.Unstructured{}
	infra.SetAPIVersion(infrastructureAPIVersion)
	infra.SetKind(infrastructureKind)

	err := r.Get(ctx, types.NamespacedName{Name: infrastructureName}, infra)
	if err != nil {
		return "", fmt.Errorf("failed to get Infrastructure CR: %w", err)
	}

	status, ok := infra.Object["status"].(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("infrastructure CR has no status")
	}

	apiServerURL, ok := status["apiServerURL"].(string)
	if !ok || apiServerURL == "" {
		return "", fmt.Errorf("infrastructure CR has no apiServerURL in status")
	}

	return apiServerURL, nil
}

func (r *ConsolePluginReconciler) getClusterID(ctx context.Context) (string, error) {
	cv := &unstructured.Unstructured{}
	cv.SetAPIVersion(clusterVersionAPIVersion)
	cv.SetKind(clusterVersionKind)

	err := r.Get(ctx, types.NamespacedName{Name: clusterVersionName}, cv)
	if err != nil {
		return "", fmt.Errorf("failed to get ClusterVersion CR: %w", err)
	}

	spec, ok := cv.Object["spec"].(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("ClusterVersion CR has no spec")
	}

	clusterID, ok := spec["clusterID"].(string)
	if !ok || clusterID == "" {
		return "", fmt.Errorf("ClusterVersion CR has no clusterID in spec")
	}

	return clusterID, nil
}

func (r *ConsolePluginReconciler) resolveOLMOwnership(ctx context.Context) error {
	if r.ownerInfoResolved {
		return nil
	}

	logger := log.FromContext(ctx)

	if r.PodName == "" || r.DirectReader == nil {
		logger.Info("POD_NAME not set, skipping OLM ownership resolution")
		r.ownerInfoResolved = true
		return nil
	}

	pod := &v1.Pod{}
	if err := r.DirectReader.Get(ctx, types.NamespacedName{
		Name: r.PodName, Namespace: r.OperatorNamespace,
	}, pod); err != nil {
		return fmt.Errorf("failed to get controller pod: %w", err)
	}

	deploy, err := r.resolveOwnerDeployment(ctx, pod)
	if err != nil {
		return err
	}
	if deploy == nil {
		r.ownerInfoResolved = true
		return nil
	}

	if deploy.Labels["olm.managed"] != "true" {
		logger.Info("Controller deployment is not OLM-managed, skipping OLM ownership resolution")
		r.ownerInfoResolved = true
		return nil
	}

	olmOwner, ok := deploy.Labels["olm.owner"]
	if !ok || olmOwner == "" {
		return fmt.Errorf("OLM-managed deployment has no olm.owner label")
	}

	r.deploymentOwnerRef = &metav1.OwnerReference{
		APIVersion: "apps/v1",
		Kind:       "Deployment",
		Name:       deploy.Name,
		UID:        deploy.UID,
	}

	crb, err := r.findOLMClusterRoleBinding(ctx, olmOwner, pod.Spec.ServiceAccountName)
	if err != nil {
		return err
	}

	r.clusterOwnerRef = &metav1.OwnerReference{
		APIVersion: "rbac.authorization.k8s.io/v1",
		Kind:       "ClusterRoleBinding",
		Name:       crb.Name,
		UID:        crb.UID,
	}
	logger.Info("Resolved OLM ownership",
		"deployment", deploy.Name,
		"clusterRoleBinding", crb.Name)
	r.ownerInfoResolved = true
	return nil
}

func (r *ConsolePluginReconciler) resolveOwnerDeployment(ctx context.Context, pod *v1.Pod) (*appsv1.Deployment, error) {
	logger := log.FromContext(ctx)

	rsRef := findOwnerRef(pod.OwnerReferences, "ReplicaSet")
	if rsRef == nil {
		logger.Info("Controller pod has no ReplicaSet owner, skipping OLM ownership resolution")
		return nil, nil
	}

	rs := &appsv1.ReplicaSet{}
	if err := r.DirectReader.Get(ctx, types.NamespacedName{
		Name: rsRef.Name, Namespace: r.OperatorNamespace,
	}, rs); err != nil {
		return nil, fmt.Errorf("failed to get owner ReplicaSet: %w", err)
	}

	deployRef := findOwnerRef(rs.OwnerReferences, "Deployment")
	if deployRef == nil {
		logger.Info("ReplicaSet has no Deployment owner, skipping OLM ownership resolution")
		return nil, nil
	}

	deploy := &appsv1.Deployment{}
	if err := r.DirectReader.Get(ctx, types.NamespacedName{
		Name: deployRef.Name, Namespace: r.OperatorNamespace,
	}, deploy); err != nil {
		return nil, fmt.Errorf("failed to get owner Deployment: %w", err)
	}

	return deploy, nil
}

func (r *ConsolePluginReconciler) findOLMClusterRoleBinding(ctx context.Context, olmOwner, saName string) (*rbacv1.ClusterRoleBinding, error) {
	crbList := &rbacv1.ClusterRoleBindingList{}
	if err := r.DirectReader.List(ctx, crbList,
		client.MatchingLabels{"olm.owner": olmOwner}); err != nil {
		return nil, fmt.Errorf("failed to list ClusterRoleBindings: %w", err)
	}

	for i := range crbList.Items {
		crb := &crbList.Items[i]
		for _, subject := range crb.Subjects {
			if subject.Kind == "ServiceAccount" &&
				subject.Name == saName &&
				subject.Namespace == r.OperatorNamespace {
				return crb, nil
			}
		}
	}

	return nil, fmt.Errorf("no ClusterRoleBinding found with olm.owner=%s and SA %s/%s as subject",
		olmOwner, r.OperatorNamespace, saName)
}

func findOwnerRef(refs []metav1.OwnerReference, kind string) *metav1.OwnerReference {
	for i := range refs {
		if refs[i].Kind == kind {
			return &refs[i]
		}
	}
	return nil
}

func ensureOwnerRef(obj metav1.Object, ref *metav1.OwnerReference) {
	if ref == nil {
		return
	}
	existing := obj.GetOwnerReferences()
	for i, r := range existing {
		if r.UID == ref.UID {
			existing[i] = *ref
			obj.SetOwnerReferences(existing)
			return
		}
	}
	obj.SetOwnerReferences(append(existing, *ref))
}

// Faas console plugin keep SCM credentials in Secrets in its own
// namespace.

// imageManifestValues assembles the render inputs for image-sourced plugin
// manifests: deployment coordinates, cluster facts, and the controller-config
// ConfigMap verbatim so plugin templates can read feature knobs (.Config)
// without per-knob operator code.
func (r *ConsolePluginReconciler) imageManifestValues(ctx context.Context, apiServerURL, clusterID string) (manifests.Values, error) {
	vals := manifests.Values{
		Namespace:    r.OperatorNamespace,
		Name:         consolePluginName,
		Image:        r.ConsolePluginImage,
		APIServerURL: apiServerURL,
		ClusterID:    clusterID,
		CertSecret:   consolePluginCertSecret,
		Env:          map[string]string{"GH_API_URL": os.Getenv("GH_API_URL")},
	}

	cm := &v1.ConfigMap{}
	err := r.Get(ctx, types.NamespacedName{Namespace: r.OperatorNamespace, Name: controllerConfigName}, cm)
	if apierrors.IsNotFound(err) {
		return vals, nil
	}
	if err != nil {
		return vals, fmt.Errorf("failed to get operator config configmap: %w", err)
	}
	vals.Config = cm.Data
	return vals, nil
}

// manifestSourceWithAuth returns a copy of ManifestSource with the pull
// secret, when configured, resolved into a registry keychain. The secret is
// read per fetch so credential rotation needs no operator restart.
func (r *ConsolePluginReconciler) manifestSourceWithAuth(ctx context.Context) (*manifests.Source, error) {
	src := new(manifests.Source)
	*src = *r.ManifestSource
	if src.PullSecretName == "" {
		return src, nil
	}

	secret := &v1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{
		Namespace: r.OperatorNamespace, Name: src.PullSecretName,
	}, secret); err != nil {
		return nil, fmt.Errorf("reading pull secret %s/%s: %w", r.OperatorNamespace, src.PullSecretName, err)
	}
	keychain, err := manifests.ParseDockerConfig(secret.Data[".dockerconfigjson"])
	if err != nil {
		return nil, fmt.Errorf("parsing pull secret %s/%s: %w", r.OperatorNamespace, src.PullSecretName, err)
	}
	src.Keychain = keychain
	return src, nil
}

// ensureImageManifests renders the plugin resources from the plugin image
// (pulled in-process, digest-cached) and ensures each one, replacing the Go
// builders when ManifestSource is set.
func (r *ConsolePluginReconciler) ensureImageManifests(ctx context.Context, apiServerURL, clusterID string) error {
	logger := log.FromContext(ctx)

	// controller-runtime serializes Reconcile calls per controller, so lazy
	// initialization needs no extra locking.
	if r.manifestsCache == nil {
		r.manifestsCache = manifests.NewCache("")
	}

	vals, err := r.imageManifestValues(ctx, apiServerURL, clusterID)
	if err != nil {
		return err
	}

	src, err := r.manifestSourceWithAuth(ctx)
	if err != nil {
		return err
	}
	resources, err := r.manifestsCache.Fetch(ctx, *src, vals)
	if err != nil {
		return fmt.Errorf("fetching plugin manifests from image: %w", err)
	}

	for _, res := range resources {
		logger.Info("Ensuring plugin resource from image",
			"path", res.Path, "kind", res.Obj.GetKind(), "name", res.Obj.GetName())
		if err := r.ensureRenderedResource(ctx, res.Obj); err != nil {
			return err
		}
	}
	return nil
}

// ensureRenderedResource create-or-updates one rendered resource. All
// non-metadata content is replaced with the rendered desired state; the
// generic path covers top-level fields (e.g. Role rules) as well as spec.
func (r *ConsolePluginReconciler) ensureRenderedResource(ctx context.Context, desired *unstructured.Unstructured) error {
	logger := log.FromContext(ctx)

	ownerRef := r.deploymentOwnerRef
	if desired.GetKind() == consolePluginKind {
		ownerRef = r.clusterOwnerRef
	}

	existing := &unstructured.Unstructured{}
	existing.SetAPIVersion(desired.GetAPIVersion())
	existing.SetKind(desired.GetKind())

	err := r.Get(ctx, types.NamespacedName{Name: desired.GetName(), Namespace: desired.GetNamespace()}, existing)
	if apierrors.IsNotFound(err) {
		ensureOwnerRef(desired, ownerRef)
		logger.Info("Creating resource from image manifests",
			"kind", desired.GetKind(), "name", desired.GetName())
		return r.Create(ctx, desired)
	}
	if err != nil {
		return fmt.Errorf("failed to get %s %s: %w", desired.GetKind(), desired.GetName(), err)
	}

	for key, val := range desired.Object {
		if key == "metadata" || key == "status" {
			continue
		}
		existing.Object[key] = val
	}
	existing.SetLabels(desired.GetLabels())
	existing.SetAnnotations(desired.GetAnnotations())
	ensureOwnerRef(existing, ownerRef)
	logger.Info("Updating resource from image manifests",
		"kind", desired.GetKind(), "name", desired.GetName())
	return r.Update(ctx, existing)
}

func (r *ConsolePluginReconciler) deleteConsolePlugin(ctx context.Context) error {
	logger := log.FromContext(ctx)

	cp := &unstructured.Unstructured{}
	cp.SetAPIVersion(consolePluginAPIVersion)
	cp.SetKind(consolePluginKind)
	cp.SetName(consolePluginName)

	err := r.Delete(ctx, cp)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to delete ConsolePlugin: %w", err)
	}

	logger.Info("Deleted ConsolePlugin", "name", consolePluginName)
	return nil
}

func (r *ConsolePluginReconciler) deleteDeployment(ctx context.Context) error {
	logger := log.FromContext(ctx)

	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      consolePluginName,
			Namespace: r.OperatorNamespace,
		},
	}

	err := r.Delete(ctx, deploy)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to delete Deployment: %w", err)
	}

	logger.Info("Deleted Deployment", "name", consolePluginName)
	return nil
}

func (r *ConsolePluginReconciler) deleteService(ctx context.Context) error {
	logger := log.FromContext(ctx)

	svc := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      consolePluginName,
			Namespace: r.OperatorNamespace,
		},
	}

	err := r.Delete(ctx, svc)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to delete Service: %w", err)
	}

	logger.Info("Deleted Service", "name", consolePluginName)
	return nil
}

func (r *ConsolePluginReconciler) deleteRoleBinding(ctx context.Context) error {
	logger := log.FromContext(ctx)

	rb := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      consolePluginName,
			Namespace: r.OperatorNamespace,
		},
	}

	err := r.Delete(ctx, rb)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to delete RoleBinding: %w", err)
	}

	logger.Info("Deleted RoleBinding", "name", consolePluginName)
	return nil
}

func (r *ConsolePluginReconciler) deleteRole(ctx context.Context) error {
	logger := log.FromContext(ctx)

	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:      consolePluginName,
			Namespace: r.OperatorNamespace,
		},
	}

	err := r.Delete(ctx, role)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to delete Role: %w", err)
	}

	logger.Info("Deleted Role", "name", consolePluginName)
	return nil
}

func (r *ConsolePluginReconciler) deleteServiceAccount(ctx context.Context) error {
	logger := log.FromContext(ctx)

	sa := &v1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      consolePluginName,
			Namespace: r.OperatorNamespace,
		},
	}

	err := r.Delete(ctx, sa)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to delete ServiceAccount: %w", err)
	}

	logger.Info("Deleted ServiceAccount", "name", consolePluginName)
	return nil
}

func (r *ConsolePluginReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.ConsolePluginImage == "" {
		log.Log.Info("CONSOLE_PLUGIN_IMAGE not set, skipping console plugin controller setup")
		return nil
	}

	_, err := mgr.GetRESTMapper().RESTMapping(
		schema.GroupKind{Group: "console.openshift.io", Kind: consolePluginKind},
		"v1",
	)
	if err != nil {
		log.Log.Info("ConsolePlugin CRD not found, skipping console plugin controller setup")
		return nil
	}

	return ctrl.NewControllerManagedBy(mgr).
		Named("consoleplugin").
		WatchesMetadata(
			&metav1.PartialObjectMetadata{
				TypeMeta: metav1.TypeMeta{
					APIVersion: consolePluginAPIVersion,
					Kind:       consolePluginKind,
				},
			},
			handler.EnqueueRequestsFromMapFunc(r.mapToRequest),
			builder.WithPredicates(predicate.NewPredicateFuncs(func(obj client.Object) bool {
				return obj.GetName() == consolePluginName
			})),
		).
		Watches(
			&v1.ConfigMap{},
			handler.EnqueueRequestsFromMapFunc(r.mapToRequest),
			builder.WithPredicates(predicate.NewPredicateFuncs(func(obj client.Object) bool {
				return obj.GetName() == controllerConfigName && obj.GetNamespace() == r.OperatorNamespace
			})),
		).
		Watches(
			&appsv1.Deployment{},
			handler.EnqueueRequestsFromMapFunc(r.mapToRequest),
			builder.WithPredicates(predicate.NewPredicateFuncs(func(obj client.Object) bool {
				return obj.GetName() == consolePluginName && obj.GetNamespace() == r.OperatorNamespace
			})),
		).
		Watches(
			&v1.Service{},
			handler.EnqueueRequestsFromMapFunc(r.mapToRequest),
			builder.WithPredicates(predicate.NewPredicateFuncs(func(obj client.Object) bool {
				return obj.GetName() == consolePluginName && obj.GetNamespace() == r.OperatorNamespace
			})),
		).
		Watches(
			&v1.ServiceAccount{},
			handler.EnqueueRequestsFromMapFunc(r.mapToRequest),
			builder.WithPredicates(predicate.NewPredicateFuncs(func(obj client.Object) bool {
				return obj.GetName() == consolePluginName && obj.GetNamespace() == r.OperatorNamespace
			})),
		).
		Complete(r)
}

func (r *ConsolePluginReconciler) mapToRequest(_ context.Context, _ client.Object) []reconcile.Request {
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: consolePluginName}}}
}

