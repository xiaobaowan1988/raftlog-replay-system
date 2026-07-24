// Package deploy applies rendered FlinkDeployment CRs to a Kubernetes cluster.
//
// This is the one write path in the system. It uses client-go's dynamic client
// with Server-Side Apply (SSA): the dashboard owns a named field manager, so
// re-applying the same object is idempotent and conflicts are attributed.
//
// Config resolution follows the usual client-go order: in-cluster config first
// (when the backend runs as a pod in the target cluster), then the kubeconfig
// at GITOPS_KUBECONFIG / $KUBECONFIG / ~/.kube/config. If none is available the
// Applier fails to construct and the deploy endpoint reports it as unconfigured
// — the read-only API keeps working regardless.
package deploy

import (
	"context"
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/yaml"
)

// Result describes one applied object.
type Result struct {
	Group     string `json:"group"`
	Version   string `json:"version"`
	Kind      string `json:"kind"`
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	DryRun    bool   `json:"dryRun"`
}

// Applier performs Server-Side Apply of CR manifests.
type Applier struct {
	dyn          dynamic.Interface
	fieldManager string
}

// New builds an Applier from the resolved rest.Config. kubeconfigPath may be
// empty to use in-cluster / default-kubeconfig resolution.
func New(kubeconfigPath, fieldManager string) (*Applier, error) {
	cfg, err := restConfig(kubeconfigPath)
	if err != nil {
		return nil, err
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("build dynamic client: %w", err)
	}
	if fieldManager == "" {
		fieldManager = "gitops-dashboard"
	}
	return &Applier{dyn: dyn, fieldManager: fieldManager}, nil
}

func restConfig(kubeconfigPath string) (*rest.Config, error) {
	if kubeconfigPath == "" {
		if c, err := rest.InClusterConfig(); err == nil {
			return c, nil
		}
	}
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfigPath != "" {
		rules.ExplicitPath = kubeconfigPath
	}
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{}).ClientConfig()
}

// Apply server-side-applies every document in manifest into defaultNS (unless a
// document sets its own namespace). Each object is validated first. When dryRun
// is true the request is sent with DryRun=All so the apiserver validates and
// admits without persisting.
func (a *Applier) Apply(ctx context.Context, defaultNS, manifest string, dryRun bool) ([]Result, error) {
	objs, err := Decode(manifest)
	if err != nil {
		return nil, err
	}
	if len(objs) == 0 {
		return nil, fmt.Errorf("manifest contains no objects")
	}

	var results []Result
	for _, obj := range objs {
		if err := validateObject(obj); err != nil {
			return nil, err
		}
		ns := obj.GetNamespace()
		if ns == "" {
			ns = defaultNS
			obj.SetNamespace(ns)
		}
		gvk := obj.GroupVersionKind()
		data, err := obj.MarshalJSON()
		if err != nil {
			return nil, err
		}
		opts := metav1.PatchOptions{FieldManager: a.fieldManager, Force: boolPtr(true)}
		if dryRun {
			opts.DryRun = []string{metav1.DryRunAll}
		}
		_, err = a.dyn.Resource(gvrFor(gvk)).Namespace(ns).
			Patch(ctx, obj.GetName(), types.ApplyPatchType, data, opts)
		if err != nil {
			return nil, fmt.Errorf("apply %s %q: %w", gvk.Kind, obj.GetName(), err)
		}
		results = append(results, Result{
			Group: gvk.Group, Version: gvk.Version, Kind: gvk.Kind,
			Name: obj.GetName(), Namespace: ns, DryRun: dryRun,
		})
	}
	return results, nil
}

// Decode splits a (possibly multi-document) YAML manifest into unstructured
// objects, skipping empty / comment-only documents.
func Decode(manifest string) ([]*unstructured.Unstructured, error) {
	var out []*unstructured.Unstructured
	for _, doc := range strings.Split(manifest, "\n---") {
		trimmed := strings.TrimSpace(doc)
		if trimmed == "" {
			continue
		}
		jb, err := yaml.YAMLToJSON([]byte(doc))
		if err != nil {
			return nil, fmt.Errorf("yaml→json: %w", err)
		}
		if s := strings.TrimSpace(string(jb)); s == "" || s == "null" {
			continue // comment-only document
		}
		obj := &unstructured.Unstructured{}
		if err := obj.UnmarshalJSON(jb); err != nil {
			return nil, fmt.Errorf("decode object: %w", err)
		}
		out = append(out, obj)
	}
	return out, nil
}

// gvrFor derives the resource plural from a GVK. Sufficient for the kinds this
// service deploys (FlinkDeployment → flinkdeployments). For a broader set, back
// this with a discovery-driven RESTMapper.
func gvrFor(gvk schema.GroupVersionKind) schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    gvk.Group,
		Version:  gvk.Version,
		Resource: strings.ToLower(gvk.Kind) + "s",
	}
}

func boolPtr(b bool) *bool { return &b }
