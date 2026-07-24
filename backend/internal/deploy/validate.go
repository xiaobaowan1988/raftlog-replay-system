package deploy

import (
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Validate decodes a rendered manifest and checks that every object is a
// well-formed, deployable CR. This is the offline "CR 校验" gate: it runs
// without a cluster and catches the mistakes a bad template/merge would
// produce (wrong GVK, missing name, empty spec, missing required Flink fields)
// before anything is sent to the apiserver.
func Validate(manifest string) ([]Result, error) {
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
		gvk := obj.GroupVersionKind()
		results = append(results, Result{
			Group: gvk.Group, Version: gvk.Version, Kind: gvk.Kind,
			Name: obj.GetName(), Namespace: obj.GetNamespace(),
		})
	}
	return results, nil
}

func validateObject(obj *unstructured.Unstructured) error {
	gvk := obj.GroupVersionKind()
	if gvk.Kind == "" || gvk.GroupVersion().Empty() {
		return fmt.Errorf("object missing apiVersion/kind")
	}
	if obj.GetName() == "" {
		return fmt.Errorf("%s: metadata.name is required", gvk.Kind)
	}
	if _, found, _ := unstructured.NestedMap(obj.Object, "spec"); !found {
		return fmt.Errorf("%s/%s: spec is required", gvk.Kind, obj.GetName())
	}

	// FlinkDeployment-specific expectations, mirroring the operator's CRD.
	if gvk.Group == "flink.apache.org" && gvk.Kind == "FlinkDeployment" {
		return validateFlinkDeployment(obj)
	}
	return nil
}

func validateFlinkDeployment(obj *unstructured.Unstructured) error {
	name := obj.GetName()
	requireString := func(path ...string) error {
		v, found, err := unstructured.NestedString(obj.Object, path...)
		if err != nil {
			return fmt.Errorf("FlinkDeployment/%s: spec.%v: %w", name, path[1:], err)
		}
		if !found || v == "" {
			return fmt.Errorf("FlinkDeployment/%s: spec.%v is required", name, path[1:])
		}
		return nil
	}
	if err := requireString("spec", "image"); err != nil {
		return err
	}
	if err := requireString("spec", "flinkVersion"); err != nil {
		return err
	}

	// A FlinkDeployment must define compute: jobManager + taskManager resources.
	for _, comp := range []string{"jobManager", "taskManager"} {
		if _, found, _ := unstructured.NestedMap(obj.Object, "spec", comp, "resource"); !found {
			return fmt.Errorf("FlinkDeployment/%s: spec.%s.resource is required", name, comp)
		}
	}

	// Application-mode deployments carry a job; if present, jarURI is required.
	if job, found, _ := unstructured.NestedMap(obj.Object, "spec", "job"); found {
		if v, ok := job["jarURI"].(string); !ok || v == "" {
			return fmt.Errorf("FlinkDeployment/%s: spec.job.jarURI is required for application mode", name)
		}
	}
	return nil
}
