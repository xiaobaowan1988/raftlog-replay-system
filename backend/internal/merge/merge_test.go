package merge

import "testing"

func TestDeepMerge_OverrideAndInherit(t *testing.T) {
	base := map[string]any{
		"replicaCount": 1,
		"port":         8080,
		"image": map[string]any{
			"repository": "my-registry/placeholder",
			"tag":        "latest",
		},
		"resources": map[string]any{
			"requests": map[string]any{"cpu": "100m", "memory": "128Mi"},
		},
	}
	override := map[string]any{
		"replicaCount": 3,
		"image": map[string]any{
			"repository": "my-registry/service-a",
			"tag":        "v2.0.1",
		},
	}

	got := DeepMerge(base, override)

	if got["replicaCount"] != 3 {
		t.Errorf("replicaCount: want 3, got %v", got["replicaCount"])
	}
	if got["port"] != 8080 {
		t.Errorf("port should be inherited from base: want 8080, got %v", got["port"])
	}
	img := got["image"].(map[string]any)
	if img["repository"] != "my-registry/service-a" || img["tag"] != "v2.0.1" {
		t.Errorf("image not overridden correctly: %v", img)
	}
	// Nested map only present in base must survive.
	res := got["resources"].(map[string]any)
	req := res["requests"].(map[string]any)
	if req["cpu"] != "100m" {
		t.Errorf("resources.requests.cpu should be inherited: got %v", req["cpu"])
	}
}

func TestDeepMerge_DoesNotMutateInputs(t *testing.T) {
	base := map[string]any{"a": map[string]any{"x": 1}}
	override := map[string]any{"a": map[string]any{"y": 2}}

	_ = DeepMerge(base, override)

	if len(base["a"].(map[string]any)) != 1 {
		t.Errorf("base was mutated: %v", base)
	}
	if len(override["a"].(map[string]any)) != 1 {
		t.Errorf("override was mutated: %v", override)
	}
}
