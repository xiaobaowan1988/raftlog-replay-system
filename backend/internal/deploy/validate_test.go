package deploy_test

import (
	"context"
	"strings"
	"testing"

	"github.com/xiaobaowan1988/raftlog-replay-system/backend/internal/deploy"
	"github.com/xiaobaowan1988/raftlog-replay-system/backend/internal/gitrepo"
	"github.com/xiaobaowan1988/raftlog-replay-system/backend/internal/store"
)

// TestRenderedManifestsAreValidFlinkDeployments builds the snapshot from the
// sample repo and validates every service's rendered CR — the offline CR-check
// that proves what "Run" would apply is a well-formed FlinkDeployment, without
// needing a live cluster.
func TestRenderedManifestsAreValidFlinkDeployments(t *testing.T) {
	repo := gitrepo.New("", "main", "../../testdata/repo")
	st := store.New(repo)
	if err := st.Rebuild(context.Background()); err != nil {
		t.Fatalf("rebuild snapshot: %v", err)
	}

	services := st.Tree().Services
	if len(services) == 0 {
		t.Fatal("no services in snapshot")
	}

	for _, meta := range services {
		t.Run(meta.ID, func(t *testing.T) {
			detail, ok := st.Service(meta.ID)
			if !ok {
				t.Fatalf("service %s missing from snapshot", meta.ID)
			}
			results, err := deploy.Validate(detail.Content.Manifest)
			if err != nil {
				t.Fatalf("rendered manifest failed CR validation: %v", err)
			}
			if len(results) != 1 {
				t.Fatalf("expected exactly one object, got %d", len(results))
			}
			r := results[0]
			if r.Group != "flink.apache.org" || r.Version != "v1beta1" || r.Kind != "FlinkDeployment" {
				t.Errorf("unexpected GVK: %s/%s %s", r.Group, r.Version, r.Kind)
			}
			if r.Name != meta.ID {
				t.Errorf("CR name %q does not match service id %q", r.Name, meta.ID)
			}
		})
	}
}

func TestValidateRejectsBadManifests(t *testing.T) {
	cases := map[string]string{
		"missing kind":     "apiVersion: flink.apache.org/v1beta1\nmetadata:\n  name: x\nspec:\n  image: flink\n",
		"missing name":     "apiVersion: flink.apache.org/v1beta1\nkind: FlinkDeployment\nspec:\n  image: flink\n",
		"missing spec":     "apiVersion: flink.apache.org/v1beta1\nkind: FlinkDeployment\nmetadata:\n  name: x\n",
		"missing image":    "apiVersion: flink.apache.org/v1beta1\nkind: FlinkDeployment\nmetadata:\n  name: x\nspec:\n  flinkVersion: v1_18\n  jobManager:\n    resource: {cpu: 1, memory: 1g}\n  taskManager:\n    resource: {cpu: 1, memory: 1g}\n",
		"job without jar":  "apiVersion: flink.apache.org/v1beta1\nkind: FlinkDeployment\nmetadata:\n  name: x\nspec:\n  image: flink\n  flinkVersion: v1_18\n  jobManager:\n    resource: {cpu: 1, memory: 1g}\n  taskManager:\n    resource: {cpu: 1, memory: 1g}\n  job:\n    parallelism: 2\n",
	}
	for name, manifest := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := deploy.Validate(manifest); err == nil {
				t.Errorf("expected validation error for %q, got nil", name)
			} else if strings.TrimSpace(err.Error()) == "" {
				t.Errorf("expected descriptive error for %q", name)
			}
		})
	}
}
