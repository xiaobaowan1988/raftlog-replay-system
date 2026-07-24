// Command dump-manifests renders every service's FlinkDeployment CR from the
// sample repo and writes each to <outDir>/<service>.k8s.yaml. Used by
// hack/validate-crs.sh to feed rendered CRs into kubeconform.
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/xiaobaowan1988/raftlog-replay-system/backend/internal/gitrepo"
	"github.com/xiaobaowan1988/raftlog-replay-system/backend/internal/store"
)

func main() {
	outDir := "."
	if len(os.Args) > 1 {
		outDir = os.Args[1]
	}
	repoDir := os.Getenv("GITOPS_REPO_DIR")
	if repoDir == "" {
		repoDir = "./testdata/repo"
	}

	st := store.New(gitrepo.New("", "main", repoDir))
	if err := st.Rebuild(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "rebuild:", err)
		os.Exit(1)
	}

	for _, meta := range st.Tree().Services {
		detail, _ := st.Service(meta.ID)
		path := filepath.Join(outDir, meta.ID+".k8s.yaml")
		if err := os.WriteFile(path, []byte(detail.Content.Manifest), 0o644); err != nil {
			fmt.Fprintln(os.Stderr, "write:", err)
			os.Exit(1)
		}
		fmt.Println("wrote", path)
	}
}
