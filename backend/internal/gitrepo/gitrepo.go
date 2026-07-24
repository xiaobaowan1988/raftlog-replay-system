// Package gitrepo keeps a local checkout of the GitOps repository in sync with
// its remote and answers per-file commit questions.
//
// It shells out to the `git` binary rather than pulling in a Git library, to
// keep the dependency surface minimal. When no remote URL is configured the
// repo runs in "local mode": the working directory is used as-is (handy for
// development against testdata/), and commit lookups degrade gracefully.
package gitrepo

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// Repo is a synced working copy of the GitOps repository.
type Repo struct {
	url    string // remote URL; empty => local mode (no clone/pull)
	branch string
	dir    string // working directory on disk
}

// New constructs a Repo. If url is empty, dir is treated as a pre-populated
// working directory and no network operations are performed.
func New(url, branch, dir string) *Repo {
	if branch == "" {
		branch = "main"
	}
	return &Repo{url: url, branch: branch, dir: dir}
}

// Dir returns the working directory path.
func (r *Repo) Dir() string { return r.dir }

// Sync makes the working copy reflect origin/<branch>. It clones on first use
// and hard-resets to the remote tip on subsequent calls, so the local state is
// always exactly what the remote holds (Single Source of Truth). In local mode
// it is a no-op beyond verifying the directory exists.
func (r *Repo) Sync(ctx context.Context) error {
	if r.url == "" {
		if _, err := os.Stat(r.dir); err != nil {
			return fmt.Errorf("local repo dir %q not accessible: %w", r.dir, err)
		}
		return nil
	}

	if _, err := os.Stat(filepath.Join(r.dir, ".git")); os.IsNotExist(err) {
		if err := os.MkdirAll(filepath.Dir(r.dir), 0o755); err != nil {
			return err
		}
		if _, err := r.git(ctx, "", "clone", "--branch", r.branch, "--single-branch", r.url, r.dir); err != nil {
			return fmt.Errorf("clone: %w", err)
		}
		return nil
	}

	if _, err := r.git(ctx, r.dir, "fetch", "--prune", "origin", r.branch); err != nil {
		return fmt.Errorf("fetch: %w", err)
	}
	if _, err := r.git(ctx, r.dir, "reset", "--hard", "origin/"+r.branch); err != nil {
		return fmt.Errorf("reset: %w", err)
	}
	return nil
}

// LastCommit returns the short hash and human-relative time of the most recent
// commit that touched relPath. In local mode (or when the directory is not a
// Git repo) it returns sensible placeholders instead of an error, so the API
// still works when developing against a plain directory.
func (r *Repo) LastCommit(ctx context.Context, relPath string) (hash, when string) {
	out, err := r.git(ctx, r.dir, "log", "-1", "--format=%h\x1f%cr", "--", relPath)
	if err != nil {
		return "local", "uncommitted"
	}
	parts := strings.SplitN(strings.TrimSpace(out), "\x1f", 2)
	if len(parts) != 2 || parts[0] == "" {
		return "unknown", "unknown"
	}
	return parts[0], parts[1]
}

func (r *Repo) git(ctx context.Context, dir string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	if dir != "" {
		cmd.Dir = dir
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}
