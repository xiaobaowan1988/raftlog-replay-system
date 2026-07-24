// Package store owns the in-memory representation of the whole GitOps repo.
//
// On Rebuild it: syncs Git, scans services/, loads each service's referenced
// template defaults, deep-merges values, renders manifests, and atomically
// swaps in a fresh immutable Snapshot. Readers (the HTTP handlers) never block
// on a rebuild — they read the current snapshot under an RWMutex.
package store

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/xiaobaowan1988/raftlog-replay-system/backend/internal/gitrepo"
	"github.com/xiaobaowan1988/raftlog-replay-system/backend/internal/merge"
	"github.com/xiaobaowan1988/raftlog-replay-system/backend/internal/model"
	"github.com/xiaobaowan1988/raftlog-replay-system/backend/internal/render"

	"gopkg.in/yaml.v3"
)

// Snapshot is an immutable view of the repo at one point in time.
type Snapshot struct {
	Tree     model.Tree
	Services map[string]model.ServiceDetail
}

// Store holds the current Snapshot and rebuilds it from Git.
type Store struct {
	repo *gitrepo.Repo

	rebuildMu sync.Mutex // serializes rebuilds (overlapping webhooks)

	mu   sync.RWMutex // guards snap
	snap *Snapshot
}

// New returns a Store backed by repo. Call Rebuild before serving.
func New(repo *gitrepo.Repo) *Store {
	return &Store{repo: repo, snap: &Snapshot{Services: map[string]model.ServiceDetail{}}}
}

// serviceFile is the on-disk shape of a services/*.yaml file.
type serviceFile struct {
	Name        string         `yaml:"name"`
	Env         string         `yaml:"env"`
	TemplateRef struct{ Path string `yaml:"path"` } `yaml:"template_ref"`
	Values      map[string]any `yaml:"values"`
}

// Rebuild re-reads the entire repo and swaps in a new snapshot. Safe to call
// concurrently; calls are serialized so a webhook storm can't interleave.
func (s *Store) Rebuild(ctx context.Context) error {
	s.rebuildMu.Lock()
	defer s.rebuildMu.Unlock()

	if err := s.repo.Sync(ctx); err != nil {
		return fmt.Errorf("git sync: %w", err)
	}

	root := s.repo.Dir()
	next := &Snapshot{Services: map[string]model.ServiceDetail{}}

	if err := s.loadTemplates(root, next); err != nil {
		return err
	}
	if err := s.loadServices(ctx, root, next); err != nil {
		return err
	}

	s.mu.Lock()
	s.snap = next
	s.mu.Unlock()
	return nil
}

func (s *Store) loadTemplates(root string, snap *Snapshot) error {
	dir := filepath.Join(root, "templates")
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // templates/ is optional
		}
		return fmt.Errorf("read templates dir: %w", err)
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		snap.Tree.Templates = append(snap.Tree.Templates, model.TemplateMeta{ID: e.Name(), Name: e.Name()})
	}
	sort.Slice(snap.Tree.Templates, func(i, j int) bool {
		return snap.Tree.Templates[i].ID < snap.Tree.Templates[j].ID
	})
	return nil
}

func (s *Store) loadServices(ctx context.Context, root string, snap *Snapshot) error {
	dir := filepath.Join(root, "services")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("read services dir: %w", err)
	}

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		ext := strings.ToLower(filepath.Ext(e.Name()))
		if ext != ".yaml" && ext != ".yml" {
			continue
		}
		detail, err := s.buildService(ctx, root, e.Name())
		if err != nil {
			return fmt.Errorf("service %s: %w", e.Name(), err)
		}
		snap.Services[detail.ID] = detail
		snap.Tree.Services = append(snap.Tree.Services, detail.ServiceMeta)
	}

	sort.Slice(snap.Tree.Services, func(i, j int) bool {
		return snap.Tree.Services[i].ID < snap.Tree.Services[j].ID
	})
	return nil
}

func (s *Store) buildService(ctx context.Context, root, fileName string) (model.ServiceDetail, error) {
	relPath := filepath.Join("services", fileName)
	rawBytes, err := os.ReadFile(filepath.Join(root, relPath))
	if err != nil {
		return model.ServiceDetail{}, err
	}

	var svc serviceFile
	if err := yaml.Unmarshal(rawBytes, &svc); err != nil {
		return model.ServiceDetail{}, fmt.Errorf("parse yaml: %w", err)
	}
	if svc.Values == nil {
		svc.Values = map[string]any{}
	}

	id := strings.TrimSuffix(fileName, filepath.Ext(fileName))
	templateID := filepath.Base(svc.TemplateRef.Path)
	templateDir := filepath.Join(root, filepath.FromSlash(svc.TemplateRef.Path))

	// Deep-merge template default values with the service's overrides.
	defaults, err := loadTemplateValues(templateDir)
	if err != nil {
		return model.ServiceDetail{}, err
	}
	merged := merge.DeepMerge(defaults, svc.Values)

	// Build the "merged" view: the full document with merged values.
	mergedYAML, err := marshalMerged(svc, merged, templateID)
	if err != nil {
		return model.ServiceDetail{}, err
	}

	// Render the K8s manifests.
	manifest, err := render.Render(templateDir, render.Context{Name: svc.Name, Env: svc.Env, Values: merged})
	if err != nil {
		// A rendering problem shouldn't sink the whole service; surface it in-band.
		manifest = fmt.Sprintf("# manifest render failed: %v\n", err)
	}

	hash, when := s.repo.LastCommit(ctx, relPath)

	return model.ServiceDetail{
		ServiceMeta: model.ServiceMeta{
			ID:       id,
			Name:     fileName,
			Template: templateID,
			Commit:   hash,
			Time:     when,
		},
		Content: model.Content{
			Source:   string(rawBytes),
			Merged:   mergedYAML,
			Manifest: manifest,
		},
	}, nil
}

// loadTemplateValues reads <templateDir>/values.yaml. A missing file yields an
// empty default set (the service then stands entirely on its own values).
func loadTemplateValues(templateDir string) (map[string]any, error) {
	b, err := os.ReadFile(filepath.Join(templateDir, "values.yaml"))
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]any{}, nil
		}
		return nil, fmt.Errorf("read template values: %w", err)
	}
	var vals map[string]any
	if err := yaml.Unmarshal(b, &vals); err != nil {
		return nil, fmt.Errorf("parse template values: %w", err)
	}
	if vals == nil {
		vals = map[string]any{}
	}
	return vals, nil
}

func marshalMerged(svc serviceFile, merged map[string]any, templateID string) (string, error) {
	doc := struct {
		Name        string         `yaml:"name"`
		Env         string         `yaml:"env"`
		TemplateRef struct{ Path string `yaml:"path"` } `yaml:"template_ref"`
		Values      map[string]any `yaml:"values"`
	}{Name: svc.Name, Env: svc.Env, Values: merged}
	doc.TemplateRef.Path = svc.TemplateRef.Path

	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(doc); err != nil {
		return "", err
	}
	_ = enc.Close()
	header := fmt.Sprintf("# Auto-merged with defaults from template [%s]\n", templateID)
	return header + buf.String(), nil
}

// Tree returns the current sidebar payload.
func (s *Store) Tree() model.Tree {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snap.Tree
}

// Service returns the detail for id, and whether it exists.
func (s *Store) Service(id string) (model.ServiceDetail, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	d, ok := s.snap.Services[id]
	return d, ok
}
