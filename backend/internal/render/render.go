// Package render turns a template directory + merged values into the final K8s
// manifests previewed in the "底层清单 (Manifests Preview)" tab.
//
// It is intentionally a lightweight, Helm-flavored renderer: every *.yaml under
// <templateDir>/templates is executed as a Go text/template against a context
// exposing .Name, .Env and .Values, and the results are concatenated with the
// standard `---` document separator.
package render

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/template"
)

// Context is the data made available to manifest templates.
type Context struct {
	Name   string
	Env    string
	Values map[string]any
}

// Render executes all manifest templates found under templateDir/templates.
// Files are processed in lexical order for deterministic output.
func Render(templateDir string, ctx Context) (string, error) {
	manifestDir := filepath.Join(templateDir, "templates")
	entries, err := os.ReadDir(manifestDir)
	if err != nil {
		return "", fmt.Errorf("read template manifests %q: %w", manifestDir, err)
	}

	var files []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if ext := strings.ToLower(filepath.Ext(e.Name())); ext == ".yaml" || ext == ".yml" {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)

	var docs []string
	for _, name := range files {
		raw, err := os.ReadFile(filepath.Join(manifestDir, name))
		if err != nil {
			return "", err
		}
		tmpl, err := template.New(name).Option("missingkey=zero").Parse(string(raw))
		if err != nil {
			return "", fmt.Errorf("parse %s: %w", name, err)
		}
		var buf bytes.Buffer
		if err := tmpl.Execute(&buf, ctx); err != nil {
			return "", fmt.Errorf("render %s: %w", name, err)
		}
		docs = append(docs, strings.TrimRight(buf.String(), "\n"))
	}

	if len(docs) == 0 {
		return "# (this template renders no manifests)\n", nil
	}
	return strings.Join(docs, "\n---\n") + "\n", nil
}
