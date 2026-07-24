// Package render turns a template directory + merged values into the final CR
// previewed in the "底层清单 (Manifests Preview)" tab.
//
// For this project the templates are FlinkDeployment CR skeletons: each *.yaml
// under <templateDir>/templates is executed as a Go text/template against a
// context exposing .Name, .Env and .Values (the deep-merged spec). Helm-style
// `toYaml` / `indent` / `nindent` helpers let a skeleton splice the whole
// merged spec into the CR without hard-coding every field — important because
// FlinkDeployment specs carry free-form maps like flinkConfiguration.
package render

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/template"

	"gopkg.in/yaml.v3"
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
		tmpl, err := template.New(name).Funcs(funcs()).Option("missingkey=zero").Parse(string(raw))
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

// funcs provides a small, Helm-compatible subset of template helpers.
func funcs() template.FuncMap {
	return template.FuncMap{
		"toYaml":  toYAML,
		"indent":  indent,
		"nindent": func(n int, s string) string { return "\n" + indent(n, s) },
	}
}

func toYAML(v any) (string, error) {
	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2) // conventional K8s / kubectl 2-space nesting
	if err := enc.Encode(v); err != nil {
		return "", err
	}
	_ = enc.Close()
	return strings.TrimRight(buf.String(), "\n"), nil
}

// indent prefixes every non-empty line of s with n spaces.
func indent(n int, s string) string {
	pad := strings.Repeat(" ", n)
	lines := strings.Split(s, "\n")
	for i, l := range lines {
		if l != "" {
			lines[i] = pad + l
		}
	}
	return strings.Join(lines, "\n")
}
