// Package model defines the JSON payloads exchanged with the frontend.
//
// The shapes mirror what src/mockData.js exposes today, so pointing the React
// app at this backend is a drop-in swap:
//
//	services[]  -> ServiceMeta
//	templates[] -> TemplateMeta
//	yamlContent -> Content{ source, merged, manifest }
package model

// ServiceMeta is a sidebar entry plus the metadata-header fields for one
// service configuration file tracked in Git.
type ServiceMeta struct {
	ID       string `json:"id"`       // stable id, e.g. "service-a"
	Name     string `json:"name"`     // file name, e.g. "service-a.yaml"
	Template string `json:"template"` // referenced template id, e.g. "standard-web-chart"
	Commit   string `json:"commit"`   // short commit hash that last touched the file
	Time     string `json:"time"`     // human-relative time, e.g. "10 minutes ago"
}

// TemplateMeta is a read-only reference entry under templates/.
type TemplateMeta struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// Tree is the payload for the left sidebar.
type Tree struct {
	Services  []ServiceMeta  `json:"services"`
	Templates []TemplateMeta `json:"templates"`
}

// Content holds the three read-only editor views for a single service.
type Content struct {
	Source   string `json:"source"`   // raw file bytes committed to Git (audit view)
	Merged   string `json:"merged"`   // service values deep-merged over template defaults
	Manifest string `json:"manifest"` // rendered K8s YAML that ships to the cluster
}

// ServiceDetail is the full per-service response: metadata + the three views.
type ServiceDetail struct {
	ServiceMeta
	Content Content `json:"content"`
}
