// Package api exposes the mostly-read-only HTTP surface consumed by the React
// app, the GitLab webhook that triggers a re-sync, and the one write path:
// deploying a rendered FlinkDeployment CR to the cluster.
//
// Routes:
//
//	GET  /api/healthz              liveness probe
//	GET  /api/tree                 sidebar: { services[], templates[] }
//	GET  /api/services/{id}        one service: metadata + { source, merged, manifest }
//	POST /api/services/{id}/deploy Server-Side Apply the rendered CR (?dryRun=true supported)
//	POST /api/webhook/gitlab       re-sync trigger (validates X-Gitlab-Token)
//
// All GET routes are pure reads of the current in-memory snapshot. The deploy
// route is the sole mutation and only touches the cluster, never the repo.
package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/xiaobaowan1988/raftlog-replay-system/backend/internal/deploy"
	"github.com/xiaobaowan1988/raftlog-replay-system/backend/internal/store"
)

// Rebuilder is the subset of *store.Store the webhook needs.
type Rebuilder interface {
	Rebuild(ctx context.Context) error
}

// Server wires the store, webhook secret, CORS origin and (optional) cluster
// applier into an http.Handler.
type Server struct {
	store         *store.Store
	webhookSecret string
	allowOrigin   string
	applier       *deploy.Applier // nil when the cluster is not configured
	deployNS      string
	log           *slog.Logger
}

// Options configures the deploy write path.
type Options struct {
	Applier         *deploy.Applier // nil => deploy endpoint returns 503
	DeployNamespace string          // target namespace when a CR omits one
}

// New builds the Server. allowOrigin is the CORS origin to allow (e.g.
// "http://localhost:5173"); empty disables CORS headers. opts may be zero.
func New(st *store.Store, webhookSecret, allowOrigin string, opts Options, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	ns := opts.DeployNamespace
	if ns == "" {
		ns = "default"
	}
	return &Server{
		store: st, webhookSecret: webhookSecret, allowOrigin: allowOrigin,
		applier: opts.Applier, deployNS: ns, log: log,
	}
}

// Handler returns the fully-routed http.Handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/healthz", s.handleHealth)
	mux.HandleFunc("GET /api/tree", s.handleTree)
	mux.HandleFunc("GET /api/services/{id}", s.handleService)
	mux.HandleFunc("POST /api/services/{id}/deploy", s.handleDeploy)
	mux.HandleFunc("POST /api/webhook/gitlab", s.handleWebhook)
	return s.withCORS(mux)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleTree(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.store.Tree())
}

func (s *Server) handleService(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	detail, ok := s.store.Service(id)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "service not found: " + id})
		return
	}
	writeJSON(w, http.StatusOK, detail)
}

// handleDeploy Server-Side-Applies the service's rendered FlinkDeployment CR to
// the cluster. ?dryRun=true asks the apiserver to validate/admit without
// persisting. Returns 503 when no cluster is configured.
func (s *Server) handleDeploy(w http.ResponseWriter, r *http.Request) {
	if s.applier == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"error": "deploy is not configured: no reachable kubeconfig / in-cluster config on the backend",
		})
		return
	}

	id := r.PathValue("id")
	detail, ok := s.store.Service(id)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "service not found: " + id})
		return
	}

	dryRun := r.URL.Query().Get("dryRun") == "true"

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	results, err := s.applier.Apply(ctx, s.deployNS, detail.Content.Manifest, dryRun)
	if err != nil {
		s.log.Warn("deploy failed", "service", id, "dryRun", dryRun, "err", err)
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	s.log.Info("deployed", "service", id, "dryRun", dryRun, "objects", len(results))
	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "applied",
		"dryRun":  dryRun,
		"applied": results,
	})
}

// handleWebhook validates the GitLab secret token and triggers a rebuild.
// GitLab sends the configured secret in the X-Gitlab-Token header.
func (s *Server) handleWebhook(w http.ResponseWriter, r *http.Request) {
	if s.webhookSecret != "" && r.Header.Get("X-Gitlab-Token") != s.webhookSecret {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid webhook token"})
		return
	}

	// Rebuild in the background so GitLab gets a fast 202 and we don't hold the
	// request open through a git fetch.
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := s.store.Rebuild(ctx); err != nil {
			s.log.Error("rebuild after webhook failed", "err", err)
			return
		}
		s.log.Info("rebuilt snapshot after webhook")
	}()

	writeJSON(w, http.StatusAccepted, map[string]string{"status": "rebuild scheduled"})
}

func (s *Server) withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.allowOrigin != "" {
			w.Header().Set("Access-Control-Allow-Origin", s.allowOrigin)
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Gitlab-Token")
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
