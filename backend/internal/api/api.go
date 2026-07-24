// Package api exposes the read-only HTTP surface consumed by the React app,
// plus the GitLab webhook that triggers a re-sync.
//
// Routes:
//
//	GET  /api/healthz              liveness probe
//	GET  /api/tree                 sidebar: { services[], templates[] }
//	GET  /api/services/{id}        one service: metadata + { source, merged, manifest }
//	POST /api/webhook/gitlab       re-sync trigger (validates X-Gitlab-Token)
//
// Every non-webhook route is a pure read of the current in-memory snapshot, in
// keeping with the Zero-Write principle: this server never mutates repo state.
package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/xiaobaowan1988/raftlog-replay-system/backend/internal/store"
)

// Rebuilder is the subset of *store.Store the webhook needs.
type Rebuilder interface {
	Rebuild(ctx context.Context) error
}

// Server wires the store, webhook secret and CORS origin into an http.Handler.
type Server struct {
	store         *store.Store
	webhookSecret string
	allowOrigin   string
	log           *slog.Logger
}

// New builds the Server. allowOrigin is the CORS origin to allow (e.g.
// "http://localhost:5173"); empty disables CORS headers.
func New(st *store.Store, webhookSecret, allowOrigin string, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{store: st, webhookSecret: webhookSecret, allowOrigin: allowOrigin, log: log}
}

// Handler returns the fully-routed http.Handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/healthz", s.handleHealth)
	mux.HandleFunc("GET /api/tree", s.handleTree)
	mux.HandleFunc("GET /api/services/{id}", s.handleService)
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
