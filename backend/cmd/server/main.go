// Command server is the read-only GitOps configuration API.
//
// It keeps a local checkout of the GitOps repo in sync, deep-merges each
// service's values with its template defaults, renders manifests, and serves
// the result as JSON for the React dashboard. Configuration is via env vars:
//
//	GITOPS_REPO_URL          remote to clone/pull; empty => use REPO_DIR as-is (local mode)
//	GITOPS_REPO_BRANCH       branch to track (default "main")
//	GITOPS_REPO_DIR          working dir on disk (default "./data/repo";
//	                         in local mode point this at your repo, e.g. ./testdata/repo)
//	GITOPS_WEBHOOK_SECRET    expected X-Gitlab-Token value (empty => webhook unauthenticated)
//	GITOPS_LISTEN            listen address (default ":8080")
//	GITOPS_REFRESH_INTERVAL  periodic re-sync as a Go duration (default "5m"; "0" disables)
//	GITOPS_CORS_ORIGIN       CORS origin to allow (default "http://localhost:5173")
//	GITOPS_KUBECONFIG        kubeconfig path for deploys; empty => in-cluster then default kubeconfig
//	GITOPS_DEPLOY_NAMESPACE  namespace to apply FlinkDeployments into (default "default")
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/xiaobaowan1988/raftlog-replay-system/backend/internal/api"
	"github.com/xiaobaowan1988/raftlog-replay-system/backend/internal/deploy"
	"github.com/xiaobaowan1988/raftlog-replay-system/backend/internal/gitrepo"
	"github.com/xiaobaowan1988/raftlog-replay-system/backend/internal/store"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg := loadConfig()
	log.Info("starting gitops-dashboard backend",
		"listen", cfg.listen, "repoDir", cfg.repoDir, "branch", cfg.branch,
		"localMode", cfg.repoURL == "", "refresh", cfg.refresh)

	repo := gitrepo.New(cfg.repoURL, cfg.branch, cfg.repoDir)
	st := store.New(repo)

	// Initial build. Fail fast if we can't produce a first snapshot.
	initCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	if err := st.Rebuild(initCtx); err != nil {
		cancel()
		log.Error("initial rebuild failed", "err", err)
		os.Exit(1)
	}
	cancel()
	log.Info("initial snapshot built", "services", len(st.Tree().Services))

	// Best-effort cluster applier for the deploy write path. If no kubeconfig /
	// in-cluster config is reachable, the server still serves the read API and
	// the deploy endpoint reports itself unconfigured (503).
	var opts api.Options
	opts.DeployNamespace = cfg.deployNamespace
	if applier, err := deploy.New(cfg.kubeconfig, "gitops-dashboard"); err != nil {
		log.Warn("deploy disabled: no reachable cluster config", "err", err)
	} else {
		opts.Applier = applier
		log.Info("deploy enabled", "namespace", cfg.deployNamespace)
	}

	rootCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if cfg.refresh > 0 {
		go periodicRefresh(rootCtx, st, cfg.refresh, log)
	}

	srv := &http.Server{
		Addr:              cfg.listen,
		Handler:           api.New(st, cfg.webhookSecret, cfg.corsOrigin, opts, log).Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		log.Info("listening", "addr", cfg.listen)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server error", "err", err)
			stop()
		}
	}()

	<-rootCtx.Done()
	log.Info("shutting down")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	_ = srv.Shutdown(shutdownCtx)
}

func periodicRefresh(ctx context.Context, st *store.Store, every time.Duration, log *slog.Logger) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c, cancel := context.WithTimeout(ctx, 2*time.Minute)
			if err := st.Rebuild(c); err != nil {
				log.Warn("periodic rebuild failed", "err", err)
			} else {
				log.Debug("periodic rebuild ok")
			}
			cancel()
		}
	}
}

type config struct {
	repoURL         string
	branch          string
	repoDir         string
	webhookSecret   string
	listen          string
	refresh         time.Duration
	corsOrigin      string
	kubeconfig      string
	deployNamespace string
}

func loadConfig() config {
	c := config{
		repoURL:         os.Getenv("GITOPS_REPO_URL"),
		branch:          envOr("GITOPS_REPO_BRANCH", "main"),
		repoDir:         envOr("GITOPS_REPO_DIR", "./data/repo"),
		webhookSecret:   os.Getenv("GITOPS_WEBHOOK_SECRET"),
		listen:          envOr("GITOPS_LISTEN", ":8080"),
		corsOrigin:      envOr("GITOPS_CORS_ORIGIN", "http://localhost:5173"),
		kubeconfig:      os.Getenv("GITOPS_KUBECONFIG"),
		deployNamespace: envOr("GITOPS_DEPLOY_NAMESPACE", "default"),
		refresh:         5 * time.Minute,
	}
	if v := strings.TrimSpace(os.Getenv("GITOPS_REFRESH_INTERVAL")); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			c.refresh = d
		}
	}
	return c
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
