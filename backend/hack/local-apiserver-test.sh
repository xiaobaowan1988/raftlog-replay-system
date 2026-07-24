#!/usr/bin/env bash
# End-to-end deploy test against a REAL Kubernetes apiserver — no Docker, no
# container registry, no nodes. Uses envtest (kube-apiserver + etcd binaries
# fetched by setup-envtest) so it works even where image pulls are blocked.
#
# It: starts a control plane, installs the real Flink FlinkDeployment CRD, runs
# the backend against it, deploys every sample service via the /deploy endpoint
# (dry-run then apply), and shows the objects the apiserver persisted.
#
# Requires: go, curl, and network access to the (allow-listed) k8s binary host.
# Usage: backend/hack/local-apiserver-test.sh
set -euo pipefail

K8S_VER="${K8S_VER:-1.31.0}"
FLINK_BRANCH="${FLINK_BRANCH:-release-1.10}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d)"
KUBECONFIG_FILE="$WORK/envtest.kubeconfig"
BACKEND_PORT="${BACKEND_PORT:-8080}"
pids=()
cleanup() { for p in "${pids[@]:-}"; do kill "$p" 2>/dev/null || true; done; rm -rf "$WORK"; }
trap cleanup EXIT

echo "==> Fetching kube-apiserver + etcd (setup-envtest)"
GOBIN="$WORK" go install sigs.k8s.io/controller-runtime/tools/setup-envtest@latest
ASSETS="$("$WORK/setup-envtest" use "$K8S_VER" --bin-dir "$WORK/kubebin" -p path)"
KUBECTL="$WORK/kubebin/k8s/${K8S_VER}-linux-amd64/kubectl"

echo "==> Fetching the real FlinkDeployment CRD (${FLINK_BRANCH})"
mkdir -p "$WORK/crds"
curl -sL "https://raw.githubusercontent.com/apache/flink-kubernetes-operator/${FLINK_BRANCH}/helm/flink-kubernetes-operator/crds/flinkdeployments.flink.apache.org-v1.yml" \
  -o "$WORK/crds/flinkdeployment.yaml"

echo "==> Starting control plane + installing CRD"
( cd "$ROOT/hack/localcluster" && KUBEBUILDER_ASSETS="$ASSETS" go run . "$WORK/crds" "$KUBECONFIG_FILE" ) &
pids+=($!)
for _ in $(seq 1 30); do [ -s "$KUBECONFIG_FILE" ] && break; sleep 1; done
export KUBECONFIG="$KUBECONFIG_FILE"
"$KUBECTL" wait --for=condition=Established crd/flinkdeployments.flink.apache.org --timeout=30s

echo "==> Starting backend wired to the control plane"
( cd "$ROOT" && GITOPS_REPO_DIR=./testdata/repo GITOPS_REFRESH_INTERVAL=0 \
    GITOPS_LISTEN=":$BACKEND_PORT" GITOPS_KUBECONFIG="$KUBECONFIG_FILE" \
    GITOPS_DEPLOY_NAMESPACE=default go run ./cmd/server ) &
pids+=($!)
for _ in $(seq 1 30); do curl -sf "localhost:$BACKEND_PORT/api/healthz" >/dev/null 2>&1 && break; sleep 1; done

echo "==> Deploying every sample service (dry-run, then apply)"
for s in $(curl -s "localhost:$BACKEND_PORT/api/tree" | grep -o '"id":"[^"]*"' | cut -d'"' -f4); do
  echo "  - $s (dryRun): $(curl -s -X POST "localhost:$BACKEND_PORT/api/services/$s/deploy?dryRun=true" -o /dev/null -w '%{http_code}')"
  echo "  - $s (apply):  $(curl -s -X POST "localhost:$BACKEND_PORT/api/services/$s/deploy" -o /dev/null -w '%{http_code}')"
done

echo "==> FlinkDeployments the apiserver persisted:"
"$KUBECTL" get flinkdeployments -A
echo "==> Applied by field manager:"
"$KUBECTL" get flinkdeployment -n default -o jsonpath='{range .items[*]}{.metadata.name}{" -> "}{.metadata.managedFields[0].manager}{"\n"}{end}'
