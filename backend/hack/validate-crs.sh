#!/usr/bin/env bash
# Offline CR validation: render every service's FlinkDeployment and validate it
# against the REAL Apache Flink Kubernetes Operator CRD schema — no live cluster
# needed. Complements the in-repo Go validator (internal/deploy) which also runs
# as `go test ./internal/deploy/`.
#
# Requires: go, python3 (+pyyaml), and kubeconform on PATH
#   go install github.com/yannh/kubeconform/cmd/kubeconform@latest
#
# Usage: backend/hack/validate-crs.sh [flink-operator-release-branch]
set -euo pipefail

BRANCH="${1:-release-1.10}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

CRD_URL="https://raw.githubusercontent.com/apache/flink-kubernetes-operator/${BRANCH}/helm/flink-kubernetes-operator/crds/flinkdeployments.flink.apache.org-v1.yml"

echo "==> Fetching FlinkDeployment CRD (${BRANCH})"
curl -sL "$CRD_URL" -o "$WORK/crd.yaml"

echo "==> Converting CRD -> JSON schema"
curl -sL "https://raw.githubusercontent.com/yannh/kubeconform/master/scripts/openapi2jsonschema.py" -o "$WORK/o2j.py"
( cd "$WORK" && FILENAME_FORMAT='{kind}-{group}-{version}' python3 o2j.py crd.yaml >/dev/null )
SCHEMA="$(ls "$WORK"/*.json | head -1)"

echo "==> Rendering manifests from the backend snapshot"
cd "$ROOT"
go run ./hack/dump-manifests "$WORK"   # writes <service>.yaml into $WORK

echo "==> Validating against the real CRD schema"
kubeconform -schema-location "$SCHEMA" -summary -verbose "$WORK"/*.k8s.yaml
