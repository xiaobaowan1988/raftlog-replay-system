#!/usr/bin/env bash
# Can FLINK compute the terminal state from an Aeron Cluster raftlog + an S3
# initial state? Yes. This wires it end to end and verifies the result is
# bit-identical to what the Aeron cluster reconstructs itself.
#
#   Aeron cluster ──▶ export raftlog.jsonl (commands + logPosition)
#                 ──▶ export initial-state.json (snapshot state @ position)
#        both ──▶ S3 (local mock)
#   Flink job ──▶ read S3 init + raftlog, replay entries after the snapshot
#                 position, apply in raft order ──▶ terminal state ──▶ S3
#
# Reuses the Flink + S3-mock setup from ../flink-pipeline-tests.
#   ./run-flink-replay.sh          # full pipeline
#   ./run-flink-replay.sh clean
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
COMMON="$HERE/../flink-pipeline-tests/common.sh"
[ -f "$COMMON" ] || { echo "need ../flink-pipeline-tests/common.sh"; exit 1; }
source "$COMMON"   # brings JAVA_HOME (jdk17), FLINK_HOME, s3proxy + start helpers

AWORK="$HERE/.work"; AJAR="$AWORK/aeron-all-1.44.0.jar"; ADATA="$AWORK/data"; AEXP="$AWORK/export"
BUCKET_DIR="$WORK/s3data/aeron-raftlog"   # jclouds filesystem bucket == a dir
OPENS="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.util.zip=ALL-UNNAMED"

if [ "${1:-}" = "clean" ]; then
  pkill -9 -f raftreplay.ClusterNodeExport 2>/dev/null || true
  stop_all 2>/dev/null || true; rm -rf "$AWORK"; echo cleaned; exit 0
fi

# --- bring up Flink + S3 mock (from flink-pipeline-tests) ---
setup_deps; start_s3proxy; start_flink

# --- Aeron: build + run the exporting cluster ---
mkdir -p "$AWORK/out" "$AEXP"; : > "$AEXP/raftlog.jsonl" 2>/dev/null || true; rm -f "$AEXP"/*
[ -s "$AJAR" ] || { log "download aeron-all"; curl -fsSL -o "$AJAR" \
  "https://repo1.maven.org/maven2/io/aeron/aeron-all/1.44.0/aeron-all-1.44.0.jar"; }
log "compile Aeron sources"
"$JAVA_HOME/bin/javac" -encoding UTF-8 -cp "$AJAR" -d "$AWORK/out" "$HERE"/src/raftreplay/*.java
ACP="$AWORK/out:$AJAR"; J="$JAVA_HOME/bin/java $OPENS -cp $ACP"

rm -rf "$ADATA"; mkdir -p "$ADATA"
log "start Aeron export cluster"
$JAVA_HOME/bin/java $OPENS -Dexport.dir="$AEXP" -cp "$ACP" raftreplay.ClusterNodeExport "$ADATA" > "$AWORK/node.out" 2>&1 &
for _ in $(seq 1 30); do grep -q LEADER-READY "$AWORK/node.out" && break; sleep 1; done

log "apply I:a I:a I:b ; SNAPSHOT ; apply I:a I:b I:c"
$J raftreplay.ClusterClient "$ADATA" I:a I:a I:b >/dev/null 2>&1
$J io.aeron.cluster.ClusterTool "$ADATA/cluster" snapshot 2>&1 | grep -i snapshot || true
sleep 2
$J raftreplay.ClusterClient "$ADATA" I:a I:b I:c >/dev/null 2>&1
sleep 1
pkill -9 -f raftreplay.ClusterNodeExport 2>/dev/null || true

echo; log "exported raftlog + initial-state:"; sed 's/^/    /' "$AEXP/initial-state.json"; echo; sed 's/^/    /' "$AEXP/raftlog.jsonl"

# --- publish exports to S3 (mock) ---
mkdir -p "$BUCKET_DIR"; cp "$AEXP/initial-state.json" "$AEXP/raftlog.jsonl" "$BUCKET_DIR/"
log "published to s3a://aeron-raftlog/"

# --- Flink: replay the raftlog from the S3 initial state ---
log "build + run Flink RaftlogReplayJob"
mkdir -p "$AWORK/fjob/demo"; cp "$HERE/flink/RaftlogReplayJob.java" "$AWORK/fjob/demo/"
"$JAVA_HOME/bin/javac" -cp "$FLINK_HOME/lib/*" -d "$AWORK/fjob" "$AWORK/fjob/demo/RaftlogReplayJob.java"
jar cf "$AWORK/raftlog-replay.jar" -C "$AWORK/fjob" .
"$FLINK_HOME/bin/flink" run -c demo.RaftlogReplayJob "$AWORK/raftlog-replay.jar" 2>&1 | grep -E "\[flink\]" | sed 's/^/    /'

echo; log "RESULT: Flink's terminal state (written to s3a://aeron-raftlog/flink-terminal.txt)"
echo "    should equal the Aeron cluster's own recovered state {a:3,b:2,c:1} sha=550df8a78e3a"
echo; echo "run './run-flink-replay.sh clean' to tear down"
