#!/usr/bin/env bash
# Aeron Cluster raftlog-replay demo: initial state (snapshot) + raft log -> terminal state.
#
# Runs a real single-node Aeron Cluster with a deterministic counter state
# machine. It:
#   1. applies commands (I:a I:a I:b)           -> state {a:2, b:1}
#   2. takes a SNAPSHOT at that log position    -> the "initial state" (S3-persistable)
#   3. applies more commands (I:a I:b I:c)      -> terminal state {a:3, b:2, c:1}
#   4. CRASHES the node (kill -9, no shutdown snapshot)
#   5. RESTARTS it -> loads the snapshot, then REPLAYS the post-snapshot raft log,
#      reconstructing {a:3, b:2, c:1} exactly (verified by state hash).
#
#   ./run-demo.sh          # full demo
#   ./run-demo.sh clean    # stop + delete ./.work
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
WORK="${WORK:-$HERE/.work}"
AERON_VER="${AERON_VER:-1.44.0}"
JAR="$WORK/aeron-all-${AERON_VER}.jar"
DATA="$WORK/data"
JAVA="${JAVA:-java}"
OPENS="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.util.zip=ALL-UNNAMED"
log(){ echo -e "\033[1;34m==>\033[0m $*"; }

if [ "${1:-}" = "clean" ]; then pkill -9 -f raftreplay.ClusterNode 2>/dev/null || true; rm -rf "$WORK"; echo cleaned; exit 0; fi

# Java 17+ required (Aeron 1.44).
ver="$("$JAVA" -version 2>&1 | sed -n 's/.*version "\([0-9]*\).*/\1/p' | head -1)"
[ "${ver:-0}" -ge 17 ] || { echo "need Java 17+ (found ${ver:-none}); set JAVA=/path/to/jdk17/bin/java"; exit 1; }

mkdir -p "$WORK"
[ -s "$JAR" ] || { log "download aeron-all ${AERON_VER}"; curl -fsSL -o "$JAR" \
  "https://repo1.maven.org/maven2/io/aeron/aeron-all/${AERON_VER}/aeron-all-${AERON_VER}.jar"; }

log "compile"
mkdir -p "$WORK/out"
"${JAVA%java}javac" -encoding UTF-8 -cp "$JAR" -d "$WORK/out" "$HERE"/src/raftreplay/*.java
CP="$WORK/out:$JAR"
NLOG="$WORK/node.out"

node_up(){ for _ in $(seq 1 30); do grep -q LEADER-READY "$NLOG" 2>/dev/null && return 0; sleep 1; done; return 1; }

rm -rf "$DATA"; mkdir -p "$DATA"
log "start cluster node"
$JAVA $OPENS -cp "$CP" raftreplay.ClusterNode "$DATA" > "$NLOG" 2>&1 &
node_up || { echo "node failed to reach leader"; tail -20 "$NLOG"; exit 1; }

log "apply I:a I:a I:b   (expect state {a:2,b:1})"
$JAVA $OPENS -cp "$CP" raftreplay.ClusterClient "$DATA" I:a I:a I:b >/dev/null 2>&1

log "take SNAPSHOT (records the initial state at this log position)"
$JAVA $OPENS -cp "$JAR" io.aeron.cluster.ClusterTool "$DATA/cluster" snapshot 2>&1 | grep -i snapshot || true
sleep 2

log "apply I:a I:b I:c   (terminal state {a:3,b:2,c:1}, AFTER the snapshot)"
$JAVA $OPENS -cp "$CP" raftreplay.ClusterClient "$DATA" I:a I:b I:c >/dev/null 2>&1
sleep 1

log "CRASH node (kill -9 -> no shutdown snapshot)"
pkill -9 -f raftreplay.ClusterNode; sleep 2

log "back up raftlog + snapshot recordings (this is what you'd store in S3):"
du -sh "$DATA/archive" | sed 's/^/    /'

: > "$NLOG"
log "RESTART node -> recover: load snapshot, then REPLAY the raft log after it"
$JAVA $OPENS -cp "$CP" raftreplay.ClusterNode "$DATA" > "$NLOG" 2>&1 &
node_up || { echo "recovery failed"; tail -20 "$NLOG"; exit 1; }

echo
log "RECOVERY TRACE:"
grep -E "loaded snapshot|REPLAY|LEADER-READY" "$NLOG" | sed 's/^/    /'
echo
recovered="$(grep LEADER-READY "$NLOG" | tail -1 | sed -n 's/.*STATE \(.*\)/\1/p')"
log "recovered terminal state: $recovered"
echo "    (snapshot alone was {a:2,b:1}; reaching {a:3,b:2,c:1} proves the post-snapshot log was replayed)"

pkill -9 -f raftreplay.ClusterNode 2>/dev/null || true
echo; echo "run './run-demo.sh clean' to tear down"
