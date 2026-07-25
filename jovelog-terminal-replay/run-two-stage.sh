#!/usr/bin/env bash
# Where does the "initial state" come from? A prior consumption's checkpoint.
#
# STAGE 1 (materializer): fold the .bin from genesis (seq 1) up to a cut, and
#   PERSIST per-shard (cursor, fingerprint) — the analog of a Flink checkpoint =
#   keyed state + per-shard source offset. THIS is the initial state.
# STAGE 2 (RaftlogTerminalJob): resume from that checkpoint, fold only seq > cursor
#   -> terminal state. Equals the from-seq-1 reference, at any parallelism, and for
#   ANY cut position (the snapshot boundary is arbitrary).
#
#   ./run-two-stage.sh          # full demo
#   ./run-two-stage.sh clean
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
WORK="${WORK:-$HERE/.work}"
FLINK_VER="${FLINK_VER:-1.18.1}"
JDK_URL="${JDK_URL:-https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.13%2B11/OpenJDK17U-jdk_x64_linux_hotspot_17.0.13_11.tar.gz}"
JAVA_HOME="$WORK/jdk17"; FLINK_HOME="$WORK/flink"; DATA="$WORK/data"
OPENS="--add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED"
log(){ echo -e "\033[1;34m==>\033[0m $*"; }

[ "${1:-}" = "clean" ] && { rm -rf "$WORK"; echo cleaned; exit 0; }

mkdir -p "$WORK"
[ -d "$JAVA_HOME" ] || { log "fetch JDK17"; curl -fsSL -o "$WORK/jdk.tgz" "$JDK_URL"; tar xzf "$WORK/jdk.tgz" -C "$WORK"; mv "$WORK"/jdk-17* "$JAVA_HOME"; }
[ -d "$FLINK_HOME" ] || { log "fetch Flink $FLINK_VER"; curl -fsSL -o "$WORK/flink.tgz" \
  "https://archive.apache.org/dist/flink/flink-${FLINK_VER}/flink-${FLINK_VER}-bin-scala_2.12.tgz"; tar xzf "$WORK/flink.tgz" -C "$WORK"; mv "$WORK/flink-${FLINK_VER}" "$FLINK_HOME"; }

log "compile"; mkdir -p "$WORK/out"
"$JAVA_HOME/bin/javac" -encoding UTF-8 -cp "$FLINK_HOME/lib/*" -d "$WORK/out" "$HERE"/src/demo/*.java
CP="$WORK/out:$FLINK_HOME/lib/*"
J(){ "$JAVA_HOME/bin/java" $OPENS -cp "$CP" "$@" 2>/dev/null; }

rm -rf "$DATA"; mkdir -p "$DATA"
log "generate .bin raftlog"
REF="$("$JAVA_HOME/bin/java" -cp "$WORK/out" demo.BinGen "$DATA" 2>/dev/null | sed -n 's/^REFERENCE_TERMINAL.*sha=\([0-9a-f]*\).*/\1/p')"
echo "    reference terminal (fold from seq 1) sha=$REF"

run_cut() {   # cut-spec label
  local cut="$1" label="$2"
  echo
  log "STAGE 1 — materialize checkpoint at cut $label ($cut)  [this IS the initial state]"
  J demo.Materializer "$DATA" "$cut" "$DATA/initial-state.txt" | grep "shard=" | sed 's/^/    /'
  echo "    persisted checkpoint file:"; sed 's/^/      /' "$DATA/initial-state.txt"
  log "STAGE 2 — resume from that checkpoint, fold seq>cursor (parallelism 1 & 4)"
  local ok=1
  for P in 1 4; do
    line="$(J demo.RaftlogTerminalJob "$DATA" "$P" | grep PARALLELISM)"
    echo "    $line"
    echo "$line" | grep -q "sha=$REF" || ok=0
  done
  [ "$ok" = 1 ] && echo "    OK: terminal == reference $REF" || { echo "    MISMATCH"; exit 1; }
}

run_cut "5:8,42:5,109:12"  "A"
run_cut "5:15,42:18,109:3" "B (different cut → different checkpoint)"

echo
echo "CONCLUSION: the initial state is stage-1's persisted (cursor, fingerprint) checkpoint"
echo "            = keyed state + per-shard seq offset. Stage-2 replay from ANY such cut"
echo "            reconstructs the same terminal state. No snapshot at all == cut at seq 0."
