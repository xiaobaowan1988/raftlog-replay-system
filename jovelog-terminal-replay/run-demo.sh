#!/usr/bin/env bash
# jovelog terminal-state replay: given an S3 "initial state" (per-shard startSeq +
# fingerprint) and the .bin raftlog, compute the terminal state with Flink and
# prove it is INVARIANT across parallelism 1..N (because shards are independent).
#
# Models the okj-jove-flink design: jovelog = the sequencer's raft log, sharded by
# user into 128 shards; each entry carries (shard, seq), seq strictly +1 per shard;
# same-shard order = seq, cross-shard = no order. .bin envelope:
#   file = MAGIC(BA BE CA FE) + [ SEP(EF) | seq(8) | kafkaOffset(8) | shard(4) | len(4) | data ]*
#
#   ./run-demo.sh          # full demo
#   ./run-demo.sh clean
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
WORK="${WORK:-$HERE/.work}"
FLINK_VER="${FLINK_VER:-1.18.1}"
JDK_URL="${JDK_URL:-https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.13%2B11/OpenJDK17U-jdk_x64_linux_hotspot_17.0.13_11.tar.gz}"
JAVA_HOME="$WORK/jdk17"; FLINK_HOME="$WORK/flink"; DATA="$WORK/data"
log(){ echo -e "\033[1;34m==>\033[0m $*"; }

[ "${1:-}" = "clean" ] && { rm -rf "$WORK"; echo cleaned; exit 0; }

mkdir -p "$WORK"
[ -d "$JAVA_HOME" ] || { log "fetch JDK17"; curl -fsSL -o "$WORK/jdk.tgz" "$JDK_URL"; tar xzf "$WORK/jdk.tgz" -C "$WORK"; mv "$WORK"/jdk-17* "$JAVA_HOME"; }
[ -d "$FLINK_HOME" ] || { log "fetch Flink $FLINK_VER"; curl -fsSL -o "$WORK/flink.tgz" \
  "https://archive.apache.org/dist/flink/flink-${FLINK_VER}/flink-${FLINK_VER}-bin-scala_2.12.tgz"; tar xzf "$WORK/flink.tgz" -C "$WORK"; mv "$WORK/flink-${FLINK_VER}" "$FLINK_HOME"; }

log "compile"
mkdir -p "$WORK/out"
"$JAVA_HOME/bin/javac" -encoding UTF-8 -cp "$FLINK_HOME/lib/*" -d "$WORK/out" "$HERE"/src/demo/*.java

log "generate .bin raftlog + S3 initial-state (per-shard startSeq snapshot)"
rm -rf "$DATA"; mkdir -p "$DATA"
REF="$("$JAVA_HOME/bin/java" -cp "$WORK/out" demo.BinGen "$DATA" 2>/dev/null | tee /dev/stderr | sed -n 's/^REFERENCE_TERMINAL.*sha=\([0-9a-f]*\).*/\1/p')"

log "compute terminal state with Flink at parallelism 1,2,4,8 (expect identical hash = $REF):"
OPENS="--add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED"
ok=1
for P in 1 2 4 8; do
  line="$("$JAVA_HOME/bin/java" $OPENS -cp "$WORK/out:$FLINK_HOME/lib/*" demo.RaftlogTerminalJob "$DATA" "$P" 2>/dev/null | grep PARALLELISM)"
  echo "    $line"
  echo "$line" | grep -q "sha=$REF" || ok=0
done
echo
if [ "$ok" = 1 ]; then
  echo "RESULT: terminal hash invariant across parallelism AND equals the from-seq-1 reference"
  echo "        => S3 init + per-shard replay reconstructs the terminal state; (shard,seq) dedup works;"
  echo "           per-shard independence lets you scale to N without changing the result."
else
  echo "RESULT: MISMATCH (unexpected)"; exit 1
fi
