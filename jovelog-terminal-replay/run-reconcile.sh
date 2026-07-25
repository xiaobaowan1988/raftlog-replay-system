#!/usr/bin/env bash
# "How do I compare the replayed terminal state against production's?"
# Production publishes a per-shard rolling state root {shard, seq, stateRoot}
# (tagged with the exact seq it reflects). The reconciler folds the replay and
# compares at each such checkpoint — continuous, per-shard, no global barrier,
# and it LOCALIZES the first (shard, seq) that diverges.
#
#   SCENARIO 1  replay == production         -> ALL SHARDS MATCH
#   SCENARIO 2  single-shard S3 corruption   -> only that shard red, localized to a seq
#   SCENARIO 3  replay logic drift           -> all shards red from the first checkpoint
#
#   ./run-reconcile.sh          # run all three
#   ./run-reconcile.sh clean
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
WORK="${WORK:-$HERE/.work}"
FLINK_VER="${FLINK_VER:-1.18.1}"
JDK_URL="${JDK_URL:-https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.13%2B11/OpenJDK17U-jdk_x64_linux_hotspot_17.0.13_11.tar.gz}"
JAVA_HOME="$WORK/jdk17"; FLINK_HOME="$WORK/flink"; DATA="$WORK/data"; ROOTS="$WORK/roots.txt"
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
gen(){ rm -rf "$DATA"; mkdir -p "$DATA"; "$JAVA_HOME/bin/java" -cp "$WORK/out" demo.BinGen "$DATA" >/dev/null 2>&1; }
roots(){ "$JAVA_HOME/bin/java" -cp "$WORK/out" demo.ProducerWithRoots "$DATA" 4 "$ROOTS" >/dev/null 2>&1; }
recon(){ "$JAVA_HOME/bin/java" $OPENS ${1:-} -cp "$CP" demo.Reconcile "$DATA" "$ROOTS" 4 2>/dev/null | grep -E "shard=|RECONCILE" | sed 's/^/    /'; }

echo; echo "########## SCENARIO 1 — replay matches production ##########"
gen; roots
echo "  production roots (shard,seq,stateRoot), a rolling per-shard digest:"; head -4 "$ROOTS" | sed 's/^/      /'
recon

echo; echo "########## SCENARIO 2 — single-shard S3 corruption (shard 42, seq 14) ##########"
"$JAVA_HOME/bin/java" -cp "$WORK/out" demo.CorruptRecord "$DATA" 42 14 2>/dev/null | sed 's/^/    /'
recon
echo "    => only shard 42 red, localized; shards 5 & 109 stay green (independent)."

echo; echo "########## SCENARIO 3 — replay logic drift (wrong fold) ##########"
gen; roots
recon "-DfoldM=1000033"
echo "    => every shard diverges from its first checkpoint."

echo
echo "TAKEAWAY: production emits {shard, seq, stateRoot}; reconciler compares at each"
echo "          (shard,seq) as it folds — per-shard, no global barrier, first divergence"
echo "          localized to a (shard, seq). This is the dual-run acceptance gate."
