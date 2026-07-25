#!/usr/bin/env bash
# "How do I know the replay is correct — do I need to compare to production?"
# This shows which errors internal invariants catch for free, and which ones
# ONLY an oracle (reference / production / golden) can catch.
#
#   SCENARIO 1  complete + correct  -> internal checks pass AND matches oracle
#   SCENARIO 2  a HOLE (missing seq) -> the +1 completeness check catches it (no oracle needed)
#   SCENARIO 3  LOGIC DRIFT (wrong fold) -> every internal check PASSES; only the oracle differs
#
#   ./run-verify.sh          # run all three
#   ./run-verify.sh clean
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
gen(){ "$JAVA_HOME/bin/java" -cp "$WORK/out" demo.BinGen "$DATA" "${1:-}"; }
job(){ "$JAVA_HOME/bin/java" $OPENS ${2:-} -cp "$CP" demo.RaftlogTerminalJob "$DATA" "$1" 2>/dev/null | grep -E "PARALLELISM|COMPLETENESS"; }

echo; echo "########## SCENARIO 1 — complete data, correct logic ##########"
rm -rf "$DATA"; mkdir -p "$DATA"
REF="$(gen 2>/dev/null | sed -n 's/^REFERENCE_TERMINAL.*sha=\([0-9a-f]*\).*/\1/p')"
echo "oracle/reference sha=$REF"; job 4 | sed 's/^/    /'
echo "    => internal checks pass AND == oracle  => correct"

echo; echo "########## SCENARIO 2 — a HOLE: shard 42 missing seq 14 ##########"
rm -rf "$DATA"; mkdir -p "$DATA"; gen "42:14" >/dev/null 2>&1
job 4 | sed 's/^/    /'
echo "    => the +1 completeness check caught it. Known wrong WITHOUT any oracle."

echo; echo "########## SCENARIO 3 — LOGIC DRIFT: complete data, wrong fold constant ##########"
rm -rf "$DATA"; mkdir -p "$DATA"; gen >/dev/null 2>&1
job 1 "-DfoldM=1000033" | sed 's/^/    /'
job 4 "-DfoldM=1000033" | sed 's/^/    /'
echo "    => completeness OK, dedup OK, parallelism-invariant (deterministic) — all GREEN"
echo "    => but sha != oracle $REF.  ONLY comparing to the oracle catches logic drift."

echo
echo "TAKEAWAY:"
echo "  input/completeness/determinism bugs  -> internal invariants catch for free (no prod)"
echo "  apply-logic / semantic bugs          -> invisible to all internal checks; need an ORACLE"
echo "  cheapest oracle = REUSE the production apply code (then logic can't drift by construction);"
echo "  otherwise dual-run vs production / a golden derived from it."
