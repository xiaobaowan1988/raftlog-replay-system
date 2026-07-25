#!/usr/bin/env bash
# Shared setup for the Flink pipeline tests: downloads Java 17, Flink, Kafka and
# the s3proxy S3 mock into ./.work, configures Flink (S3 plugin + endpoint +
# checkpoint/savepoint dirs), and provides start/stop helpers.
#
# Everything is local — no Docker, no Kubernetes, no cloud S3. The S3 mock speaks
# the same s3a:// API and checkpoint/savepoint mechanics as real S3.
set -euo pipefail

# ---- versions (override via env) ----
FLINK_VER="${FLINK_VER:-1.18.1}"
KAFKA_VER="${KAFKA_VER:-3.7.1}"
KAFKA_SCALA="${KAFKA_SCALA:-2.13}"
JDK_URL="${JDK_URL:-https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.13%2B11/OpenJDK17U-jdk_x64_linux_hotspot_17.0.13_11.tar.gz}"
S3PROXY_URL="${S3PROXY_URL:-https://github.com/gaul/s3proxy/releases/download/s3proxy-2.4.1/s3proxy}"
MAVEN="${MAVEN:-https://repo1.maven.org/maven2}"

# ---- layout ----
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORK="${WORK:-$HERE/.work}"
export JAVA_HOME="$WORK/jdk17"
export FLINK_HOME="$WORK/flink"
export KAFKA_HOME="$WORK/kafka"
export PATH="$JAVA_HOME/bin:$FLINK_HOME/bin:$KAFKA_HOME/bin:$PATH"

# S3 mock endpoint + bucket
S3_ENDPOINT="http://127.0.0.1:9000"
S3_KEY="flinkkey"; S3_SECRET="flinksecret"; S3_BUCKET="flink-state"

log() { echo -e "\033[1;34m==>\033[0m $*"; }

fetch() { # url dest
  [ -s "$2" ] || { log "download $(basename "$2")"; curl -fsSL --retry 3 -o "$2" "$1"; }
}

setup_deps() {
  mkdir -p "$WORK"
  fetch "$JDK_URL" "$WORK/jdk17.tgz"
  fetch "https://archive.apache.org/dist/flink/flink-${FLINK_VER}/flink-${FLINK_VER}-bin-scala_2.12.tgz" "$WORK/flink.tgz"
  fetch "https://archive.apache.org/dist/kafka/${KAFKA_VER}/kafka_${KAFKA_SCALA}-${KAFKA_VER}.tgz" "$WORK/kafka.tgz"
  fetch "$S3PROXY_URL" "$WORK/s3proxy"; chmod +x "$WORK/s3proxy"

  [ -d "$JAVA_HOME" ]  || { tar xzf "$WORK/jdk17.tgz"  -C "$WORK"; mv "$WORK"/jdk-17* "$JAVA_HOME"; }
  [ -d "$FLINK_HOME" ] || { tar xzf "$WORK/flink.tgz"  -C "$WORK"; mv "$WORK/flink-${FLINK_VER}" "$FLINK_HOME"; }
  [ -d "$KAFKA_HOME" ] || { tar xzf "$WORK/kafka.tgz"  -C "$WORK"; mv "$WORK/kafka_${KAFKA_SCALA}-${KAFKA_VER}" "$KAFKA_HOME"; }
  "$JAVA_HOME/bin/java" -version
}

configure_flink() {
  # S3 filesystem plugin (bundled in Flink's opt/)
  mkdir -p "$FLINK_HOME/plugins/s3-fs-hadoop"
  cp -n "$FLINK_HOME/opt/flink-s3-fs-hadoop-${FLINK_VER}.jar" "$FLINK_HOME/plugins/s3-fs-hadoop/" || true
  local conf="$FLINK_HOME/conf/flink-conf.yaml"
  if ! grep -q "gitops-flink-test" "$conf"; then
    cat >> "$conf" <<EOF

# ---- gitops-flink-test additions ----
taskmanager.numberOfTaskSlots: 4
parallelism.default: 1
rest.port: 8081
s3.endpoint: $S3_ENDPOINT
s3.path.style.access: true
s3.access-key: $S3_KEY
s3.secret-key: $S3_SECRET
state.checkpoints.dir: s3a://$S3_BUCKET/checkpoints
state.savepoints.dir: s3a://$S3_BUCKET/savepoints
execution.checkpointing.interval: 5000
execution.checkpointing.mode: EXACTLY_ONCE
EOF
  fi
}

start_s3proxy() {
  cat > "$WORK/s3proxy.conf" <<EOF
s3proxy.endpoint=$S3_ENDPOINT
s3proxy.authorization=aws-v2-or-v4
s3proxy.identity=$S3_KEY
s3proxy.credential=$S3_SECRET
jclouds.provider=filesystem
jclouds.filesystem.basedir=$WORK/s3data
EOF
  mkdir -p "$WORK/s3data/$S3_BUCKET"   # jclouds filesystem: top-level dir == bucket
  log "start s3proxy ($S3_ENDPOINT)"
  JAVA_TOOL_OPTIONS="" nohup "$WORK/s3proxy" --properties "$WORK/s3proxy.conf" > "$WORK/s3proxy.log" 2>&1 &
  for _ in $(seq 1 20); do curl -s -o /dev/null "$S3_ENDPOINT/" && break; sleep 1; done
}

start_flink() {
  configure_flink
  log "start Flink standalone cluster"
  "$FLINK_HOME/bin/start-cluster.sh" >/dev/null
  for _ in $(seq 1 30); do
    [ "$(curl -s http://127.0.0.1:8081/overview | sed -n 's/.*"taskmanagers":\([0-9]*\).*/\1/p')" = "1" ] && break; sleep 1
  done
  log "Flink UI: http://127.0.0.1:8081"
}

stop_all() {
  "$FLINK_HOME/bin/stop-cluster.sh" >/dev/null 2>&1 || true
  pkill -f s3proxy 2>/dev/null || true
  pkill -f kafka.Kafka 2>/dev/null || true
}

# List objects the S3 mock stored, or cat a key's content.
s3_ls()  { find "$WORK/s3data/$S3_BUCKET/${1:-}" -type f 2>/dev/null | sed "s#$WORK/s3data/#s3a://#"; }
s3_cat() { cat "$WORK/s3data/$S3_BUCKET/$1"; }
