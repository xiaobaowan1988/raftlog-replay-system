#!/usr/bin/env bash
# Kafka -> Flink (keyed ValueState count) -> S3, then prove state restore from an
# S3 savepoint. Reproduces the live test end to end on a single machine.
#
#   ./run-kafka-s3.sh          # full demo
#   ./run-kafka-s3.sh clean    # stop services + delete ./.work
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; source "$HERE/common.sh"

if [ "${1:-}" = "clean" ]; then stop_all; rm -rf "$WORK"; echo "cleaned"; exit 0; fi

setup_deps
start_s3proxy
start_flink

# ---- Kafka (KRaft, single broker) ----
if ! kafka-broker-api-versions.sh --bootstrap-server 127.0.0.1:9092 >/dev/null 2>&1; then
  sed -i "s#^log.dirs=.*#log.dirs=$WORK/kafka-logs#" "$KAFKA_HOME/config/kraft/server.properties"
  CID="$(kafka-storage.sh random-uuid)"
  kafka-storage.sh format -t "$CID" -c "$KAFKA_HOME/config/kraft/server.properties" >/dev/null
  log "start Kafka broker"
  nohup kafka-server-start.sh "$KAFKA_HOME/config/kraft/server.properties" > "$WORK/kafka.log" 2>&1 &
  for _ in $(seq 1 30); do kafka-broker-api-versions.sh --bootstrap-server 127.0.0.1:9092 >/dev/null 2>&1 && break; sleep 1; done
fi
kafka-topics.sh --create --if-not-exists --topic events --bootstrap-server 127.0.0.1:9092 --partitions 2 --replication-factor 1 >/dev/null

# ---- build the job (fat jar: connector + kafka-clients) ----
log "build Kafka->S3 job"
mkdir -p "$WORK/joblib" "$WORK/kjob/out"
fetch "$MAVEN/org/apache/flink/flink-connector-kafka/3.1.0-1.18/flink-connector-kafka-3.1.0-1.18.jar" "$WORK/joblib/flink-connector-kafka.jar"
fetch "$MAVEN/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar" "$WORK/joblib/kafka-clients.jar"
mkdir -p "$WORK/kjob/src/demo"; cp "$HERE/jobs/FlinkKafkaS3Job.java" "$WORK/kjob/src/demo/"
javac -cp "$FLINK_HOME/lib/*:$WORK/joblib/*" -d "$WORK/kjob/out" "$WORK/kjob/src/demo/FlinkKafkaS3Job.java"
( cd "$WORK/kjob/out" && unzip -oq "$WORK/joblib/flink-connector-kafka.jar" -x 'META-INF/*.SF' 'META-INF/*.RSA' 'META-INF/*.DSA' \
  && unzip -oq "$WORK/joblib/kafka-clients.jar" -x 'META-INF/*.SF' 'META-INF/*.RSA' 'META-INF/*.DSA' )
jar cf "$WORK/kafka-s3-job.jar" -C "$WORK/kjob/out" .

# ---- run + produce ----
log "submit job, produce a,a,b"
flink run -d -c demo.FlinkKafkaS3Job "$WORK/kafka-s3-job.jar" >/dev/null
JID="$(curl -s http://127.0.0.1:8081/jobs | sed -n 's/.*"id":"\([a-f0-9]*\)","status":"RUNNING".*/\1/p' | head -1)"
sleep 8
printf 'a\na\nb\n' | kafka-console-producer.sh --bootstrap-server 127.0.0.1:9092 --topic events
sleep 8
log "counts so far (TM stdout):"; grep -hE '^[abc]=' "$FLINK_HOME"/log/*taskexecutor*.out | tail -3

# ---- stop with savepoint to S3, restore, produce more ----
log "stop WITH savepoint to S3"
SVP="$(flink stop -D client.timeout=180000 --savepointPath s3a://$S3_BUCKET/savepoints "$JID" 2>&1 | sed -n 's/.*Path: \(s3a:[^ ]*\).*/\1/p')"
echo "  savepoint: $SVP"
log "restore FROM S3 savepoint, produce a,b (expect counts CONTINUE: a=3,b=2)"
flink run -s "$SVP" -d -c demo.FlinkKafkaS3Job "$WORK/kafka-s3-job.jar" >/dev/null
sleep 6
printf 'a\nb\n' | kafka-console-producer.sh --bootstrap-server 127.0.0.1:9092 --topic events
sleep 8

echo; log "RESULT — running counts across the restart (state restored from S3):"
grep -hE '^[abc]=' "$FLINK_HOME"/log/*taskexecutor*.out | tail -6
echo; log "S3 objects:"; s3_ls | grep -E 'savepoints/.*_metadata|output/.*/part-' | head
echo; echo "Flink UI: http://127.0.0.1:8081   (run './run-kafka-s3.sh clean' to tear down)"
