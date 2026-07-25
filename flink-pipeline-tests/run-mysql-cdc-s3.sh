#!/usr/bin/env bash
# MySQL binlog (CDC via Debezium) -> Flink -> S3, then prove restore-from-S3
# resumes at the exact binlog offset (no re-snapshot).
#
#   ./run-mysql-cdc-s3.sh          # full demo
#   ./run-mysql-cdc-s3.sh clean    # stop services + delete ./.work
#
# Requires a local MySQL 8 with ROW binlog. On Debian/Ubuntu without one, this
# script installs + starts mysql-server (needs root, no systemd required).
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"; source "$HERE/common.sh"
CDC_VER="${CDC_VER:-3.0.1}"   # com.ververica flink-sql-connector-mysql-cdc (Flink 1.18)

if [ "${1:-}" = "clean" ]; then stop_all; rm -rf "$WORK"; echo "cleaned (MySQL left running)"; exit 0; fi

# ---- MySQL 8 with ROW binlog ----
ensure_mysql() {
  if ! command -v mysqld >/dev/null 2>&1; then
    log "install mysql-server-8.0 (apt)"
    DEBIAN_FRONTEND=noninteractive apt-get update -y >/dev/null 2>&1 || true
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends mysql-server-8.0 mysql-client-8.0
  fi
  cat > /etc/mysql/mysql.conf.d/cdc.cnf <<'EOF'
[mysqld]
server-id=1
log_bin=/var/lib/mysql/binlog
binlog_format=ROW
binlog_row_image=FULL
bind-address=127.0.0.1
EOF
  if ! mysqladmin ping >/dev/null 2>&1; then
    mkdir -p /var/run/mysqld && chown mysql:mysql /var/run/mysqld
    log "start mysqld"
    nohup mysqld --user=mysql > /var/log/mysql/manual.log 2>&1 &
    for _ in $(seq 1 30); do mysqladmin ping >/dev/null 2>&1 && break; sleep 1; done
  fi
  mysql <<'SQL'
CREATE DATABASE IF NOT EXISTS inventory;
USE inventory;
DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
  id INT PRIMARY KEY, product VARCHAR(64), qty INT, amount DECIMAL(10,2),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP);
INSERT INTO orders (id,product,qty,amount) VALUES (1,'widget',3,29.97),(2,'gadget',1,19.99),(3,'gizmo',5,55.00);
CREATE USER IF NOT EXISTS 'flinkcdc'@'%' IDENTIFIED BY 'flinkpw';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'flinkcdc'@'%';
FLUSH PRIVILEGES;
SQL
  log "MySQL ready: inventory.orders seeded (3 rows), user flinkcdc"
}

setup_deps
ensure_mysql
# CDC connector must be on the cluster classpath -> drop into lib/ before starting Flink
fetch "$MAVEN/com/ververica/flink-sql-connector-mysql-cdc/${CDC_VER}/flink-sql-connector-mysql-cdc-${CDC_VER}.jar" \
      "$WORK/flink-sql-connector-mysql-cdc.jar"
configure_flink
cp -n "$WORK/flink-sql-connector-mysql-cdc.jar" "$FLINK_HOME/lib/" || true
start_s3proxy
start_flink

# ---- build + submit the CDC job ----
log "build MySQL-CDC->S3 job"
mkdir -p "$WORK/mjob/src/demo" "$WORK/mjob/out"; cp "$HERE/jobs/MySqlCdcToS3Job.java" "$WORK/mjob/src/demo/"
javac -cp "$FLINK_HOME/lib/*" -d "$WORK/mjob/out" "$WORK/mjob/src/demo/MySqlCdcToS3Job.java"
jar cf "$WORK/mysql-cdc-job.jar" -C "$WORK/mjob/out" .
log "submit job (snapshots 3 rows, then streams binlog)"
flink run -d -c demo.MySqlCdcToS3Job "$WORK/mysql-cdc-job.jar" >/dev/null
JID="$(curl -s http://127.0.0.1:8081/jobs | sed -n 's/.*"id":"\([a-f0-9]*\)","status":"RUNNING".*/\1/p' | head -1)"
sleep 16

# ---- change data in MySQL ----
log "apply INSERT / UPDATE / DELETE in MySQL"
mysql inventory <<'SQL'
INSERT INTO orders (id,product,qty,amount) VALUES (4,'sprocket',8,88.00);
UPDATE orders SET qty=10 WHERE id=1;
DELETE FROM orders WHERE id=2;
SQL
sleep 12

echo; log "RESULT — change events captured to S3 (op: r=snapshot c=insert u=update d=delete):"
grep -hoE '\{"before".*\}' "$FLINK_HOME"/log/*taskexecutor*.out | tail -6

log "stop WITH savepoint to S3 (captures binlog offset)"
flink stop -D client.timeout=180000 --savepointPath s3a://$S3_BUCKET/savepoints "$JID" >/dev/null 2>&1 || true
echo; log "binlog offset stored in the S3 savepoint:"
MD="$(find "$WORK/s3data/$S3_BUCKET/savepoints" -name _metadata | tail -1)"
strings -n 5 "$MD" | grep -oE '\{"transaction_id".*"server_id":"[0-9]*"\}' | head -1
echo "MySQL now at: $(mysql -N -e "SHOW BINARY LOG STATUS" 2>/dev/null | awk '{print $1":"$2}')"
echo; echo "Flink UI: http://127.0.0.1:8081   (run './run-mysql-cdc-s3.sh clean' to tear down Flink/S3)"
