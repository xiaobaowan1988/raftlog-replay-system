# Flink pipeline tests — Kafka / MySQL-CDC → S3, with state restore from S3

Reproducible, **local** integration tests for Apache Flink data pipelines:

- **Kafka → S3** with keyed state, and **restore of that state from an S3 savepoint**.
- **MySQL (CDC) → S3** capturing snapshot + insert/update/delete, and **restore
  that resumes from the exact binlog offset stored in S3**.

No Docker, no Kubernetes, no cloud account. Flink/Kafka/JDK are plain tarballs;
S3 is a local [s3proxy](https://github.com/gaul/s3proxy) mock that speaks the
same `s3a://` API and checkpoint/savepoint mechanics as real S3 (to point at real
S3, just change `s3.endpoint` / `s3.access-key` / `s3.secret-key`).

> These jobs are the kind of workload the GitOps dashboard in this repo deploys
> as `FlinkDeployment` CRs — here we actually run them and verify behavior.

## Layout

```
flink-pipeline-tests/
├── common.sh                 # download+configure Java17/Flink/Kafka/s3proxy; start/stop helpers
├── run-kafka-s3.sh           # Kafka→S3 + savepoint-restore demo
├── run-mysql-cdc-s3.sh       # MySQL-CDC→S3 + restore-from-binlog-offset demo
└── jobs/
    ├── FlinkKafkaS3Job.java  # KafkaSource → keyed ValueState count → FileSink(s3a)
    └── MySqlCdcToS3Job.java   # MySqlSource(Debezium JSON) → FileSink(s3a)
```

Downloads and runtime data land in `./.work/` (git-ignored).

## Requirements

- Linux x86_64, `curl`, `unzip`, `jar`, ~2 GB disk for the tarballs.
- Network access to `archive.apache.org`, `repo1.maven.org`, and GitHub releases.
- The MySQL demo installs/starts `mysql-server-8.0` via apt (needs root; works
  without systemd). If you already run MySQL 8 with ROW binlog, it's reused.

## Run

```bash
cd flink-pipeline-tests

./run-kafka-s3.sh        # ~2-3 min incl. downloads
./run-mysql-cdc-s3.sh    # ~2-3 min incl. downloads

./run-kafka-s3.sh clean  # tear down + delete .work
```

## What each demo proves

### Kafka → S3 (`run-kafka-s3.sh`)

Produces `a, a, b`, then stop-with-savepoint to S3, restore, produce `a, b`:

```
a=1  a=2  b=1        # before savepoint
a=3  b=2             # after restore from S3 — counts CONTINUE, not reset
```

The keyed `ValueState` is small, so Flink inlines it into the savepoint's
`_metadata` in S3 (`a→2, b→1` at savepoint time). Committed S3 output part files
under `s3a://flink-state/output/` show the same continuity.

### MySQL CDC → S3 (`run-mysql-cdc-s3.sh`)

Snapshots the `orders` table, then applies `INSERT/UPDATE/DELETE`:

```
[snapshot] id=1/2/3
[INSERT]   id=4
[UPDATE]   id=1 qty 3 -> 10     (before + after)
[DELETE]   id=2
```

All land in `s3a://flink-state/mysql-output/` as append-only Debezium JSON. The
S3 savepoint stores the CDC source state — the **binlog file + position** — so a
restore resumes exactly there (no re-snapshot, no gaps/dups).

## Notes / knobs

- **DECIMAL as base64?** Debezium's default `decimal.handling.mode=precise`
  encodes DECIMAL as bytes. `MySqlCdcToS3Job` sets `decimal.handling.mode=string`
  for readable amounts (`"amount":"123.45"`).
- **Update/delete to a file sink:** the job uses `JsonDebeziumDeserializationSchema`
  (append-only), so every change is a JSON line — the FileSink accepts it. A Flink
  SQL `INSERT INTO <filesystem> SELECT * FROM <cdc>` would fail on retract streams;
  for current-state tables use a changelog sink (Hudi/Iceberg/Paimon).
- **State backend:** these use the default HashMapStateBackend (canonical
  savepoints, small state inlined). With RocksDB, checkpoints upload SST files to
  `s3a://.../checkpoints/<job>/shared/` instead.
- **Versions:** Flink 1.18.1, Kafka 3.7.1, Temurin JDK 17, mysql-cdc 3.0.1.
  Override via env vars (see `common.sh`).
