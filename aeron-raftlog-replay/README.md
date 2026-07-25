# Aeron Cluster raftlog replay — initial state (snapshot) + raft log → terminal state

A minimal, runnable **Aeron Cluster** that demonstrates the question:

> Given an initial state (a snapshot, e.g. persisted to S3) and the raft log,
> can we reconstruct the terminal state?

**Yes — exactly, not an estimate**, as long as the state machine is
deterministic, the log is complete and ordered, and the snapshot carries its log
position. This is precisely how Aeron Cluster recovers.

```
terminal_state = replay( snapshot , raftlog[ snapshotPosition .. end ] )
```

## What it runs

A real single-node cluster: `ArchivingMediaDriver` + `ConsensusModule` (Raft) +
`ClusteredServiceContainer`. The state machine (`CounterClusteredService`) keeps
per-key counters — every committed log entry flows through `onSessionMessage`.

- `src/raftreplay/CounterClusteredService.java` — the deterministic RSM:
  `onSessionMessage` (apply), `onTakeSnapshot` (serialize state), `onStart`
  (load snapshot on recovery). Logs each apply as `LIVE` or `REPLAY`.
- `src/raftreplay/ClusterNode.java` — launches the cluster; archive + cluster
  dirs persist across restarts so relaunch triggers recovery.
- `src/raftreplay/ClusterClient.java` — sends `I:key` (increment) / `Q:key`
  (query) commands through cluster ingress.

## Run

```bash
cd aeron-raftlog-replay
./run-demo.sh          # Java 17+ required (Aeron 1.44). ~1 min incl. download.
./run-demo.sh clean
```

If your default `java` isn't 17+, pass one: `JAVA=/path/to/jdk17/bin/java ./run-demo.sh`.

## What it proves (observed output)

```
apply I:a I:a I:b                 -> state {a:2, b:1}
take SNAPSHOT                     -> onTakeSnapshot wrote {a:2, b:1}   (the "initial state")
apply I:a I:b I:c                 -> terminal state {a:3, b:2, c:1}
CRASH (kill -9)                   -> no shutdown snapshot
RESTART -> RECOVERY:
    [svc] loaded snapshot -> {a=2, b=1}     <- initial state
    [svc] REPLAY op=I a=3                    <- raft log replayed after the snapshot
    [svc] REPLAY op=I b=2
    [svc] REPLAY op=I c=1
    [svc] LEADER-READY STATE a=3 b=2 c=1 sha=550df8a78e3a
```

`sha` of the recovered state equals the SHA-256 of `{a:3,b:2,c:1}` → the terminal
state is reconstructed **bit-for-bit**. Had recovery only loaded the snapshot, it
would be `{a:2,b:1}`; reaching `{a:3,b:2,c:1}` is the post-snapshot log replay.

## Where S3 fits

Aeron stores the raft log and snapshots as **Aeron Archive recordings** on disk
(`data/archive/*.rec`, catalog in `data/cluster/recording.log`). To make them
durable you back that up to object storage — the demo tars the log+snapshot and
uploads it to an S3 bucket (see the repo's `flink-pipeline-tests` for the same
local S3 mock). Recovery from S3 = restore those recordings, then replay.

## When it's exact vs only an estimate

Exact reconstruction requires:

- **Determinism** — `apply(state, cmd)` is a pure function (no wall-clock,
  randomness, map-iteration-order, external I/O). Aeron Cluster is built for this.
- **Complete, ordered log** — every committed entry from the snapshot position
  onward, in commit order (Raft guarantees the total order).
- **Snapshot carries its log position** — so replay starts at exactly the next
  entry (no double-apply, no gap). Aeron stores this in the snapshot recording.

Break any of these (non-deterministic RSM, truncated/gappy log, unknown snapshot
position) and you can only *approximate* the terminal state. External side
effects during apply can't be re-derived from state alone — those need
idempotency keys / dedup.
