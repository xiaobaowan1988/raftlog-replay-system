# jovelog terminal-state replay — S3 init + (shard, seq) raftlog → terminal, with Flink

Models the `okj-jove-flink` jovelog design and answers: **given an S3 initial
state, can Flink compute the terminal state from the sharded raftlog?** Yes — and
the result is **invariant across parallelism**, because the log is sharded by user
and each shard is independently ordered.

## The model (from the design doc)

- jovelog = the sequencer's (Aeron) raft log, republished to Kafka.
- Sharded by user into **128 shards** (0..127). Each entry carries `(shard, seq)`;
  `seq` is strictly **+1 per shard**. **Same-shard order = seq; cross-shard = no order.**
- Backed up to S3, one file per shard, seq strictly increasing:
  `raft-log-v2/shard=<n>/…/jovelog_<minSeq>_<maxSeq>_<minCt>_<maxCt>_st<k>.bin`
- `.bin` envelope:
  `MAGIC(BA BE CA FE) + [ SEP(EF) | seq(8) | kafkaOffset(8) | shard(4) | len(4) | data ]*`

Because `seq` is a dense per-shard index, it is simultaneously the **ordering key**,
the **completeness check** (strict +1 ⇒ exact gap detection), and half of the
**dedup key** `(shard, seq)`.

## What this demo does

1. `BinGen` writes real `.bin` files for a few shards (with a duplicate injected,
   since the design promises no-loss but **not** no-duplicate), plus
   `initial-state.txt` = the S3 initial state: per-shard `startSeq` + the
   fingerprint of the state at that seq (the snapshot boundary).
2. `RaftlogTerminalJob` (Flink) mirrors the design's §7.3 assembly —
   `fromCollection(shards).rebalance().flatMap(ShardReplayer)` — where each shard is
   replayed inside one subtask: list `.bin` in seq order, **dedup by `(shard,seq)`
   cursor**, apply only `seq > startSeq`, and fold an **order-sensitive** per-shard
   fingerprint. Terminal state = `{shard → fingerprint}`, hashed.
3. It runs at parallelism 1, 2, 4, 8 and checks the hash is identical and equals the
   from-`seq=1` reference.

## Run

```bash
cd jovelog-terminal-replay
./run-demo.sh      # Java 17 + Flink fetched into ./.work on first run
./run-demo.sh clean
```

## Observed result

```
REFERENCE_TERMINAL {5=…, 42=…, 109=…} sha=102de9c8bdf50bf2      # full fold from seq 1
SHUFFLED_ORDER     … sha=32a4f77d061b3635  (DIFFERS ⇒ fold is order-sensitive)

PARALLELISM=1  TERMINAL {…}  sha=102de9c8bdf50bf2
PARALLELISM=2  TERMINAL {…}  sha=102de9c8bdf50bf2
PARALLELISM=4  TERMINAL {…}  sha=102de9c8bdf50bf2
PARALLELISM=8  TERMINAL {…}  sha=102de9c8bdf50bf2
```

Three things proven at once:

- **S3 init + replay = terminal.** Starting from the per-shard `startSeq` snapshot and
  folding only `seq > startSeq` reproduces the full-from-seq-1 hash.
- **`(shard, seq)` dedup works.** A duplicate record was injected in shard 5; the cursor
  drops it and shard 5 still matches.
- **Parallelism-invariant.** 1→8 give the identical hash. The fold is order-sensitive
  (the shuffled hash differs), so this invariance is meaningful: per-shard seq order is
  preserved even under parallel execution, because a shard is atomic to one subtask.

## Where does the initial state come from? (`run-two-stage.sh`)

The initial state is **not** computed at replay time — it's a **prior consumption's
persisted checkpoint**: per-shard `(cursor, state)` = keyed state + per-shard seq
offset (exactly what a Flink checkpoint/savepoint holds).

- **Stage 1 — `Materializer`**: folds the `.bin` from genesis (seq 1) up to a cut and
  persists per-shard `(cursor, fingerprint)`. *This is the initial state, produced.*
- **Stage 2 — `RaftlogTerminalJob`**: resumes from that checkpoint, folds only
  `seq > cursor` → terminal.

```
STAGE 1 cut {5:8,42:5,109:12}   -> checkpoint  5,8,-3250815405651330967  42,5,…  109,12,…
STAGE 2 from checkpoint         -> sha=102de9c8bdf50bf2   (== from-seq-1 reference)

STAGE 1 cut {5:15,42:18,109:3}  -> a DIFFERENT checkpoint
STAGE 2 from checkpoint         -> sha=102de9c8bdf50bf2   (same terminal — cut is arbitrary)
```

So: the snapshot is just a **persisted fold-so-far tagged with its per-shard seq
cut**; where you cut is arbitrary; and with **no** snapshot you simply cut at seq 0 and
replay the whole log (the `REFERENCE` path). jovelog's S3 backup is *log-only* — the
snapshot is a separate artifact (a consumer checkpoint / a state-machine snapshot / a
DB export) that records the per-shard seq it's consistent at. The one correctness rule:
the checkpoint's `cursor` must exactly match the state it stores (else double-apply or gaps).

## Why it works (and the one caveat)

Per-shard order is all a per-user (per-shard) state machine needs, so 128 shards replay
in parallel with no cross-shard coordination. The **one caveat**: if the state you want
has **cross-shard** dependencies (an operation touching two users in different shards),
per-shard replay can't order those — jovelog's per-shard `seq` gives no cross-shard order.
For that you'd need a global sequence. For per-user/per-order derived state, this is the
canonical, scalable approach — the same shape as the design's "reconstruction-type"
replay (`ReplaySpec.startSeqByShard`).
