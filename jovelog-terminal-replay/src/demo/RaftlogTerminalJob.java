package demo;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;

import java.io.DataInputStream;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * Computes the jovelog terminal state with Flink, from an S3 "initial state"
 * (per-shard startSeq + fingerprint) + the .bin raftlog. Mirrors the design's
 * §7.3 assembly: fromCollection(shards).rebalance().flatMap(ShardReplayer).
 *
 * Each shard is replayed entirely inside ONE flatMap call (one subtask): list
 * its .bin files in seq order, dedup by (shard,seq) cursor, apply only seq >
 * startSeq, fold an ORDER-SENSITIVE per-shard fingerprint. Because a shard is
 * atomic to a subtask and read in seq order, the result is invariant to
 * parallelism — which this job proves by running at several parallelisms.
 */
public final class RaftlogTerminalJob {
    static final long M = 1_000_003L;   // must match BinGen

    public static void main(String[] args) throws Exception {
        final String dataDir = args[0];
        final int parallelism = Integer.parseInt(args[1]);

        // load the S3 "initial state": shard -> {startSeq, fingerprintAtStartSeq}
        final Map<Integer, long[]> init = new HashMap<>();
        for (String line : Files.readAllLines(Path.of(dataDir, "initial-state.txt"))) {
            String[] p = line.split(",");
            init.put(Integer.parseInt(p[0]), new long[]{Long.parseLong(p[1]), Long.parseLong(p[2])});
        }
        final List<Integer> shards = new ArrayList<>(init.keySet());
        Collections.sort(shards);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(parallelism);

        final DataStream<Tuple2<Integer, Long>> perShard = env
                .fromCollection(shards)
                .rebalance() // one shard per subtask; shards independent
                .flatMap(new ShardReplayer(dataDir, init))
                .returns(org.apache.flink.api.common.typeinfo.Types.TUPLE(
                        org.apache.flink.api.common.typeinfo.Types.INT,
                        org.apache.flink.api.common.typeinfo.Types.LONG));

        final TreeMap<Integer, Long> terminal = new TreeMap<>();
        try (CloseableIterator<Tuple2<Integer, Long>> it = perShard.executeAndCollect()) {
            while (it.hasNext()) {
                Tuple2<Integer, Long> t = it.next();
                terminal.put(t.f0, t.f1);
            }
        }
        System.out.println("PARALLELISM=" + parallelism + "  TERMINAL " + terminal + "  sha=" + hash(terminal));
    }

    /** Replays one shard: seq-ordered files, cursor dedup, startSeq skip, order-sensitive fold. */
    static final class ShardReplayer implements FlatMapFunction<Integer, Tuple2<Integer, Long>> {
        private final String dataDir;
        private final HashMap<Integer, long[]> init;
        ShardReplayer(String dataDir, Map<Integer, long[]> init) { this.dataDir = dataDir; this.init = new HashMap<>(init); }

        @Override
        public void flatMap(Integer shard, Collector<Tuple2<Integer, Long>> out) throws Exception {
            final long startSeq = init.get(shard)[0];
            long fp = init.get(shard)[1];      // seeded from the S3 snapshot fingerprint
            long cursor = startSeq;            // already-applied max seq (dedup + startSeq boundary)

            final File dir = new File(dataDir, "shard=" + shard);
            final File[] files = dir.listFiles((d, name) -> name.endsWith(".bin"));
            Arrays.sort(files, Comparator.comparingLong(f -> minSeqOf(f.getName())));  // seq order across files

            for (File f : files) {
                final byte[] bytes = Files.readAllBytes(f.toPath());
                try (DataInputStream in = new DataInputStream(new java.io.ByteArrayInputStream(bytes))) {
                    final int magic = in.readInt();
                    if (magic != 0xBABECAFE) throw new IllegalStateException("bad magic in " + f);
                    while (in.available() > 0) {
                        in.readByte();                 // SEP
                        final long seq = in.readLong();
                        in.readLong();                 // kafkaOffset
                        in.readInt();                  // shard
                        final int len = in.readInt();
                        final byte[] data = new byte[len];
                        in.readFully(data);
                        final long value = new DataInputStream(new java.io.ByteArrayInputStream(data)).readLong();
                        if (seq <= cursor) continue;   // dedup / already-seen / <= startSeq
                        fp = fp * M + value;           // ORDER-SENSITIVE fold
                        cursor = seq;
                    }
                }
            }
            out.collect(Tuple2.of(shard, fp));
        }

        static long minSeqOf(String name) {          // jovelog_<minSeq>_<maxSeq>_..._st0.bin
            String[] p = name.split("_");
            return Long.parseLong(p[1]);
        }
    }

    static String hash(TreeMap<Integer, Long> m) {
        StringBuilder sb = new StringBuilder();
        m.forEach((k, v) -> sb.append(k).append('=').append(v).append(';'));
        try {
            byte[] h = java.security.MessageDigest.getInstance("SHA-256").digest(sb.toString().getBytes("UTF-8"));
            StringBuilder hex = new StringBuilder();
            for (int i = 0; i < 8; i++) hex.append(String.format("%02x", h[i]));
            return hex.toString();
        } catch (Exception e) { return "err"; }
    }
}
