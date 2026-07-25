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
 * STAGE 2: fold the jovelog terminal state from an S3 initial state + the .bin raftlog.
 *
 * Internal invariants (need NO external oracle):
 *   - completeness: strict +1 per shard; a gap throws REPLAY_INCOMPLETE.
 *   - dedup: (shard,seq) cursor drops duplicates.
 *   - determinism: order-sensitive fold, atomic per shard -> parallelism-invariant.
 *
 * The fold multiplier is injectable via -DfoldM=... to simulate "logic drift": a
 * wrong apply that STILL passes every internal invariant but produces a terminal
 * that only an oracle (reference / production / golden) can flag.
 */
public final class RaftlogTerminalJob {
    public static void main(String[] args) throws Exception {
        final String dataDir = args[0];
        final int parallelism = Integer.parseInt(args[1]);
        final long foldM = Long.parseLong(System.getProperty("foldM", "1000003"));

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
                .fromCollection(shards).rebalance()
                .flatMap(new ShardReplayer(dataDir, init, foldM))
                .returns(org.apache.flink.api.common.typeinfo.Types.TUPLE(
                        org.apache.flink.api.common.typeinfo.Types.INT,
                        org.apache.flink.api.common.typeinfo.Types.LONG));

        final TreeMap<Integer, Long> terminal = new TreeMap<>();
        try (CloseableIterator<Tuple2<Integer, Long>> it = perShard.executeAndCollect()) {
            while (it.hasNext()) {
                Tuple2<Integer, Long> t = it.next();
                terminal.put(t.f0, t.f1);
            }
        } catch (Exception e) {
            String msg = rootMessage(e);
            if (msg != null && msg.contains("REPLAY_INCOMPLETE")) {
                System.out.println("PARALLELISM=" + parallelism + "  COMPLETENESS-CHECK FAILED -> " + msg
                        + "   (internal check caught it; no oracle needed)");
                return;
            }
            throw e;
        }
        System.out.println("PARALLELISM=" + parallelism + "  TERMINAL " + terminal + "  sha=" + hash(terminal)
                + (foldM == 1000003L ? "" : "   [foldM=" + foldM + " -> LOGIC DRIFT]"));
    }

    static final class ShardReplayer implements FlatMapFunction<Integer, Tuple2<Integer, Long>> {
        private final String dataDir;
        private final HashMap<Integer, long[]> init;
        private final long M;
        ShardReplayer(String dataDir, Map<Integer, long[]> init, long m) { this.dataDir = dataDir; this.init = new HashMap<>(init); this.M = m; }

        @Override
        public void flatMap(Integer shard, Collector<Tuple2<Integer, Long>> out) throws Exception {
            final long startSeq = init.get(shard)[0];
            long fp = init.get(shard)[1];
            long cursor = startSeq;
            final File dir = new File(dataDir, "shard=" + shard);
            final File[] files = dir.listFiles((d, name) -> name.endsWith(".bin"));
            Arrays.sort(files, Comparator.comparingLong(f -> Long.parseLong(f.getName().split("_")[1])));
            for (File f : files) {
                final byte[] bytes = Files.readAllBytes(f.toPath());
                try (DataInputStream in = new DataInputStream(new java.io.ByteArrayInputStream(bytes))) {
                    in.readInt();                          // MAGIC
                    while (in.available() > 0) {
                        in.readByte();
                        long seq = in.readLong();
                        in.readLong();
                        in.readInt();
                        int len = in.readInt();
                        byte[] data = new byte[len];
                        in.readFully(data);
                        long value = new DataInputStream(new java.io.ByteArrayInputStream(data)).readLong();
                        if (seq <= cursor) continue;       // dedup / already-seen
                        if (seq != cursor + 1)             // COMPLETENESS: strict +1
                            throw new RuntimeException("REPLAY_INCOMPLETE shard=" + shard + " expectedSeq=" + (cursor + 1) + " gotSeq=" + seq);
                        fp = fp * M + value;               // order-sensitive fold (M injectable)
                        cursor = seq;
                    }
                }
            }
            out.collect(Tuple2.of(shard, fp));
        }
    }

    static String rootMessage(Throwable e) {
        Throwable t = e; String m = null;
        while (t != null) { if (t.getMessage() != null) m = t.getMessage(); t = t.getCause(); }
        return m;
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
