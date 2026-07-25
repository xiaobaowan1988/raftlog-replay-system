package demo;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;

import java.io.DataInputStream;
import java.io.File;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * STAGE 1 (the "materializer"): folds the jovelog .bin from genesis (seq 1) up to a
 * per-shard cut, and PERSISTS its state — this is where an "initial state" actually
 * comes from. The output file is the analog of a Flink checkpoint / savepoint:
 *
 *     per shard:  (cursor = seq consumed up to, fingerprint = state at that cursor)
 *
 * i.e. keyed state + per-shard source offset. STAGE 2 (RaftlogTerminalJob) then
 * resumes from this file and folds only seq > cursor.
 *
 * The genesis fold (SEED, M) matches BinGen so that
 *   materialize(1..cut) then replay(cut+1..N) == full fold(1..N).
 */
public final class Materializer {
    static final long M = 1_000_003L;
    static final long SEED = 1_125_899_906_842_597L;

    public static void main(String[] args) throws Exception {
        final String dataDir = args[0];
        final String cutSpec = args[1];          // "5:8,42:5,109:12"
        final String outFile = args[2];
        final int parallelism = args.length > 3 ? Integer.parseInt(args[3]) : 1;

        final HashMap<Integer, Long> cut = new HashMap<>();
        for (String kv : cutSpec.split(",")) {
            String[] p = kv.split(":");
            cut.put(Integer.parseInt(p[0]), Long.parseLong(p[1]));
        }
        final List<Integer> shards = new ArrayList<>(cut.keySet());
        Collections.sort(shards);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(parallelism);

        final DataStream<Tuple3<Integer, Long, Long>> checkpoints = env
                .fromCollection(shards)
                .rebalance()
                .flatMap(new FoldUpToCut(dataDir, cut))
                .returns(org.apache.flink.api.common.typeinfo.Types.TUPLE(
                        org.apache.flink.api.common.typeinfo.Types.INT,
                        org.apache.flink.api.common.typeinfo.Types.LONG,
                        org.apache.flink.api.common.typeinfo.Types.LONG));

        final TreeMap<Integer, long[]> state = new TreeMap<>();
        try (CloseableIterator<Tuple3<Integer, Long, Long>> it = checkpoints.executeAndCollect()) {
            while (it.hasNext()) {
                Tuple3<Integer, Long, Long> t = it.next();
                state.put(t.f0, new long[]{t.f1, t.f2});
            }
        }
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(Path.of(outFile)))) {
            for (Map.Entry<Integer, long[]> e : state.entrySet()) {
                w.println(e.getKey() + "," + e.getValue()[0] + "," + e.getValue()[1]);
            }
        }
        System.out.println("STAGE1 materialized checkpoint (shard,cursor,fp) -> " + outFile);
        state.forEach((s, v) -> System.out.println("   shard=" + s + " cursor=" + v[0] + " fp=" + v[1]));
    }

    static final class FoldUpToCut implements FlatMapFunction<Integer, Tuple3<Integer, Long, Long>> {
        private final String dataDir;
        private final HashMap<Integer, Long> cut;
        FoldUpToCut(String dataDir, Map<Integer, Long> cut) { this.dataDir = dataDir; this.cut = new HashMap<>(cut); }

        @Override
        public void flatMap(Integer shard, Collector<Tuple3<Integer, Long, Long>> out) throws Exception {
            final long cutSeq = cut.get(shard);
            long fp = SEED;                 // genesis
            long cursor = 0;
            final File dir = new File(dataDir, "shard=" + shard);
            final File[] files = dir.listFiles((d, name) -> name.endsWith(".bin"));
            Arrays.sort(files, Comparator.comparingLong(f -> minSeqOf(f.getName())));
            for (File f : files) {
                final byte[] bytes = Files.readAllBytes(f.toPath());
                try (DataInputStream in = new DataInputStream(new java.io.ByteArrayInputStream(bytes))) {
                    in.readInt();          // MAGIC
                    while (in.available() > 0) {
                        in.readByte();
                        long seq = in.readLong();
                        in.readLong();
                        in.readInt();
                        int len = in.readInt();
                        byte[] data = new byte[len];
                        in.readFully(data);
                        long value = new DataInputStream(new java.io.ByteArrayInputStream(data)).readLong();
                        if (seq <= cursor || seq > cutSeq) continue;   // dedup + stop at the cut
                        fp = fp * M + value;
                        cursor = seq;
                    }
                }
            }
            out.collect(Tuple3.of(shard, cutSeq, fp));   // cursor recorded = the cut (snapshot boundary)
        }

        static long minSeqOf(String name) { return Long.parseLong(name.split("_")[1]); }
    }
}
