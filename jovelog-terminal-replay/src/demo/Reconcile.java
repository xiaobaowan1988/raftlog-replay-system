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
 * Reconciles the REPLAY-computed state against PRODUCTION's per-shard state roots.
 * For each shard it folds from genesis and, at every seq where production published
 * a {shard, seq, root}, compares its own fingerprint. Reports per shard: all matched,
 * or the FIRST (shard, seq) that diverged — continuous, per-shard, localized.
 *
 * -DfoldM=... injects a replay logic drift to show a mismatch being localized.
 */
public final class Reconcile {
    static final long SEED = 1_125_899_906_842_597L;

    public static void main(String[] args) throws Exception {
        final String dataDir = args[0];
        final String rootsFile = args[1];
        final int parallelism = Integer.parseInt(args[2]);
        final long foldM = Long.parseLong(System.getProperty("foldM", "1000003"));

        // production roots: shard -> (seq -> root)
        final Map<Integer, TreeMap<Long, Long>> roots = new HashMap<>();
        for (String line : Files.readAllLines(Path.of(rootsFile))) {
            String[] p = line.split(",");
            roots.computeIfAbsent(Integer.parseInt(p[0]), k -> new TreeMap<>())
                 .put(Long.parseLong(p[1]), Long.parseLong(p[2]));
        }
        final List<Integer> shards = new ArrayList<>(roots.keySet());
        Collections.sort(shards);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(parallelism);

        final DataStream<Tuple2<Integer, String>> report = env
                .fromCollection(shards).rebalance()
                .flatMap(new Reconciler(dataDir, roots, foldM))
                .returns(org.apache.flink.api.common.typeinfo.Types.TUPLE(
                        org.apache.flink.api.common.typeinfo.Types.INT,
                        org.apache.flink.api.common.typeinfo.Types.STRING));

        final TreeMap<Integer, String> out = new TreeMap<>();
        try (CloseableIterator<Tuple2<Integer, String>> it = report.executeAndCollect()) {
            while (it.hasNext()) { Tuple2<Integer, String> t = it.next(); out.put(t.f0, t.f1); }
        }
        boolean allOk = true;
        for (Map.Entry<Integer, String> e : out.entrySet()) {
            System.out.println("  shard=" + e.getKey() + "  " + e.getValue());
            if (e.getValue().startsWith("MISMATCH")) allOk = false;
        }
        System.out.println(allOk ? "RECONCILE: ALL SHARDS MATCH PRODUCTION" : "RECONCILE: DIVERGENCE FOUND (see above)");
    }

    static final class Reconciler implements FlatMapFunction<Integer, Tuple2<Integer, String>> {
        private final String dataDir; private final HashMap<Integer, TreeMap<Long, Long>> roots; private final long M;
        Reconciler(String dataDir, Map<Integer, TreeMap<Long, Long>> roots, long m) {
            this.dataDir = dataDir; this.roots = new HashMap<>(roots); this.M = m;
        }

        @Override
        public void flatMap(Integer shard, Collector<Tuple2<Integer, String>> out) throws Exception {
            final TreeMap<Long, Long> cps = roots.get(shard);
            long fp = SEED, cursor = 0;
            int checked = 0, matched = 0; long firstBad = -1;
            for (File f : ProducerWithRoots.sortedBins(new File(dataDir, "shard=" + shard))) {
                for (long[] rec : ProducerWithRoots.records(f)) {
                    long seq = rec[0], value = rec[1];
                    if (seq <= cursor) continue;
                    fp = fp * M + value;
                    cursor = seq;
                    Long prodRoot = cps.get(seq);
                    if (prodRoot != null) {
                        checked++;
                        if (fp == prodRoot) matched++;
                        else if (firstBad < 0) firstBad = seq;
                    }
                }
            }
            String res = (firstBad < 0)
                    ? "OK (checkpoints " + matched + "/" + checked + " matched)"
                    : "MISMATCH first diverged at seq=" + firstBad + "  (checkpoints " + matched + "/" + checked + " matched)";
            out.collect(Tuple2.of(shard, res));
        }
    }
}
