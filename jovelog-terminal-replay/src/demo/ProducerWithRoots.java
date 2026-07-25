package demo;

import java.io.DataInputStream;
import java.io.File;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * Stands in for PRODUCTION: as it applies the log, it periodically publishes a
 * per-shard state digest tagged with the seq it is at:  {shard, seq, stateRoot}.
 * stateRoot = the running fingerprint of that shard's state at that seq.
 *
 * This is the "ground truth" the reconciler compares against — the linchpin is
 * that every digest carries the exact per-shard seq it reflects, so comparison
 * is per-shard and needs no global barrier.
 */
public final class ProducerWithRoots {
    static final long M = 1_000_003L;
    static final long SEED = 1_125_899_906_842_597L;

    public static void main(String[] args) throws Exception {
        final String dataDir = args[0];
        final int everyK = Integer.parseInt(args[1]);   // emit a root every K seq per shard
        final String outFile = args[2];

        final File root = new File(dataDir);
        final List<Integer> shards = new ArrayList<>();
        for (File d : Objects.requireNonNull(root.listFiles((f, n) -> n.startsWith("shard=")))) {
            shards.add(Integer.parseInt(d.getName().substring("shard=".length())));
        }
        Collections.sort(shards);

        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(Path.of(outFile)))) {
            for (int shard : shards) {
                long fp = SEED, cursor = 0;
                for (File f : sortedBins(new File(dataDir, "shard=" + shard))) {
                    for (long[] rec : records(f)) {           // rec = {seq, value}
                        long seq = rec[0], value = rec[1];
                        if (seq <= cursor) continue;
                        fp = fp * M + value;
                        cursor = seq;
                        if (seq % everyK == 0) w.println(shard + "," + seq + "," + fp);   // publish digest
                    }
                }
                w.println(shard + "," + cursor + "," + fp);   // final root
            }
        }
        System.out.println("PRODUCER emitted per-shard state roots (every " + everyK + " seq) -> " + outFile);
    }

    static File[] sortedBins(File dir) {
        File[] fs = dir.listFiles((d, n) -> n.endsWith(".bin"));
        Arrays.sort(fs, Comparator.comparingLong(f -> Long.parseLong(f.getName().split("_")[1])));
        return fs;
    }

    static List<long[]> records(File f) throws Exception {
        List<long[]> out = new ArrayList<>();
        byte[] bytes = Files.readAllBytes(f.toPath());
        try (DataInputStream in = new DataInputStream(new java.io.ByteArrayInputStream(bytes))) {
            in.readInt();                       // MAGIC
            while (in.available() > 0) {
                in.readByte(); long seq = in.readLong(); in.readLong(); in.readInt();
                int len = in.readInt(); byte[] data = new byte[len]; in.readFully(data);
                long value = new DataInputStream(new java.io.ByteArrayInputStream(data)).readLong();
                out.add(new long[]{seq, value});
            }
        }
        return out;
    }
}
