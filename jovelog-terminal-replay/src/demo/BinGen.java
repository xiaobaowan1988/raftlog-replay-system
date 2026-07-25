package demo;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * Generates jovelog .bin files in the exact envelope format from the design:
 *   file   = MAGIC(BA BE CA FE) + record*
 *   record = SEP(EF) + seq(8B) + kafkaOffset(8B) + shard(4B) + len(4B) + data(len)  (big-endian)
 *   data   = 8-byte "value" (stands in for the JoveLog protobuf payload)
 *
 * Also writes initial-state.txt: per-shard  (startSeq, fingerprint-at-startSeq) = the S3 "initial state".
 * A per-shard, ORDER-SENSITIVE fingerprint is folded so that "wrong order" would change the result.
 */
public final class BinGen {
    static final int MAGIC = 0xBABECAFE;
    static final byte SEP = (byte) 0xEF;
    static final long M = 1_000_003L;      // fold multiplier (order matters)
    static final long SEED = 1_125_899_906_842_597L;

    static long value(int shard, long seq) { return seq * 2_654_435_761L + shard * 40_503L; }

    static int dropShard = -1; static long dropSeq = -1;   // punch a hole (gap) for the verification demo

    public static void main(String[] args) throws Exception {
        Path out = Path.of(args[0]);
        if (args.length > 1 && !args[1].isEmpty()) {        // optional "shard:seq" to omit
            String[] d = args[1].split(":"); dropShard = Integer.parseInt(d[0]); dropSeq = Long.parseLong(d[1]);
        }
        int[] shards = {5, 42, 109};
        int n = 20;                                  // seq 1..20 per shard
        Map<Integer, Long> startSeq = Map.of(5, 8L, 42, 5L, 109, 12L);

        // reference: full fold from seq 1..n (the terminal state) + a shuffled-order fold (to prove order matters)
        TreeMap<Integer, Long> fullFp = new TreeMap<>();
        TreeMap<Integer, Long> shuffledFp = new TreeMap<>();
        Map<Integer, Long> initFp = new HashMap<>();
        for (int shard : shards) {
            long fp = SEED;
            for (long seq = 1; seq <= n; seq++) {
                fp = fp * M + value(shard, seq);
                if (seq == startSeq.get(shard)) initFp.put(shard, fp);   // snapshot fingerprint at startSeq
            }
            fullFp.put(shard, fp);
            // shuffled order (same values, different order) -> different fp
            List<Long> vs = new ArrayList<>();
            for (long seq = 1; seq <= n; seq++) vs.add(value(shard, seq));
            Collections.rotate(vs, 3); Collections.swap(vs, 0, 7);
            long sf = SEED; for (long v : vs) sf = sf * M + v; shuffledFp.put(shard, sf);
        }

        // write two .bin files per shard (seq 1..10, 11..20); inject a duplicate (shard 5, seq 15 twice)
        for (int shard : shards) {
            Path dir = out.resolve("shard=" + shard);
            Files.createDirectories(dir);
            writeBin(dir.resolve(binName(1, 10)), shard, 1, 10, -1);
            writeBin(dir.resolve(binName(11, 20)), shard, 11, 20, shard == 5 ? 15 : -1);
        }

        // initial-state.txt = the S3 "initial state": shard,startSeq,fingerprintAtStartSeq
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(out.resolve("initial-state.txt")))) {
            for (int shard : shards) w.println(shard + "," + startSeq.get(shard) + "," + initFp.get(shard));
        }

        System.out.println("REFERENCE_TERMINAL " + fullFp + " sha=" + hash(fullFp));
        System.out.println("SHUFFLED_ORDER     " + shuffledFp + " sha=" + hash(shuffledFp) + "   (must DIFFER => fold is order-sensitive)");
        System.out.println("wrote .bin for shards " + Arrays.toString(shards) + " + initial-state.txt (startSeq " + startSeq + ")");
    }

    static String binName(long min, long max) {
        return "jovelog_" + min + "_" + max + "_0_0_st0.bin";
    }

    static void writeBin(Path file, int shard, long from, long to, long dupSeq) throws Exception {
        try (DataOutputStream o = new DataOutputStream(new FileOutputStream(file.toFile()))) {
            o.writeInt(MAGIC);
            for (long seq = from; seq <= to; seq++) {
                if (shard == dropShard && seq == dropSeq) continue;   // punch a hole (gap)
                writeRec(o, shard, seq);
                if (seq == dupSeq) writeRec(o, shard, seq);   // duplicate (same content) -> reader must dedup
            }
        }
    }

    static void writeRec(DataOutputStream o, int shard, long seq) throws Exception {
        o.writeByte(SEP);
        o.writeLong(seq);
        o.writeLong(seq);            // kafkaOffset (stand-in)
        o.writeInt(shard);
        o.writeInt(8);               // len
        o.writeLong(value(shard, seq));
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
