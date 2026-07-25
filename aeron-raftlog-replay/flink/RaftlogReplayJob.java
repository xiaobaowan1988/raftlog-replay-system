package demo;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Flink computes the terminal state from an Aeron Cluster raftlog + an S3 initial
 * state (snapshot). It reads both from S3, replays only the log entries AFTER the
 * snapshot's log position, applies them in raft order (parallelism=1), and writes
 * the terminal state back to S3.
 *
 *   terminal[key] = initial[key] + (# of 'I' commands for key with pos > snapshotPosition)
 */
public class RaftlogReplayJob {
    private static final Pattern CMD =
            Pattern.compile("\\{\"pos\":(\\d+),\"op\":\"(.)\",\"key\":\"([^\"]+)\"}");

    public static void main(final String[] args) throws Exception {
        final String bucket = "s3a://aeron-raftlog";
        final String initPath = bucket + "/initial-state.json";
        final String logPath = bucket + "/raftlog.jsonl";
        final String outPath = bucket + "/flink-terminal.txt";

        // 1) load the S3 initial state (snapshot @ position)
        final String initJson = readAll(initPath);
        final long snapshotPosition = Long.parseLong(group(initJson, "\"snapshotPosition\":(\\d+)"));
        final Map<String, Long> initial = new TreeMap<>();
        final Matcher m = Pattern.compile("\"([a-z])\":(\\d+)").matcher(
                initJson.substring(initJson.indexOf("\"state\"")));
        while (m.find()) {
            initial.put(m.group(1), Long.parseLong(m.group(2)));
        }
        System.out.println("[flink] S3 initial state = " + initial + " @pos=" + snapshotPosition);

        // 2) read the raftlog from S3 and replay entries AFTER the snapshot position
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // apply in raft total order

        final FileSource<String> src = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new Path(logPath))
                .build();
        final DataStream<String> lines = env.fromSource(src, WatermarkStrategy.noWatermarks(), "raftlog-s3");

        final DataStream<Tuple2<String, Long>> running = lines
                .flatMap((String line, org.apache.flink.util.Collector<Tuple2<String, Long>> out) -> {
                    final Matcher mm = CMD.matcher(line.trim());
                    if (mm.matches()) {
                        final long pos = Long.parseLong(mm.group(1));
                        final char op = mm.group(2).charAt(0);
                        final String key = mm.group(3);
                        if (op == 'I' && pos > snapshotPosition) {
                            out.collect(Tuple2.of(key, 1L));
                        }
                    }
                })
                .returns(org.apache.flink.api.common.typeinfo.Types.TUPLE(
                        org.apache.flink.api.common.typeinfo.Types.STRING,
                        org.apache.flink.api.common.typeinfo.Types.LONG))
                .keyBy(t -> t.f0)
                .sum(1); // per-key running count of post-snapshot increments (monotonic)

        // 3) collect: last running value per key = total post-snapshot increments
        final Map<String, Long> postIncr = new HashMap<>();
        try (CloseableIterator<Tuple2<String, Long>> it = running.executeAndCollect()) {
            while (it.hasNext()) {
                final Tuple2<String, Long> t = it.next();
                postIncr.merge(t.f0, t.f1, Math::max);
            }
        }

        // 4) terminal = initial + post-snapshot increments
        final TreeMap<String, Long> terminal = new TreeMap<>(initial);
        postIncr.forEach((k, v) -> terminal.merge(k, v, Long::sum));

        final String hash = hash(terminal);
        final String line = "FLINK terminal " + terminal + " sha=" + hash;
        System.out.println("[flink] " + line);

        // 5) write terminal back to S3
        try (org.apache.flink.core.fs.FSDataOutputStream os =
                     FileSystem.get(new java.net.URI(outPath)).create(new Path(outPath), FileSystem.WriteMode.OVERWRITE)) {
            os.write((line + "\n").getBytes(StandardCharsets.UTF_8));
        }
        System.out.println("[flink] wrote " + outPath);
    }

    private static String readAll(final String uri) throws Exception {
        final Path p = new Path(uri);
        try (FSDataInputStream in = FileSystem.get(new java.net.URI(uri)).open(p)) {
            final java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
            final byte[] buf = new byte[4096];
            int n;
            while ((n = in.read(buf)) > 0) {
                bos.write(buf, 0, n);
            }
            return bos.toString("UTF-8");
        }
    }

    private static String group(final String s, final String regex) {
        final Matcher m = Pattern.compile(regex).matcher(s);
        return m.find() ? m.group(1) : "0";
    }

    private static String hash(final Map<String, Long> state) {
        try {
            final StringBuilder sb = new StringBuilder();
            state.forEach((k, v) -> sb.append(k).append('=').append(v).append(';'));
            final byte[] h = MessageDigest.getInstance("SHA-256")
                    .digest(sb.toString().getBytes(StandardCharsets.UTF_8));
            final StringBuilder hex = new StringBuilder();
            for (int i = 0; i < 6; i++) {
                hex.append(String.format("%02x", h[i]));
            }
            return hex.toString();
        } catch (final Exception e) {
            return "err";
        }
    }
}
