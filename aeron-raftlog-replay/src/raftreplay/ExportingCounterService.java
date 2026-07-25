package raftreplay;

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;

/**
 * Same deterministic counter RSM as CounterClusteredService, but it also EXPORTS
 * the raft log so a different engine (Flink) can replay it:
 *   - raftlog.jsonl     : one committed command per line {pos, op, key}  (LIVE only)
 *   - initial-state.json: the snapshot's state + the logPosition it was taken at
 *
 * pos = Header.position() = the Aeron log position after the entry (our raft index).
 * Export dir is passed via -Dexport.dir=...
 */
public class ExportingCounterService implements ClusteredService {
    private Cluster cluster;
    private IdleStrategy idle;
    private final TreeMap<String, Long> counts = new TreeMap<>();
    private final MutableDirectBuffer replyBuf = new UnsafeBuffer(new byte[8]);
    private final String exportDir = System.getProperty("export.dir", ".");
    private BufferedWriter logWriter;
    private long lastPos = 0;

    @Override
    public void onStart(final Cluster cluster, final Image snapshotImage) {
        this.cluster = cluster;
        this.idle = cluster.idleStrategy();
        try {
            logWriter = new BufferedWriter(new FileWriter(exportDir + "/raftlog.jsonl", true));
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
        if (snapshotImage != null) {
            loadSnapshot(snapshotImage);
        }
    }

    @Override
    public void onSessionMessage(final ClientSession session, final long timestamp,
            final DirectBuffer buffer, final int offset, final int length, final Header header) {
        final char op = (char) buffer.getByte(offset);
        final String key = buffer.getStringWithoutLengthAscii(offset + 1, length - 1);
        final long value = (op == 'I')
                ? counts.merge(key, 1L, Long::sum)
                : counts.getOrDefault(key, 0L);
        lastPos = header.position();

        final boolean live = cluster.role() == Cluster.Role.LEADER;
        if (live) {
            // Export the committed command (with its raft log position) for downstream replay.
            try {
                logWriter.write("{\"pos\":" + lastPos + ",\"op\":\"" + op + "\",\"key\":\"" + key + "\"}\n");
                logWriter.flush();
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
            if (session != null) {
                replyBuf.putLong(0, value);
                while (session.offer(replyBuf, 0, 8) < 0) {
                    idle.idle();
                }
            }
        }
    }

    @Override
    public void onTakeSnapshot(final ExclusivePublication snapshotPublication) {
        // 1) normal Aeron snapshot (into the cluster snapshot recording)
        final MutableDirectBuffer buf = new UnsafeBuffer(new byte[4096]);
        int pos = 0;
        buf.putInt(pos, counts.size());
        pos += 4;
        for (final Map.Entry<String, Long> e : counts.entrySet()) {
            final byte[] k = e.getKey().getBytes(StandardCharsets.US_ASCII);
            buf.putInt(pos, k.length);
            pos += 4;
            buf.putBytes(pos, k);
            pos += k.length;
            buf.putLong(pos, e.getValue());
            pos += 8;
        }
        while (snapshotPublication.offer(buf, 0, pos) < 0) {
            idle.idle();
        }
        // 2) ALSO export the snapshot as the S3 "initial state", tagged with its log position
        final StringBuilder sb = new StringBuilder();
        sb.append("{\"snapshotPosition\":").append(lastPos).append(",\"state\":{");
        boolean first = true;
        for (final Map.Entry<String, Long> e : counts.entrySet()) {
            if (!first) sb.append(',');
            sb.append('"').append(e.getKey()).append("\":").append(e.getValue());
            first = false;
        }
        sb.append("}}");
        try (BufferedWriter w = new BufferedWriter(new FileWriter(exportDir + "/initial-state.json"))) {
            w.write(sb.toString());
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
        System.out.println("[export] snapshot initial-state @pos=" + lastPos + " -> " + counts);
    }

    private void loadSnapshot(final Image image) {
        final boolean[] done = {false};
        while (!done[0]) {
            if (image.poll((b, o, l, h) -> {
                int p = o;
                final int n = b.getInt(p); p += 4;
                for (int i = 0; i < n; i++) {
                    final int kl = b.getInt(p); p += 4;
                    final String key = b.getStringWithoutLengthAscii(p, kl); p += kl;
                    counts.put(key, b.getLong(p)); p += 8;
                }
                done[0] = true;
            }, 1) == 0) {
                if (image.isClosed() || image.isEndOfStream()) break;
                idle.idle();
            }
        }
    }

    @Override public void onRoleChange(final Cluster.Role r) {
        if (r == Cluster.Role.LEADER) System.out.println("[export] LEADER-READY state=" + counts);
    }
    @Override public void onSessionOpen(final ClientSession s, final long t) { }
    @Override public void onSessionClose(final ClientSession s, final long t, final CloseReason r) { }
    @Override public void onTimerEvent(final long c, final long t) { }
    @Override public void onTerminate(final Cluster c) {
        try { if (logWriter != null) logWriter.close(); } catch (final Exception ignore) { }
    }
}
