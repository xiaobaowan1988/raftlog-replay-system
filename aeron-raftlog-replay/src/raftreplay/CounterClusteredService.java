package raftreplay;

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Map;
import java.util.TreeMap;

/**
 * A deterministic replicated state machine: per-key counters.
 *
 * This is the "state machine" behind Aeron Cluster's raft log. Every committed
 * log entry flows through onSessionMessage and mutates {@link #counts}. Snapshots
 * (onTakeSnapshot) capture the counts at a known log position; recovery loads the
 * latest snapshot then REPLAYS the log after it — reconstructing the terminal
 * state exactly.
 */
public class CounterClusteredService implements ClusteredService {
    private Cluster cluster;
    private IdleStrategy idle;
    private final TreeMap<String, Long> counts = new TreeMap<>();
    private final MutableDirectBuffer replyBuf = new UnsafeBuffer(new byte[8]);

    @Override
    public void onStart(final Cluster cluster, final Image snapshotImage) {
        this.cluster = cluster;
        this.idle = cluster.idleStrategy();
        if (snapshotImage != null) {
            loadSnapshot(snapshotImage);
            System.out.println("[svc] loaded snapshot -> " + counts);
        } else {
            System.out.println("[svc] fresh start (no snapshot)");
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

        final boolean live = cluster.role() == Cluster.Role.LEADER;
        System.out.println("[svc] " + (live ? "LIVE  " : "REPLAY") + " op=" + op + " " + key + "=" + value);

        if (live && session != null) {
            replyBuf.putLong(0, value);
            while (session.offer(replyBuf, 0, 8) < 0) {
                idle.idle();
            }
        }
    }

    @Override
    public void onTakeSnapshot(final ExclusivePublication snapshotPublication) {
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
        System.out.println("[svc] onTakeSnapshot wrote " + counts.size() + " entries (" + pos + "B) -> " + counts);
    }

    private void loadSnapshot(final Image image) {
        final boolean[] done = {false};
        final FragmentHandler handler = (buffer, offset, length, header) -> {
            int pos = offset;
            final int n = buffer.getInt(pos);
            pos += 4;
            for (int i = 0; i < n; i++) {
                final int kl = buffer.getInt(pos);
                pos += 4;
                final String key = buffer.getStringWithoutLengthAscii(pos, kl);
                pos += kl;
                final long v = buffer.getLong(pos);
                pos += 8;
                counts.put(key, v);
            }
            done[0] = true;
        };
        while (!done[0]) {
            if (image.poll(handler, 1) == 0) {
                if (image.isClosed() || image.isEndOfStream()) {
                    break;
                }
                idle.idle();
            }
        }
    }

    @Override
    public void onRoleChange(final Cluster.Role newRole) {
        System.out.println("[svc] role -> " + newRole);
        if (newRole == Cluster.Role.LEADER) {
            System.out.println("[svc] LEADER-READY " + stateLine());
        }
    }

    @Override public void onSessionOpen(final ClientSession session, final long timestamp) { }
    @Override public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason reason) { }
    @Override public void onTimerEvent(final long correlationId, final long timestamp) { }
    @Override public void onTerminate(final Cluster cluster) {
        System.out.println("[svc] terminate " + stateLine());
    }

    String stateLine() {
        final StringBuilder sb = new StringBuilder("STATE ");
        counts.forEach((k, v) -> sb.append(k).append('=').append(v).append(' '));
        return sb.append("sha=").append(hash()).toString();
    }

    private String hash() {
        try {
            final StringBuilder sb = new StringBuilder();
            counts.forEach((k, v) -> sb.append(k).append('=').append(v).append(';'));
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
