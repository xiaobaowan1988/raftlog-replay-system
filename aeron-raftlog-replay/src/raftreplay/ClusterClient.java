package raftreplay;

import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Sends commands into the cluster ingress. Each arg after the base dir is a
 * command "I:key" (increment) or "Q:key" (query); replies (the new counter
 * value) are printed.
 */
public final class ClusterClient {
    public static void main(final String[] args) {
        final String aeronDir = args[0] + "/driver";

        final EgressListener egress = (clusterSessionId, timestamp, buffer, offset, length, header) ->
                System.out.println("[client] reply=" + buffer.getLong(offset));

        final AeronCluster.Context ctx = new AeronCluster.Context()
                .aeronDirectoryName(aeronDir)
                .egressListener(egress)
                .ingressChannel("aeron:udp")
                .ingressEndpoints("0=localhost:20000")
                .egressChannel("aeron:udp?endpoint=localhost:0");

        final BackoffIdleStrategy idle = new BackoffIdleStrategy();
        try (AeronCluster cluster = AeronCluster.connect(ctx)) {
            System.out.println("[client] connected, leaderMemberId=" + cluster.leaderMemberId());
            final UnsafeBuffer buf = new UnsafeBuffer(new byte[64]);
            for (int i = 1; i < args.length; i++) {
                final String cmd = args[i];
                buf.putByte(0, (byte) cmd.charAt(0));
                final int kl = buf.putStringWithoutLengthAscii(1, cmd.substring(2));
                idle.reset();
                while (cluster.offer(buf, 0, 1 + kl) < 0) {
                    idle.idle();
                    cluster.pollEgress();
                }
                // drain replies briefly
                final long deadline = System.nanoTime() + 400_000_000L;
                while (System.nanoTime() < deadline) {
                    cluster.pollEgress();
                    idle.idle();
                }
            }
            System.out.println("[client] sent " + (args.length - 1) + " commands");
        }
    }
}
