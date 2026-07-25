package raftreplay;

import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.concurrent.ShutdownSignalBarrier;

import java.io.File;

/**
 * Single-node Aeron Cluster: ArchivingMediaDriver + ConsensusModule (Raft) +
 * ClusteredServiceContainer (the counter state machine).
 *
 * The archive dir (recorded raft log + snapshots) and the cluster dir persist
 * across restarts, so relaunching against the same base dir triggers recovery:
 * load latest snapshot, then replay the recorded log after it.
 */
public final class ClusterNode {
    public static void main(final String[] args) {
        final String base = args[0];
        final String aeronDir = base + "/driver";
        final File archiveDir = new File(base, "archive");
        final File clusterDir = new File(base, "cluster");

        final MediaDriver.Context driverCtx = new MediaDriver.Context()
                .aeronDirectoryName(aeronDir)
                .threadingMode(ThreadingMode.SHARED)
                .termBufferSparseFile(true)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true);

        final Archive.Context archiveCtx = new Archive.Context()
                .aeronDirectoryName(aeronDir)
                .archiveDir(archiveDir)
                .controlChannel("aeron:udp?endpoint=localhost:8010")
                .localControlChannel("aeron:ipc?term-length=64k")
                .replicationChannel("aeron:udp?endpoint=localhost:0")
                .recordingEventsEnabled(false)
                .threadingMode(ArchiveThreadingMode.SHARED)
                .deleteArchiveOnStart(false);

        final ConsensusModule.Context consensusCtx = new ConsensusModule.Context()
                .aeronDirectoryName(aeronDir)
                .clusterDir(clusterDir)
                .clusterMemberId(0)
                .clusterMembers("0,localhost:20000,localhost:20001,localhost:20002,localhost:20003,localhost:8010")
                .ingressChannel("aeron:udp?term-length=64k")
                .replicationChannel("aeron:udp?endpoint=localhost:0");

        final ClusteredServiceContainer.Context serviceCtx = new ClusteredServiceContainer.Context()
                .aeronDirectoryName(aeronDir)
                .clusterDir(clusterDir)
                .clusteredService(new CounterClusteredService());

        try (ArchivingMediaDriver amd = ArchivingMediaDriver.launch(driverCtx, archiveCtx);
             ConsensusModule cm = ConsensusModule.launch(consensusCtx);
             ClusteredServiceContainer csc = ClusteredServiceContainer.launch(serviceCtx)) {
            System.out.println("[node] cluster UP (base=" + base + ")");
            new ShutdownSignalBarrier().await();
            System.out.println("[node] shutdown signal received");
        }
    }
}
