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

/** Same single-node cluster as ClusterNode, but hosts the ExportingCounterService. */
public final class ClusterNodeExport {
    public static void main(final String[] args) {
        final String base = args[0];
        final String aeronDir = base + "/driver";

        final MediaDriver.Context driverCtx = new MediaDriver.Context()
                .aeronDirectoryName(aeronDir).threadingMode(ThreadingMode.SHARED)
                .termBufferSparseFile(true).dirDeleteOnStart(true).dirDeleteOnShutdown(true);

        final Archive.Context archiveCtx = new Archive.Context()
                .aeronDirectoryName(aeronDir).archiveDir(new File(base, "archive"))
                .controlChannel("aeron:udp?endpoint=localhost:8010")
                .localControlChannel("aeron:ipc?term-length=64k")
                .replicationChannel("aeron:udp?endpoint=localhost:0")
                .recordingEventsEnabled(false).threadingMode(ArchiveThreadingMode.SHARED)
                .deleteArchiveOnStart(false);

        final ConsensusModule.Context consensusCtx = new ConsensusModule.Context()
                .aeronDirectoryName(aeronDir).clusterDir(new File(base, "cluster"))
                .clusterMemberId(0)
                .clusterMembers("0,localhost:20000,localhost:20001,localhost:20002,localhost:20003,localhost:8010")
                .ingressChannel("aeron:udp?term-length=64k")
                .replicationChannel("aeron:udp?endpoint=localhost:0");

        final ClusteredServiceContainer.Context serviceCtx = new ClusteredServiceContainer.Context()
                .aeronDirectoryName(aeronDir).clusterDir(new File(base, "cluster"))
                .clusteredService(new ExportingCounterService());

        try (ArchivingMediaDriver amd = ArchivingMediaDriver.launch(driverCtx, archiveCtx);
             ConsensusModule cm = ConsensusModule.launch(consensusCtx);
             ClusteredServiceContainer csc = ClusteredServiceContainer.launch(serviceCtx)) {
            System.out.println("[node] export cluster UP");
            new ShutdownSignalBarrier().await();
        }
    }
}
