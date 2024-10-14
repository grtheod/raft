package org.neo4j.raft;

import io.grpc.ManagedChannel;

public record PeerConnection(long nodeId,
                             ManagedChannel channel,
                             RaftServiceGrpc.RaftServiceBlockingStub blockingStub,
                             RaftServiceGrpc.RaftServiceFutureStub asyncStub) {
}
