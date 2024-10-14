package org.neo4j.raft.utils;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.neo4j.raft.AppendEntriesRequest;
import org.neo4j.raft.AppendEntriesResponse;
import org.neo4j.raft.RaftServiceGrpc;

public class RaftClient {
    private final RaftServiceGrpc.RaftServiceBlockingStub blockingStub;

    public RaftClient(String host, int port) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        blockingStub = RaftServiceGrpc.newBlockingStub(channel);
    }

    public AppendEntriesResponse sendAppendRequest(int term) {
        var request = AppendEntriesRequest.newBuilder();

        // Update request values here
        request.setTerm(term);
        request.setPrevLogIndex(-1);

        return blockingStub.appendEntries(request.build());
    }
}
