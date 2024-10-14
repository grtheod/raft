package org.neo4j.raft;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.raft.utils.RaftClient;
import org.neo4j.raft.utils.TestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

class GrpcTests {
    private final String host = "localhost";
    private final int port = 5005;
    private final List<RaftNode> nodes = new ArrayList<>();
    private final List<PeerConfiguration> configs = new ArrayList<>();

    @BeforeEach
    void setUp() {
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        TestUtils.shutdownNodes(nodes);
        nodes.clear();
        configs.clear();
    }

    @Test
    void sendAndReceiveResponse() throws IOException {
        TestUtils.createNodes(nodes, configs, 1, host, port);

        var client = new RaftClient(host, port);
        var response = client.sendAppendRequest(0);
        assertTrue(response.getSuccess());
    }

    @Test
    void nodeConnectsToPeers() throws IOException, InterruptedException {
        final int numberOfReplicas = 5;
        TestUtils.createAndConnectNodes(nodes, configs, numberOfReplicas, host, port);

        // Assert that they are connected
        var leader = nodes.get(0);
        for (var peer : leader.getPeers()) {
            var request = AppendEntriesRequest
                    .newBuilder()
                    .setTerm(0)
                    .setPrevLogIndex(-1)
                    .build();

            var response = peer.blockingStub().appendEntries(request);
            assertTrue(response.getSuccess());
        }
    }
}
