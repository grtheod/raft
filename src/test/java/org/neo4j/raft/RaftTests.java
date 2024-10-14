package org.neo4j.raft;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.raft.utils.TestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftTests {
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
    void electionsGenerateALeader() throws IOException, InterruptedException {
        final int numberOfReplicas = 5;
        TestUtils.createNodesAndStartRaft(nodes, configs, numberOfReplicas, host, port);
        assertNotEquals(-1L, TestUtils.findLeader(nodes, 10));
    }

    @Test
    void disconnectLeader() throws IOException, InterruptedException {
        final int numberOfReplicas = 5;
        TestUtils.createNodesAndStartRaft(nodes, configs, numberOfReplicas, host, port);

        // Find and disconnect the current leader
        final long lastLeader = TestUtils.findLeader(nodes, 10);
        assertNotEquals(-1L, lastLeader);
        TestUtils.disconnectNodes(nodes, List.of(new PeerConfiguration(lastLeader, "", 62)));
        var updatedNodes = new ArrayList<>(nodes);
        updatedNodes.removeIf(node -> node.getNodeId() == lastLeader);

        // A new leader gets elected
        final long newLeader = TestUtils.findLeader(updatedNodes, 10);
        assertNotEquals(-1L, newLeader);
        assertNotEquals(lastLeader, newLeader);
    }

    @Test
    void disconnectLeaderAndFollower() throws IOException, InterruptedException {
        final int numberOfReplicas = 5;
        TestUtils.createNodesAndStartRaft(nodes, configs, numberOfReplicas, host, port);

        // Find and disconnect the current leader
        final long lastLeader = TestUtils.findLeader(nodes, 10);
        assertNotEquals(-1L, lastLeader);
        TestUtils.disconnectNodes(nodes, List.of(new PeerConfiguration(lastLeader, "", 62)));
        var updatedNodes = new ArrayList<>(nodes);
        updatedNodes.removeIf(node -> node.getNodeId() == lastLeader);
        // Disconnect a random follower
        long followerId = (lastLeader + 1) % numberOfReplicas;
        TestUtils.disconnectNodes(updatedNodes, List.of(new PeerConfiguration(followerId, "", 62)));
        updatedNodes.removeIf(node -> node.getNodeId() == followerId);

        // A new leader gets elected
        final long newLeader = TestUtils.findLeader(updatedNodes, 10);
        assertNotEquals(-1L, newLeader);
        assertNotEquals(lastLeader, newLeader);
    }

    @Test
    void disconnectAllAndRestore() throws IOException, InterruptedException {
        final int numberOfReplicas = 5;
        TestUtils.createNodesAndStartRaft(nodes, configs, numberOfReplicas, host, port);

        // Disconnect all nodes
        TestUtils.disconnectNodes(nodes, configs);
        Thread.sleep(500L);
        // No leader should exist
        assertEquals(-1L, TestUtils.findLeader(nodes, 10));

        // Reconnect all nodes
        TestUtils.connectNodes(nodes, configs);
        assertNotEquals(-1L, TestUtils.findLeader(nodes, 10));
    }

    @Test
    void commitOneCommandWorks() throws IOException, InterruptedException {
        final int numberOfReplicas = 5;
        TestUtils.createNodesAndStartRaft(nodes, configs, numberOfReplicas, host, port);
        Thread.sleep(300L);

        var leaderId = TestUtils.findLeader(nodes, 5);
        var command = "Hello World";
        assertTrue(TestUtils.submitCommand(nodes, leaderId, command));

        Thread.sleep(500L);
        TestUtils.checkCommitted(nodes, List.of(command));
    }

    @Test
    void commitMultipleCommandWorks() throws IOException, InterruptedException {
        final int numberOfReplicas = 5;
        TestUtils.createNodesAndStartRaft(nodes, configs, numberOfReplicas, host, port);
        Thread.sleep(300L);

        var leaderId = TestUtils.findLeader(nodes, 5);
        var command1 = "Hello ";
        assertTrue(TestUtils.submitCommand(nodes, leaderId, command1));
        var command2 = "World";
        assertTrue(TestUtils.submitCommand(nodes, leaderId, command2));
        var command3 = "!";
        assertTrue(TestUtils.submitCommand(nodes, leaderId, command3));

        Thread.sleep(500L);
        TestUtils.checkCommitted(nodes, List.of(command1, command2, command3));
    }

    @Test
    void commitToFollowerFails() throws IOException, InterruptedException {
        final int numberOfReplicas = 5;
        TestUtils.createNodesAndStartRaft(nodes, configs, numberOfReplicas, host, port);
        Thread.sleep(300L);

        var leaderId = TestUtils.findLeader(nodes, 5);
        var followerId = (leaderId + 1) % numberOfReplicas;
        var command = "Hello World";
        assertFalse(TestUtils.submitCommand(nodes, followerId, command));
    }

    @Test
    void disconnectedFollowerEventuallyReceivesCommit() throws IOException, InterruptedException {
        final int numberOfReplicas = 5;
        TestUtils.createNodesAndStartRaft(nodes, configs, numberOfReplicas, host, port);
        Thread.sleep(300L);

        var leaderId = TestUtils.findLeader(nodes, 5);

        // Disconnect follower
        var followerId = (leaderId + 1) % numberOfReplicas;
        System.out.printf(String.format("[DBG] Disconnecting Node %d%n", followerId));
        TestUtils.disconnectNodes(nodes, List.of(new PeerConfiguration(followerId, "", 62)));

        // Submit command and wait
        var command = "Hello World";
        assertTrue(TestUtils.submitCommand(nodes, leaderId, command));

        Thread.sleep(300L);

        // Reconnect follower and wait
        TestUtils.connectNodes(nodes, configs);
        Thread.sleep(1000L);

        TestUtils.checkCommitted(nodes, List.of(command));
    }

    @Test
    void commitFailsWithoutQuorum() throws IOException, InterruptedException {
        final int numberOfReplicas = 3;
        TestUtils.createNodesAndStartRaft(nodes, configs, numberOfReplicas, host, port);
        Thread.sleep(300L);

        var leaderId = TestUtils.findLeader(nodes, 5);

        // Disconnect followers
        var followerId1 = (leaderId + 1) % numberOfReplicas;
        var followerId2 = (followerId1 + 1) % numberOfReplicas;
        TestUtils.disconnectNodes(nodes,
                List.of(new PeerConfiguration(followerId1, "", 62),
                        new PeerConfiguration(followerId2, "", 62)));
        Thread.sleep(100L);

        // Submit command and wait
        var command = "Hello World";
        assertTrue(TestUtils.submitCommand(nodes, leaderId, command));

        Thread.sleep(1000L);

        TestUtils.checkUncommitted(nodes, List.of(command));
    }
}
