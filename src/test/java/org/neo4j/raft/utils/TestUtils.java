package org.neo4j.raft.utils;

import org.neo4j.raft.CommitEntry;
import org.neo4j.raft.PeerConfiguration;
import org.neo4j.raft.RaftNode;
import org.neo4j.raft.RaftState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestUtils {

    private TestUtils() {
    }

    public static void createNodes(List<RaftNode> nodes, List<PeerConfiguration> configs, int numberOfNodes, String host, int port) throws IOException {
        for (int id = 0; id < numberOfNodes; id++) {
            var node = new RaftNode(new PeerConfiguration(id, host, port + id));
            node.start();

            configs.add(node.getConfig());
            nodes.add(node);
        }
    }

    public static void connectNodes(List<RaftNode> nodes, List<PeerConfiguration> configs) {
        for (var n : nodes) {
            n.connectToPeers(configs);
        }
    }

    public static void disconnectNodes(List<RaftNode> nodes, List<PeerConfiguration> configs) {
        for (var n : nodes) {
            n.disconnectFromPeers(configs);
        }
    }

    public static void startRaft(List<RaftNode> nodes) {
        for (var n : nodes) {
            n.startConsensusTimerAndCommitSender();
        }
    }

    public static void shutdownNodes(List<RaftNode> nodes) throws InterruptedException {
        for (var n : nodes) {
            n.stop();
        }
    }

    public static void createAndConnectNodes(List<RaftNode> nodes, List<PeerConfiguration> configs, int numberOfNodes, String host, int port) throws IOException {
        createNodes(nodes, configs, numberOfNodes, host, port);
        connectNodes(nodes, configs);
    }

    public static void createNodesAndStartRaft(List<RaftNode> nodes, List<PeerConfiguration> configs, int numberOfNodes, String host, int port) throws IOException {
        createNodes(nodes, configs, numberOfNodes, host, port);
        connectNodes(nodes, configs);
        startRaft(nodes);
    }

    public static long findLeader(List<RaftNode> nodes, int retries) throws InterruptedException {
        final long noLeader = -1L;
        final Map<Integer, Long> leaderForTerm = new HashMap<>();

        long lastLeader = noLeader;
        // Retry multiple times even whne the leader is found, to assert that there is a single leader
        for (int r = 0; r < retries; r++) {
            for (var n : nodes) {
                var state = n.getState();
                if (state.getRight() == RaftState.LEADER) {
                    var leaderId = leaderForTerm.getOrDefault(state.getLeft(), -1L);
                    if (leaderId != -1L && leaderId != n.getNodeId()) {
                        System.out.printf(String.format("[Warning] Found two leaders in term %d!%n", state.getLeft()));
                        lastLeader = noLeader;
                    } else {
                        leaderForTerm.put(state.getLeft(), n.getNodeId());
                        lastLeader = leaderId;
                    }
                }
            }
            Thread.sleep(150);
        }

        if (!leaderForTerm.isEmpty() && lastLeader != noLeader) {
            return lastLeader;
        } else {
            System.out.println("[Warning] No leader found!");
            return noLeader;
        }
    }

    public static boolean submitCommand(List<RaftNode> nodes, long leaderId, String command) {
        for (var n : nodes) {
            if (n.getNodeId() == leaderId) {
                return n.submitCommand(command);
            }
        }
        System.out.println("[Warning] No leader found!");
        return false;
    }

    public static void checkCommitted(List<RaftNode> nodes, List<String> commands) {
        // All nodes must have the same commit length and entries
        Set<String> set = new HashSet<>(commands);
        List<CommitEntry> commitEntries = new ArrayList<>();
        for (int i = 0; i < nodes.size(); ++i) {
            var node = nodes.get(i);
            var module = node.getConsensusModule();
            if (i == 0) {
                synchronized (module.getLock()) {
                    var channel = module.getCommitChannel();
                    while (!channel.isEmpty()) {
                        var entry = channel.poll();
                        commitEntries.add(entry);
                        set.remove(entry.command());
                    }
                    assertTrue(set.isEmpty());
                }
            } else {
                synchronized (module.getLock()) {
                    int cIdx = 0;
                    var channel = module.getCommitChannel();
                    while (!channel.isEmpty()) {
                        var entry = channel.poll();
                        assertEquals(entry.command(), commitEntries.get(cIdx).command());
                        assertEquals(entry.index(), commitEntries.get(cIdx).index());
                        cIdx++;
                    }
                    assertEquals(commitEntries.size(), cIdx);
                }
            }
        }
    }

    public static void checkUncommitted(List<RaftNode> nodes, List<String> commands) {
        // All nodes must have the same commit length and entries
        Set<String> set = new HashSet<>(commands);
        List<CommitEntry> commitEntries = new ArrayList<>();
        for (RaftNode node : nodes) {
            var module = node.getConsensusModule();
            synchronized (module.getLock()) {
                var channel = module.getCommitChannel();
                while (!channel.isEmpty()) {
                    var entry = channel.poll();
                    assertFalse(set.contains(entry.command()));
                }
            }
        }
    }
}
