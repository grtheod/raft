package org.neo4j.raft;

public record PeerConfiguration(long nodeId, String host, int port) {
}
