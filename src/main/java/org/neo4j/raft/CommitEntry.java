package org.neo4j.raft;

public record CommitEntry(String command, int index, int term) {
}
