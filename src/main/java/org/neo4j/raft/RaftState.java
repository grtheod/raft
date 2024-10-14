package org.neo4j.raft;

public enum RaftState {
    FOLLOWER,
    CANDIDATE,
    LEADER,
    DEAD
}
