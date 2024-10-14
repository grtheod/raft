package org.neo4j.raft.storage;

import org.neo4j.raft.LogEntry;
import org.neo4j.raft.RaftNode;
import org.neo4j.raft.utils.RaftConfiguration;
import org.neo4j.raft.utils.Utils;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PersistentStorage {

    // Cache the log, currentTerm, and votedFor to simplify the implementation
    private final List<LogEntry> log;
    // Persistent data
    private String path = null;
    private RocksDBInstance db = null;
    private int currentTerm;
    private long votedFor;

    public PersistentStorage(long nodeId) {
        this.log = new ArrayList<>();
        this.currentTerm = 0;
        this.votedFor = RaftNode.NONE;

        // Populate the log based on persisted data
        if (RaftConfiguration.USE_DISK) {
            path = System.getProperty("user.dir") + "/target/rocksdb-" + nodeId;
            try {
                db = new RocksDBInstance(path);
                this.currentTerm = db.getCurrentTerm();
                this.votedFor = db.getVotedFor();
                this.log = db.getAllLogEntries();
            } catch (RocksDBException e) {
                System.err.println("Error while starting RocksDB: " + e.getMessage());
            }
        }
    }

    public boolean isEmpty() {
        return log.isEmpty();
    }

    public int size() {
        return log.size();
    }

    public LogEntry get(int index) {
        return log.get(index);
    }

    public void add(LogEntry logEntry) {
        log.add(logEntry);
        if (db != null) {
            db.append(logEntry, true);
        }
    }

    public void addAll(List<LogEntry> logEntries) {
        for (var entry : logEntries) {
            add(entry);
        }
    }

    public List<LogEntry> subList(int fromIndex, int toIndex) {
        return log.subList(fromIndex, toIndex);
    }

    public void truncate(int fromIndex, int toIndex) {
        log.subList(fromIndex, toIndex).clear();
        if (db != null) {
            db.prune(fromIndex, toIndex);
        }
    }

    public int getCurrentTerm() {
        return currentTerm;
    }

    public void setCurrentTerm(int currentTerm) {
        this.currentTerm = currentTerm;
        if (db != null) {
            db.setCurrentTerm(currentTerm, true);
        }
    }

    public long getVotedFor() {
        return votedFor;
    }

    public void setVotedFor(long votedFor) {
        this.votedFor = votedFor;
        if (db != null) {
            db.setVotedFor(votedFor, true);
        }
    }

    public void stop() {
        log.clear();
        if (db != null) {
            db.close();
            try {
                Utils.deleteFolder(path);
            } catch (IOException e) {
                System.err.println("[Error] while deleting RocksDB folder: " + e.getMessage());
            }
        }
    }
}
