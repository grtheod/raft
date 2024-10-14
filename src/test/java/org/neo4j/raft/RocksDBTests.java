package org.neo4j.raft;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.raft.storage.RocksDBInstance;
import org.neo4j.raft.utils.Utils;
import org.rocksdb.RocksDBException;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RocksDBTests {
    private final String path = System.getProperty("user.dir") + "/target/rocksdb";
    private RocksDBInstance db;

    @BeforeEach
    void setUp() throws RocksDBException {
        db = new RocksDBInstance(path);
    }

    @AfterEach
    void tearDown() throws IOException {
        db.close();
        Utils.deleteFolder(path);
    }

    @Test
    void appendToLog() {
        LogEntry hello = LogEntry.newBuilder().setTerm(0).setCommand("Hello").build();
        db.append(hello);
        LogEntry world = LogEntry.newBuilder().setTerm(0).setCommand(" World").build();
        db.append(world);

        assertEquals(hello, db.getLogEntry(0));
        assertEquals(world, db.getLogEntry(1));
    }

    @Test
    void appendToLogAndSync() {
        var hello = LogEntry.newBuilder().setTerm(0).setCommand("Hello").build();
        db.append(hello, true);
        var world = LogEntry.newBuilder().setTerm(0).setCommand(" World").build();
        db.append(world, true);

        assertEquals(hello, db.getLogEntry(0));
        assertEquals(world, db.getLogEntry(1));
    }

    @Test
    void updateCurrentTerm() {
        assertEquals(0, db.getCurrentTerm());

        var newTerm = 42;
        db.setCurrentTerm(newTerm);
        assertEquals(newTerm, db.getCurrentTerm());
    }

    @Test
    void updateVotedFor() {
        assertEquals(-1L, db.getVotedFor());

        var votedFor = 4242L;
        db.setVotedFor(votedFor);
        assertEquals(votedFor, db.getVotedFor());
    }

    @Test
    void restartBeginsFromCorrectIndex() throws RocksDBException {
        // Update the database
        var hello = LogEntry.newBuilder().setTerm(0).setCommand("Hello").build();
        db.append(hello);
        var world = LogEntry.newBuilder().setTerm(0).setCommand(" World").build();
        db.append(world);

        var newTerm = 42;
        db.setCurrentTerm(newTerm);

        var votedFor = 4242L;
        db.setVotedFor(votedFor);

        // Flush data to disk and close the instance
        db.flush();
        db.close();

        // Restart db and append a new command
        db = new RocksDBInstance(path);
        var newCommand = LogEntry.newBuilder().setTerm(0).setCommand("!").build();
        db.append(newCommand);

        // Test contents after restart
        assertEquals(hello, db.getLogEntry(0));
        assertEquals(world, db.getLogEntry(1));
        assertEquals(newCommand, db.getLogEntry(2));
        assertEquals(newTerm, db.getCurrentTerm());
        assertEquals(votedFor, db.getVotedFor());
    }
}
