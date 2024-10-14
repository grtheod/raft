package org.neo4j.raft.storage;

import org.neo4j.raft.LogEntry;
import org.neo4j.raft.RaftNode;
import org.neo4j.raft.utils.Utils;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;

import java.util.ArrayList;
import java.util.List;

public class RocksDBInstance {
    private static final String CURRENT_TERM_KEY = "currentTerm";
    private static final String VOTED_FOR_KEY = "votedFor";

    private final RocksDB db;
    private final WriteOptions writeOptions;
    private final Options options;
    private long logIndex;
    private long numberOfEntries;

    public RocksDBInstance(String dbPath) throws RocksDBException {
        RocksDB.loadLibrary();

        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
        tableConfig.setBlockCache(new LRUCache(128 * 1024 * 1024L));
        tableConfig.setFilterPolicy(new BloomFilter(10)); // 10 bits per key

        options = new Options().setCreateIfMissing(true);
        options.setCompressionType(CompressionType.SNAPPY_COMPRESSION);
        options.setWriteBufferSize(64 * 1024 * 1024L);  // defines the size of a memtable
        options.setNumLevels(5); // Fewer levels reduce the time it takes to search across levels but can increase space amplification.
        options.setMaxBytesForLevelBase(64 * 1024 * 1024L);
        options.setCompactionStyle(CompactionStyle.LEVEL); // level compaction typically is more space-efficient
        options.setTableFormatConfig(tableConfig);
        // options.setIncreaseParallelism(Runtime.getRuntime().availableProcessors());

        db = RocksDB.open(options, dbPath);
        writeOptions = new WriteOptions().setSync(true);  // Enable synchronous writes
        logIndex = getMaxLogIndex();
        if (logIndex == 0) {
            setCurrentTerm(0);
            setVotedFor(RaftNode.NONE);
        }
        numberOfEntries = getNumberOfEntries();
    }

    public void append(LogEntry value) {
        append(value, false);
    }

    public void append(LogEntry value, boolean sync) {
        try {
            String key = String.valueOf(logIndex);
            if (sync) {
                db.put(writeOptions, key.getBytes(), Utils.logEntryToBytes(value));
            } else {
                db.put(key.getBytes(), Utils.logEntryToBytes(value));
            }

            logIndex++; // Increment log index for the next entry
            numberOfEntries++;
        } catch (RocksDBException e) {
            System.err.println("[Error] setting key-value pair: " + e.getMessage());
        }
    }

    public LogEntry getLogEntry(int index) {
        try {
            String key = String.valueOf(index);
            byte[] value = db.get(key.getBytes());
            return (value != null) ? Utils.logEntryFromBytes(value) : null;
        } catch (RocksDBException e) {
            System.err.println("[Error] getting value for key: " + e.getMessage());
            return null;
        }
    }

    public void prune(int fromIndex, int toIndex) {
        try {
            for (var i = fromIndex; i < toIndex; i++) {
                String key = String.valueOf(i);
                db.delete(key.getBytes());
                numberOfEntries--;
            }
            System.out.printf(String.format("[DBG] Pruned entries [%d, %d)%n", fromIndex, toIndex));
        } catch (RocksDBException e) {
            System.err.println("[Error] while pruning data: " + e.getMessage());
        }
    }

    public void flush() {
        try (var flushOptions = new org.rocksdb.FlushOptions()) {
            db.flush(flushOptions.setWaitForFlush(true));  // Wait until flush completes
        } catch (RocksDBException e) {
            System.err.println("[Error] flushing data " + e.getMessage());
        }
    }

    private long getMaxLogIndex() {
        try (RocksIterator iterator = db.newIterator()) {
            iterator.seekToLast();
            while (iterator.isValid()) {
                String key = new String(iterator.key());
                try {
                    return Long.parseLong(key) + 1; // Try parsing the key as a long
                } catch (NumberFormatException e) {
                    // If it's not a numeric key, move to the previous key
                    iterator.prev();
                }
            }
        }
        return 0;
    }

    public void setCurrentTerm(int term, boolean sync) {
        try {
            if (sync) {
                db.put(writeOptions, CURRENT_TERM_KEY.getBytes(), String.valueOf(term).getBytes());
            } else {
                db.put(CURRENT_TERM_KEY.getBytes(), String.valueOf(term).getBytes());
            }
        } catch (RocksDBException e) {
            System.err.println("[Error] setting currentTerm: " + e.getMessage());
        }
    }

    public int getCurrentTerm() {
        try {
            byte[] value = db.get(CURRENT_TERM_KEY.getBytes());
            return value != null ? Integer.parseInt(new String(value)) : 0;
        } catch (RocksDBException e) {
            System.err.println("[Error] getting currentTerm: " + e.getMessage());
        }
        return 0;
    }

    public void setCurrentTerm(int term) {
        setCurrentTerm(term, false);
    }

    public void setVotedFor(long votedFor, boolean sync) {
        try {
            if (sync) {
                db.put(writeOptions, VOTED_FOR_KEY.getBytes(), String.valueOf(votedFor).getBytes());
            } else {
                db.put(VOTED_FOR_KEY.getBytes(), String.valueOf(votedFor).getBytes());
            }
        } catch (RocksDBException e) {
            System.err.println("[Error] setting votedFor: " + e.getMessage());
        }
    }

    public long getVotedFor() {
        try {
            byte[] value = db.get(VOTED_FOR_KEY.getBytes());
            return value != null ? Long.parseLong(new String(value)) : RaftNode.NONE;
        } catch (RocksDBException e) {
            System.err.println("[Error] getting votedFor: " + e.getMessage());
        }
        return RaftNode.NONE;
    }

    public void setVotedFor(long votedFor) {
        setVotedFor(votedFor, false);
    }

    public void close() {
        if (db != null) {
            db.close();
        }
        if (writeOptions != null) {
            writeOptions.close();
        }
        if (options != null) {
            options.close();
        }
    }

    public int size() {
        return (int) numberOfEntries;
    }

    public boolean isEmpty() {
        return numberOfEntries == 0;
    }

    private long getNumberOfEntries() {
        long count = 0;
        try (RocksIterator iterator = db.newIterator()) {
            iterator.seekToFirst();  // Start from the first element
            while (iterator.isValid()) {
                count++;  // Increment count for each valid entry
                iterator.next();  // Move to the next key
            }
        }
        return count;
    }

    public List<LogEntry> getAllLogEntries() {
        List<LogEntry> logEntries = new ArrayList<>();
        try (RocksIterator iterator = db.newIterator()) {
            iterator.seekToFirst();
            while (iterator.isValid()) {
                String key = new String(iterator.key());

                // Skip the special keys for currentTerm and votedFor
                if (!CURRENT_TERM_KEY.equals(key) && !VOTED_FOR_KEY.equals(key)) {
                    byte[] value = iterator.value();
                    LogEntry entry = Utils.logEntryFromBytes(value);
                    logEntries.add(entry);
                }

                iterator.next();
            }
        } catch (Exception e) {
            System.err.println("[Error] retrieving all log entries: " + e.getMessage());
        }

        return logEntries;
    }
}
