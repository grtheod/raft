package org.neo4j.raft.utils;

import org.neo4j.raft.AppendEntriesRequest;
import org.neo4j.raft.AppendEntriesResponse;
import org.neo4j.raft.LogEntry;
import org.neo4j.raft.RequestVoteRequest;
import org.neo4j.raft.RequestVoteResponse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

public class Utils {
    public static final boolean PRINT_MESSAGES = true;

    private Utils() {
        throw new IllegalStateException("Utility class");
    }

    public static void printNodeMessage(long nodeId, String message) {
        if (PRINT_MESSAGES) {
            System.out.printf(String.format("[DBG] Node %d: %s%n", nodeId, message));
        }
    }

    public static String appendEntriesRequestToString(AppendEntriesRequest request) {
        StringBuilder builder = new StringBuilder();

        if (request.getEntriesCount() > 0) {
            for (var entry : request.getEntriesList()) {
                builder.append(entry.getCommand());
                builder.append(" ");
            }
        } else {
            builder.append("heartbeat");
        }

        return "[" +
                request.getTerm() +
                ", " +
                request.getLeaderId() +
                ", " +
                request.getPrevLogIndex() +
                ", " +
                request.getPrevLogTerm() +
                ", " +
                builder +
                ", " +
                request.getLeaderCommit() +
                "]";
    }

    public static String appendEntriesResponseToString(AppendEntriesResponse response) {
        return "[" +
                response.getTerm() +
                ", " +
                response.getSuccess() +
                "]";
    }

    public static String requestVoteRequestToString(RequestVoteRequest request) {
        return "[" +
                request.getTerm() +
                ", " +
                request.getCandidateId() +
                ", " +
                request.getLastLogTerm() +
                ", " +
                request.getLastLogIndex() +
                "]";
    }

    public static String requestVoteResponseToString(RequestVoteResponse response) {
        return "[" +
                response.getTerm() +
                ", " +
                response.getVoteGranted() +
                "]";
    }

    public static void deleteFolder(String pathString) throws IOException {
        Path path = Paths.get(pathString);

        Files.walkFileTree(path, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    public static byte[] logEntryToBytes(LogEntry entry) {
        // Get byte representation of command
        byte[] commandBytes = entry.getCommand().getBytes();
        int commandLength = commandBytes.length;

        // Create a buffer with space for term (4 bytes) + command bytes
        ByteBuffer buffer = ByteBuffer.allocate(4 + commandLength);
        buffer.putInt(entry.getTerm()); // Store term as an integer (4 bytes)
        buffer.put(commandBytes); // Store command bytes

        return buffer.array(); // Return the byte array
    }

    public static LogEntry logEntryFromBytes(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        int term = buffer.getInt(); // Read the term (4 bytes)
        int commandLength = buffer.remaining(); // Read the length of the command

        byte[] commandBytes = new byte[commandLength]; // Create a byte array for the command
        buffer.get(commandBytes); // Read the command bytes

        String command = new String(commandBytes); // Convert bytes to String

        return LogEntry.newBuilder().setTerm(term).setCommand(command).build();
    }
}
