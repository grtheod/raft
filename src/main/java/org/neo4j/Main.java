package org.neo4j;

import org.neo4j.raft.PeerConfiguration;
import org.neo4j.raft.RaftNode;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws InterruptedException, IOException {
        var node = new RaftNode(new PeerConfiguration(0, "localhost", 9090));
        node.start();

        System.out.println("Press Enter to stop...");
        try {
            System.in.read();  // Wait for the user to press Enter
        } catch (Exception e) {
            e.printStackTrace();
        }

        node.stop();
    }
}