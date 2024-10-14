package org.neo4j.raft.utils;

public class Timer {
    private long startTime;
    private boolean running;

    // Starts the timer
    public void start() {
        this.startTime = System.currentTimeMillis();
        this.running = true;
    }

    // Stops the timer and returns the elapsed time in milliseconds
    public long stop() {
        if (running) {
            long endTime = System.currentTimeMillis();
            this.running = false;
            return endTime - startTime;
        } else {
            throw new IllegalStateException("Timer is not running. Please start the timer first.");
        }
    }
}
