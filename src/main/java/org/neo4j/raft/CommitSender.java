package org.neo4j.raft;

import org.neo4j.raft.utils.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class CommitSender implements Runnable {
    private final ConsensusModule module;
    private final AtomicBoolean isRunning;

    public CommitSender(ConsensusModule module) {
        this.module = module;
        this.isRunning = new AtomicBoolean(true);
    }

    @Override
    public void run() {
        var channel = module.getNewCommitReadyChannel();
        while (isRunning.get()) {
            if (channel.isEmpty()) {
                Thread.yield();
                continue;
            }

            while (!channel.isEmpty()) {
                channel.poll();
                int savedTerm;
                int savedLastApplied;
                List<LogEntry> entries = new ArrayList<>();
                synchronized (module.getLock()) {
                    savedTerm = module.getCurrentTerm();
                    savedLastApplied = module.getLastAppliedIndex();
                    if (module.getCommitIndex() > module.getLastAppliedIndex()) {
                        entries = module.getLog().subList(module.getLastAppliedIndex() + 1, module.getCommitIndex() + 1);
                        module.setLastAppliedIndex(module.getCommitIndex());
                    }
                }
                Utils.printNodeMessage(module.getNode().getNodeId(),
                        String.format("commitChannelSender entries=%s, savedLastApplied=%d", entries, savedLastApplied));

                for (int i = 0; i < entries.size(); i++) {
                    LogEntry entry = entries.get(i);
                    module.getCommitChannel().add(new CommitEntry(entry.getCommand(), savedLastApplied + i + 1, savedTerm));
                }
            }
            Utils.printNodeMessage(module.getNode().getNodeId(), "commitChannelSender done");
        }
    }

    public void stop() {
        isRunning.set(false);
    }
}
