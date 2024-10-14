package org.neo4j.raft;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.raft.utils.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class ConsensusTimer implements Runnable {

    private static final int LOWER_BOUND = 150;
    private static final int UPPER_BOUND = 300;
    private final ConsensusModule module;
    private final long timeoutDuration;
    private final ExecutorService raftNodeExecutor;

    public ConsensusTimer(ConsensusModule module) {
        this.module = module;
        this.timeoutDuration = ThreadLocalRandom.current().nextInt(LOWER_BOUND, UPPER_BOUND + 1);
        this.raftNodeExecutor = module.getRaftNodeExecutor();
    }

    @Override
    public void run() {
        int termStarted;
        synchronized (module.getLock()) {
            termStarted = module.getCurrentTerm();
            Utils.printNodeMessage(module.getNode().getNodeId(),
                    String.format("election timer started (%d), term = %d", timeoutDuration, termStarted));
        }

        try {
            while (true) {
                synchronized (module.getLock()) {
                    if (module.getRaftState() != RaftState.CANDIDATE && module.getRaftState() != RaftState.FOLLOWER) {
                        Utils.printNodeMessage(module.getNode().getNodeId(), String.format("in election timer state = %s, bailing out", module.getRaftState()));
                        return; // Exit if not in a valid state
                    }

                    // Check if the term has changed
                    if (termStarted != module.getCurrentTerm()) {
                        Utils.printNodeMessage(module.getNode().getNodeId(),
                                (String.format("in election timer term changed from %d to %d, bailing out", termStarted, module.getCurrentTerm())));
                        return; // Exit if term has changed
                    }

                    // Check if the timeout has elapsed
                    long elapsed = System.currentTimeMillis() - module.getElectionResetEvent();
                    if (elapsed >= timeoutDuration) {
                        startElection();
                        return; // Start election if timeout is reached
                    }
                }

                // Sleep for 10 ms
                Thread.sleep(10L);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void startElection() {
        module.setRaftState(RaftState.CANDIDATE);
        module.setCurrentTerm(module.getCurrentTerm() + 1);
        var savedCurrentTerm = module.getCurrentTerm();
        module.setElectionResetEvent(System.currentTimeMillis());
        module.setVotedFor(module.getNode().getNodeId());

        Utils.printNodeMessage(module.getNode().getNodeId(), (String.format("becomes Candidate (currentTerm=%d)", module.getCurrentTerm())));

        asynchronousVoting(savedCurrentTerm);
    }

    private void asynchronousVoting(int savedCurrentTerm) {
        // Send RequestVote RPCs to all other servers concurrently.
        var lastIndexAndTerm = module.getLastLogIndexAndTerm();
        List<Pair<Long, ListenableFuture<RequestVoteResponse>>> futures = new ArrayList<>();
        RequestVoteRequest request = RequestVoteRequest.newBuilder()
                .setTerm(savedCurrentTerm)
                .setCandidateId(module.getVotedFor())
                .setLastLogIndex(lastIndexAndTerm.getLeft())
                .setLastLogTerm(lastIndexAndTerm.getRight())
                .build();
        for (PeerConnection peer : module.getPeers()) {
            Utils.printNodeMessage(module.getNode().getNodeId(),
                    String.format("sending requestVote to %d: %s", peer.nodeId(), Utils.requestVoteRequestToString(request)));
            futures.add(new ImmutablePair<>(peer.nodeId(), peer.asyncStub().requestVote(request)));
        }

        var votesReceived = 1;
        for (var future : futures) {
            try {
                var response = future.getRight().get(100, TimeUnit.MILLISECONDS); // 100ms timeout
                // Process the response if available
                Utils.printNodeMessage(module.getNode().getNodeId(),
                        String.format("received from %d vote response %s", future.getLeft(), Utils.requestVoteResponseToString(response)));
                synchronized (module.getLock()) {
                    if (module.getRaftState() != RaftState.CANDIDATE) {
                        Utils.printNodeMessage(module.getNode().getNodeId(), String.format("while waiting for reply, state = %s", module.getRaftState()));
                        break;
                    }

                    if (response.getTerm() > savedCurrentTerm) {
                        Utils.printNodeMessage(module.getNode().getNodeId(), "term out of date in RequestVoteReply");
                        module.becomeFollower(response.getTerm());
                        return;
                    } else if (response.getTerm() == savedCurrentTerm
                            && response.getVoteGranted()) {
                        votesReceived++;
                        if (votesReceived * 2 > module.getPeers().size() + 1) {
                            Utils.printNodeMessage(module.getNode().getNodeId(), String.format("wins election with %d votes", votesReceived));
                            module.becomeLeader();
                            return;
                        }
                    }
                }
            } catch (Exception e) {
                Utils.printNodeMessage(module.getNode().getNodeId(), "vote rcp failed to return");
            }
        }

        raftNodeExecutor.submit(new ConsensusTimer(module));
    }
}
