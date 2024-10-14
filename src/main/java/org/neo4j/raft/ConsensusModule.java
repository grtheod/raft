package org.neo4j.raft;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.eclipse.collections.api.map.primitive.MutableLongIntMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongIntHashMap;
import org.neo4j.raft.storage.PersistentStorage;
import org.neo4j.raft.utils.Timer;
import org.neo4j.raft.utils.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ConsensusModule {

    private final Lock lock;

    private final RaftNode node;
    private final Timer heartbeatTimer;
    private final List<PeerConnection> peers;
    // commitChannel is the channel where this CM is going to report committed log entries.
    // It's passed in by the client during construction.
    private final BlockingQueue<CommitEntry> commitChannel = new ArrayBlockingQueue<>(16);
    // newCommitReadyChannel is an internal notification channel used to commit new entries
    // to the log to notify that these entries may be sent on commitChannel.
    private final BlockingQueue<Object> newCommitReadyChannel = new ArrayBlockingQueue<>(16);
    // triggerMessagesChannel is used to send new log entries to replicas as they arrive
    private final BlockingQueue<Object> triggerMessagesChannel = new ArrayBlockingQueue<>(128);
    // Volatile leader state (Reinitialized after election)
    private final MutableLongIntMap nextIndex; // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
    private final MutableLongIntMap matchIndex; // for each server, index of highest log entry known to be replicated on server
    // Persistent state
    private final PersistentStorage log; // each entry contains command for state machine, and term when entry was received by leader (first index is 0)
    private ExecutorService raftNodeExecutor;
    // Volatile state
    private int commitIndex;
    private int lastAppliedIndex;
    private RaftState raftState;
    private long electionResetEvent;

    public ConsensusModule(RaftNode node, List<PeerConnection> peers, RaftState state) {
        this.lock = new ReentrantLock();
        this.node = node;
        this.commitIndex = -1;
        this.lastAppliedIndex = -1;
        this.peers = peers;
        this.raftState = state;
        this.log = new PersistentStorage(node.getNodeId());
        setCurrentTerm(0);
        setVotedFor(RaftNode.NONE);
        this.electionResetEvent = System.currentTimeMillis();
        this.heartbeatTimer = new Timer();
        this.nextIndex = new LongIntHashMap();
        this.matchIndex = new LongIntHashMap();
    }

    public void becomeFollower(int term) {
        Utils.printNodeMessage(node.getNodeId(), String.format("becomes Follower with term = %d", term));

        this.raftState = RaftState.FOLLOWER;
        setCurrentTerm(term);
        setVotedFor(RaftNode.NONE);
        restartTimer();
    }

    public void becomeLeader() {
        Utils.printNodeMessage(node.getNodeId(), String.format("becomes Leader with term = %d", getCurrentTerm()));

        this.raftState = RaftState.LEADER;
        setVotedFor(RaftNode.NONE);

        synchronized (lock) {
            Utils.printNodeMessage(node.getNodeId(),
                    String.format("Leader initializing nextIndex for all nodes as %d", getLogSize()));
            for (var peer : peers) {
                nextIndex.put(peer.nodeId(), getLogSize());
                matchIndex.put(peer.nodeId(), -1);
            }
        }

        // Send periodic heartbeats every 50ms
        raftNodeExecutor.submit(() -> {
            try {
                // Send immediatelly heartbeats
                sendHeartbeatsOrMessagesAsynchronously();
                final long waitTime = 50L;
                long remainingTime = waitTime;
                while (true) {
                    triggerMessagesChannel.poll(remainingTime, TimeUnit.MILLISECONDS);
                    synchronized (lock) {
                        if (raftState != RaftState.LEADER) {
                            return;
                        }
                    }
                    long elapsedTime = sendHeartbeatsOrMessagesAsynchronously();
                    remainingTime = elapsedTime < waitTime ? waitTime - elapsedTime : waitTime;
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void restartTimer() {
        this.electionResetEvent = System.currentTimeMillis();
        raftNodeExecutor.submit(new ConsensusTimer(this));
    }

    private long sendHeartbeatsOrMessagesAsynchronously() {
        heartbeatTimer.start();

        int savedCurrentTerm;
        synchronized (lock) {
            savedCurrentTerm = getCurrentTerm();
        }

        List<Triple<Long, Integer, ListenableFuture<AppendEntriesResponse>>> futures = new ArrayList<>();
        synchronized (lock) {
            // Have to protect the peers from disconnections
            for (PeerConnection peer : peers) {

                var next = nextIndex.get(peer.nodeId());
                var prevLogIndex = next - 1;
                var prevLogTerm = -1;
                if (prevLogIndex >= 0) {
                    prevLogTerm = getLogEntry(prevLogIndex).getTerm();
                }

                var requestBuilder = AppendEntriesRequest.newBuilder()
                        .setTerm(savedCurrentTerm)
                        .setLeaderId(node.getNodeId())
                        .setPrevLogIndex(prevLogIndex)
                        .setPrevLogTerm(prevLogTerm)
                        .setLeaderCommit(commitIndex);
                // todo: parallelize this step
                var rIdx = 0;
                for (int i = next; i < getLogSize(); ++i) {
                    requestBuilder.addEntries(getLogEntry(i));
                    rIdx++;
                }
                var request = requestBuilder.build();

                Utils.printNodeMessage(node.getNodeId(),
                        String.format("sending appendEntries to node %d with term = %d [%s]",
                                peer.nodeId(), savedCurrentTerm, Utils.appendEntriesRequestToString(request)));

                futures.add(new ImmutableTriple<>(peer.nodeId(), rIdx, peer.asyncStub().appendEntries(request)));
            }
        }

        for (var future : futures) {
            try {
                var response = future.getRight().get(10, TimeUnit.MILLISECONDS); // 10ms timeout
                var peerId = future.getLeft();
                var entriesLength = future.getMiddle();
                synchronized (lock) {
                    if (response.getTerm() > savedCurrentTerm) {
                        Utils.printNodeMessage(node.getNodeId(), "term out of date in heartbeat reply");
                        becomeFollower(response.getTerm());
                        return 0;
                    }


                    if (raftState == RaftState.LEADER && savedCurrentTerm == response.getTerm()) {
                        var next = nextIndex.get(peerId);
                        if (response.getSuccess()) {
                            nextIndex.addToValue(peerId, entriesLength);
                            matchIndex.put(peerId, nextIndex.get(peerId) - 1);

                            Utils.printNodeMessage(node.getNodeId(), String.format(
                                    "appendEntries reply from Node %d success: nextIndex := %d, matchIndex := %d", peerId, nextIndex.get(peerId), matchIndex.get(peerId)));

                            var savedCommitIndex = commitIndex;
                            for (int i = commitIndex + 1; i < getLogSize(); ++i) {
                                if (getLogEntry(i).getTerm() == getCurrentTerm()) {
                                    var matchCount = 1;
                                    for (var peer : peers) {
                                        if (matchIndex.get(peer.nodeId()) >= i) {
                                            matchCount++;
                                        }
                                    }
                                    if (matchCount * 2 > peers.size() + 1) {
                                        commitIndex = i;
                                    }
                                }
                            }
                            if (commitIndex != savedCommitIndex) {
                                Utils.printNodeMessage(node.getNodeId(),
                                        String.format("leader sets commitIndex := %d", commitIndex));
                                newCommitReadyChannel.add(new Object());
                                triggerMessagesChannel.add(new Object());
                            }
                        } else {
                            if (response.getConflictTerm() >= 0) {
                                var lastIndexOfTerm = -1;
                                for (int i = log.size() - 1; i >= 0; --i) {
                                    if (log.get(i).getTerm() == response.getConflictTerm()) {
                                        lastIndexOfTerm = i;
                                        break;
                                    }
                                }
                                if (lastIndexOfTerm >= 0) {
                                    nextIndex.put(peerId, lastIndexOfTerm + 1);
                                } else {
                                    nextIndex.put(peerId, response.getConflictIndex());
                                }
                            } else {
                                nextIndex.put(peerId, response.getConflictIndex());
                            }
                            //nextIndex.put(peerId, next - 1);
                            Utils.printNodeMessage(node.getNodeId(),
                                    String.format("appendEntries reply from Node %d !success: nextIndex := %d", peerId, next - 1));
                        }
                    }
                }
            } catch (Exception e) {
                Utils.printNodeMessage(node.getNodeId(), "heartbeat rcp failed to return");
            }
        }

        return heartbeatTimer.stop();
    }

    public void stop() {
        log.stop();
    }

    // Expects lock to be locked
    public Pair<Integer, Integer> getLastLogIndexAndTerm() {
        int lastIndex = (log.isEmpty()) ? -1 : getLogSize() - 1;
        int term = (lastIndex == -1) ? -1 : getLogEntry(lastIndex).getTerm();
        return Pair.of(lastIndex, term);
    }

    public Lock getLock() {
        return lock;
    }

    public RaftNode getNode() {
        return node;
    }

    public ExecutorService getRaftNodeExecutor() {
        return raftNodeExecutor;
    }

    public void setRaftNodeExecutor(ExecutorService raftNodeExecutor) {
        this.raftNodeExecutor = raftNodeExecutor;
    }

    public BlockingQueue<CommitEntry> getCommitChannel() {
        return commitChannel;
    }

    public BlockingQueue<Object> getNewCommitReadyChannel() {
        return newCommitReadyChannel;
    }

    public BlockingQueue<Object> getTriggerMessagesChannel() {
        return triggerMessagesChannel;
    }

    public int getCurrentTerm() {
        return log.getCurrentTerm();
    }

    public void setCurrentTerm(int currentTerm) {
        log.setCurrentTerm(currentTerm);
    }

    public long getVotedFor() {
        return log.getVotedFor();
    }

    public void setVotedFor(long votedFor) {
        log.setVotedFor(votedFor);
    }

    public PersistentStorage getLog() {
        return log;
    }

    public int getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(int commitIndex) {
        this.commitIndex = commitIndex;
    }

    public int getLastAppliedIndex() {
        return lastAppliedIndex;
    }

    public void setLastAppliedIndex(int lastAppliedIndex) {
        this.lastAppliedIndex = lastAppliedIndex;
    }

    public List<PeerConnection> getPeers() {
        return peers;
    }

    public RaftState getRaftState() {
        return raftState;
    }

    public void setRaftState(RaftState raftState) {
        this.raftState = raftState;
    }

    public long getElectionResetEvent() {
        return electionResetEvent;
    }

    public void setElectionResetEvent(long electionResetEvent) {
        this.electionResetEvent = electionResetEvent;
    }

    private LogEntry getLogEntry(int index) {
        return log.get(index);
    }

    private int getLogSize() {
        return log.size();
    }

    public String getStateAsString() {
        return "currentTerm = " +
                getCurrentTerm() +
                ", " +
                "commitIndex = " +
                commitIndex +
                ", " +
                "lastAppliedIndex = " +
                lastAppliedIndex +
                ", " +
                "log size = " +
                getLogSize();
    }
}
