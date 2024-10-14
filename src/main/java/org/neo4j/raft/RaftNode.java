package org.neo4j.raft;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.raft.utils.RaftConfiguration;
import org.neo4j.raft.utils.Utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class RaftNode extends RaftServiceGrpc.RaftServiceImplBase {
    public static final long NONE = -1L;
    private static final PeerConnection DEFAULT_CONNECTION = new PeerConnection(-1L, null, null, null);
    private final long nodeId;
    private final PeerConfiguration config;
    private final ConsensusModule module;
    private final ExecutorService executor;
    private Server server;
    private boolean consensusTimerhasStarted;
    private boolean commitSenderhasStarted;
    private CommitSender commitSender;

    public RaftNode(PeerConfiguration config) {
        this.nodeId = config.nodeId();
        this.config = config;
        this.executor = Executors.newFixedThreadPool(RaftConfiguration.POOL_SIZE);
        this.module = new ConsensusModule(this, new ArrayList<>(), RaftState.FOLLOWER);
        this.consensusTimerhasStarted = false;
        this.commitSenderhasStarted = false;
    }

    @Override
    public void requestVote(RequestVoteRequest request, StreamObserver<RequestVoteResponse> responseObserver) {
        synchronized (module.getLock()) {
            if (module.getRaftState() == RaftState.DEAD) {
                return;
            }

            var lastIndexAndTerm = module.getLastLogIndexAndTerm();
            int lastLogIndex = lastIndexAndTerm.getLeft();
            int lastLogTerm = lastIndexAndTerm.getRight();
            Utils.printNodeMessage(nodeId, String.format("requestVote %s [currentTerm = %d, votedFor = %d, log index = %d, log term = %d]",
                    Utils.requestVoteRequestToString(request), module.getCurrentTerm(), module.getVotedFor(), lastLogIndex, lastLogTerm));
            if (request.getTerm() > module.getCurrentTerm()) {
                Utils.printNodeMessage(nodeId, "... term out of date in requestVote");
                module.becomeFollower(request.getTerm());
            }

            RequestVoteResponse.Builder response = RequestVoteResponse.newBuilder();
            // If votedFor is null or candidateId, and candidate’s log is
            // at least as up-to-date as receiver’s log, grant vote
            if (request.getTerm() == module.getCurrentTerm()
                    && (module.getVotedFor() == NONE || module.getVotedFor() == request.getCandidateId())
                    && (request.getLastLogTerm() >= lastLogTerm && request.getLastLogIndex() >= lastLogIndex)
            ) {
                module.setVotedFor(request.getCandidateId());
                module.setElectionResetEvent(System.currentTimeMillis());
                response.setVoteGranted(true);
            } else {
                response.setVoteGranted(false);
            }

            response.setTerm(module.getCurrentTerm());
            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void appendEntries(AppendEntriesRequest request, StreamObserver<AppendEntriesResponse> responseObserver) {
        synchronized (module.getLock()) {
            if (module.getRaftState() == RaftState.DEAD) {
                return;
            }

            Utils.printNodeMessage(nodeId,
                    String.format("appendEntries [currentTerm = %d, %s]", module.getCurrentTerm(), Utils.appendEntriesRequestToString(request)));
            if (request.getTerm() > module.getCurrentTerm()) {
                Utils.printNodeMessage(nodeId, "... term out of date in appendEntries");
                module.becomeFollower(request.getTerm());
            }

            AppendEntriesResponse.Builder responseBuilder = AppendEntriesResponse.newBuilder();
            responseBuilder.setSuccess(false);
            if (request.getTerm() == module.getCurrentTerm()) {
                // If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
                // Append any new entries not already in the log
                // If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

                if (module.getRaftState() != RaftState.FOLLOWER) {
                    module.becomeFollower(request.getTerm());
                }
                module.setElectionResetEvent(System.currentTimeMillis());

                // Does our log contain an entry at PrevLogIndex whose term matches
                // PrevLogTerm? Note that in the extreme case of PrevLogIndex=-1 this is
                // vacuously true.
                if (request.getPrevLogIndex() == -1 ||
                        (request.getPrevLogIndex() < module.getLog().size()
                                && request.getPrevLogTerm() == module.getLog().get(request.getPrevLogIndex()).getTerm())) {
                    responseBuilder.setSuccess(true);

                    // Find an insertion point - where there's a term mismatch between
                    // the existing log starting at PrevLogIndex+1 and the new entries sent
                    // in the RPC.
                    var logInsertIndex = request.getPrevLogIndex() + 1;
                    var newEntriesIndex = 0;
                    while (true) {
                        if (logInsertIndex >= module.getLog().size() || newEntriesIndex >= request.getEntriesCount()) {
                            break;
                        }
                        if (module.getLog().get(logInsertIndex).getTerm() != request.getEntries(newEntriesIndex).getTerm()) {
                            break;
                        }
                        logInsertIndex++;
                        newEntriesIndex++;
                    }
                    // At the end of this loop:
                    // - logInsertIndex points at the end of the log, or an index where the
                    //   term mismatches with an entry from the leader
                    // - newEntriesIndex points at the end of Entries, or an index where the
                    //   term mismatches with the corresponding log entry
                    if (newEntriesIndex < request.getEntriesCount()) {
                        var entries = request.getEntriesList();
                        Utils.printNodeMessage(nodeId,
                                String.format("... inserting entries %s from index %d", entries, logInsertIndex));
                        module.getLog().truncate(logInsertIndex, module.getLog().size());
                        module.getLog().addAll(entries.subList(newEntriesIndex, entries.size()));
                    }

                    // Set commit index.
                    if (request.getLeaderCommit() > module.getCommitIndex()) {
                        module.setCommitIndex(Math.min(request.getLeaderCommit(), module.getLog().size() - 1));
                        Utils.printNodeMessage(nodeId, String.format("... setting commitIndex=%d", module.getCommitIndex()));
                        module.getNewCommitReadyChannel().add(new Object());
                    }
                } else {
                    // No match for PrevLogIndex/PrevLogTerm. Populate
                    // ConflictIndex/ConflictTerm to help the leader bring us up to date quickly.
                    var prevLogIndex = request.getPrevLogIndex();
                    if (prevLogIndex >= module.getLog().size()) {
                        responseBuilder.setConflictIndex(module.getLog().size());
                        responseBuilder.setConflictTerm(-1);
                    } else {
                        // prevLogIndex points within our log, but prevLogTerm doesn't match log[prevLogIndex].
                        responseBuilder.setConflictTerm(module.getLog().get(prevLogIndex).getTerm());

                        int index;
                        for (index = prevLogIndex - 1; index >= 0; --index) {
                            if (module.getLog().get(index).getTerm() != responseBuilder.getConflictTerm()) {
                                break;
                            }
                        }
                        responseBuilder.setConflictIndex(index + 1);
                    }
                }
            }

            responseBuilder.setTerm(module.getCurrentTerm());
            var response = responseBuilder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();

            Utils.printNodeMessage(nodeId,
                    String.format("appendEntries reply: %s [%s]",
                            Utils.appendEntriesResponseToString(response), module.getStateAsString()));
        }
    }

    public boolean submitCommand(String command) {
        synchronized (module.getLock()) {
            if (module.getRaftState() == RaftState.LEADER) {
                Utils.printNodeMessage(nodeId, String.format("submitting command [currentTerm = %d, %s]", module.getCurrentTerm(), command));
                LogEntry entry = LogEntry.newBuilder().setTerm(module.getCurrentTerm()).setCommand(command).build();
                module.getLog().add(entry);
                module.getTriggerMessagesChannel().add(new Object());
                return true;
            }
            return false;
        }
    }

    public void start() throws IOException {
        server = ServerBuilder
                .forPort(config.port())
                .addService(this)
                .executor(executor) // Using a thread pool for non-blocking behavior
                .build()
                .start();
        Utils.printNodeMessage(nodeId, String.format("raft started, listening on %d", config.port()));

        // Add a shutdown hook to stop the server gracefully on JVM termination
        /*Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Utils.printNodeMessage(nodeId, "shutting down gRPC server since JVM is shutting down");
            try {
                stop();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("Server shut down");
        }));*/
    }

    public void stop() throws InterruptedException {
        if (server != null) {
            module.stop();
            executor.shutdown();
            server.shutdown().awaitTermination(5, TimeUnit.SECONDS); // Graceful shutdown within 5 seconds
            Utils.printNodeMessage(nodeId, "raft stopped");
        }
        if (commitSender != null) {
            commitSender.stop();
        }
    }

    public void connectToPeers(List<PeerConfiguration> peersConfig) {
        synchronized (module.getLock()) {
            for (var peer : peersConfig) {
                if (peer.nodeId() == nodeId) {
                    continue;
                }
                ManagedChannel channel = ManagedChannelBuilder
                        .forAddress(peer.host(), peer.port())
                        .usePlaintext()
                        .build();
                module.getPeers().add(new PeerConnection(peer.nodeId(), channel, RaftServiceGrpc.newBlockingStub(channel), RaftServiceGrpc.newFutureStub(channel)));
            }
        }
    }

    public void disconnectFromPeers(List<PeerConfiguration> peersConfig) {
        synchronized (module.getLock()) {
            for (var peer : peersConfig) {
                if (peer.nodeId() == nodeId) {
                    for (var p : module.getPeers()) {
                        p.channel().shutdown();
                    }
                    module.getPeers().clear();
                    return;
                } else {
                    var removed = removePeerById(module.getPeers(), peer.nodeId());
                    removed.channel().shutdown();
                }
            }
        }
    }

    public void startConsensusTimerAndCommitSender() {
        if (!consensusTimerhasStarted) {
            module.setRaftNodeExecutor(executor);
            executor.submit(new ConsensusTimer(module));
            consensusTimerhasStarted = true;
        }

        if (!commitSenderhasStarted) {
            commitSender = new CommitSender(module);
            executor.submit(commitSender);
            commitSenderhasStarted = true;
        }
    }

    public List<PeerConnection> getPeers() {
        synchronized (module.getLock()) {
            return module.getPeers();
        }
    }

    public PeerConfiguration getConfig() {
        return config;
    }

    public long getNodeId() {
        return nodeId;
    }

    public Pair<Integer, RaftState> getState() {
        synchronized (module.getLock()) {
            return new ImmutablePair<>(module.getCurrentTerm(), module.getRaftState());
        }
    }

    public PeerConnection removePeerById(List<PeerConnection> list, long id) {
        var iterator = list.iterator();
        while (iterator.hasNext()) {
            var peer = iterator.next();
            if (peer.nodeId() == id) {
                iterator.remove();
                return peer;
            }
        }
        return DEFAULT_CONNECTION;
    }

    public ConsensusModule getConsensusModule() {
        return module;
    }
}
