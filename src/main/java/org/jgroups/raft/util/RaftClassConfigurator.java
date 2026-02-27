package org.jgroups.raft.util;

import static org.jgroups.protocols.raft.election.BaseElection.LEADER_ELECTED;
import static org.jgroups.protocols.raft.election.BaseElection.PRE_VOTE_REQ;
import static org.jgroups.protocols.raft.election.BaseElection.PRE_VOTE_RSP;
import static org.jgroups.protocols.raft.election.BaseElection.VOTE_REQ;
import static org.jgroups.protocols.raft.election.BaseElection.VOTE_RSP;

import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.raft.AppendEntriesRequest;
import org.jgroups.protocols.raft.AppendEntriesResponse;
import org.jgroups.protocols.raft.AppendResult;
import org.jgroups.protocols.raft.CLIENT;
import org.jgroups.protocols.raft.ELECTION;
import org.jgroups.protocols.raft.ELECTION2;
import org.jgroups.protocols.raft.InstallSnapshotRequest;
import org.jgroups.protocols.raft.LogEntries;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.REDIRECT;
import org.jgroups.protocols.raft.election.LeaderElected;
import org.jgroups.protocols.raft.election.PreVoteRequest;
import org.jgroups.protocols.raft.election.PreVoteResponse;
import org.jgroups.protocols.raft.election.VoteRequest;
import org.jgroups.protocols.raft.election.VoteResponse;
import org.jgroups.stack.Protocol;

import net.jcip.annotations.ThreadSafe;

/**
 * Centralized registration of all RAFT protocol types with JGroups ClassConfigurator.
 *
 * <p>
 * This class wraps the JGroups: {@link ClassConfigurator} in a synchronized block to ensure the protocol and headers
 * are properly registered.
 * </p>
 *
 * <p>
 * All protocol registrations are performed sequentially in this class's static initializer block, which is guaranteed by
 * the JVM to execute exactly once in a thread-safe manner. Protocol classes call {@link #initialize()} in their static
 * blocks to trigger class loading, ensuring registrations complete before any concurrent operations.
 * </p>
 *
 * <p>
 * <b>Usage:</b> Each protocol class should include this in their static block:
 * <pre>{@code
 * static {
 *     RaftClassConfigurator.initialize();
 * }
 * }</pre>
 * This forces class loading and registration to occur sequentially, regardless of which protocol class is loaded first
 * or how many threads are creating JChannels concurrently.
 * </p>
 *
 * <p>
 * <b>Thread Safety:</b> The JVM guarantees that static initializer blocks execute exactly once per class, with proper
 * synchronization. Even if multiple threads simultaneously trigger class loading, only one thread executes the static
 * block while others wait.
 * </p>
 *
 * @author José Bolina
 * @since 2.0
 * @see ClassConfigurator
 */
@ThreadSafe
public final class RaftClassConfigurator {

    private static final RaftClassConfigurator INSTANCE = new RaftClassConfigurator();

    private RaftClassConfigurator() { }

    static {
        initializeInternal();
    }

    public static void initialize() {
        // Intentionally empty - just triggers class loading and static block execution
    }

    /**
     * Performs all protocol and header registrations sequentially.
     *
     * <p>
     * Called exclusively from the static initializer block.
     * </p>
     */
    private static void initializeInternal() {
        INSTANCE.registerClient();
        INSTANCE.registerElection();
        INSTANCE.registerRaft();
        INSTANCE.registerRedirect();
    }

    /**
     * Registers the CLIENT protocol with JGroups.
     */
    private void registerClient() {
        addProtocol(CLIENT.CLIENT_ID, CLIENT.class);
    }

    /**
     * Registers all election-related protocols and message types with JGroups.
     * <p>
     * Includes ELECTION, ELECTION2, and their associated message types (VoteRequest, VoteResponse, LeaderElected,
     * PreVoteRequest, PreVoteResponse).
     * </p>
     */
    private void registerElection() {
        addProtocol(ELECTION.ELECTION_ID, ELECTION.class);
        addProtocol(ELECTION2.ELECTION_ID, ELECTION2.class);

        add(VOTE_REQ,       VoteRequest.class);
        add(VOTE_RSP,       VoteResponse.class);
        add(LEADER_ELECTED, LeaderElected.class);
        add(PRE_VOTE_REQ, PreVoteRequest.class);
        add(PRE_VOTE_RSP, PreVoteResponse.class);
    }

    /**
     * Registers the RAFT protocol and all associated message types with JGroups.
     */
    private void registerRaft() {
        addProtocol(RAFT.RAFT_ID, RAFT.class);
        add(RAFT.APPEND_ENTRIES_REQ,   AppendEntriesRequest.class);
        add(RAFT.APPEND_ENTRIES_RSP,   AppendEntriesResponse.class);
        add(RAFT.APPEND_RESULT,        AppendResult.class);
        add(RAFT.INSTALL_SNAPSHOT_REQ, InstallSnapshotRequest.class);
        add(RAFT.LOG_ENTRIES,          LogEntries.class);
    }

    /**
     * Registers the REDIRECT protocol and its header type with JGroups.
     */
    public void registerRedirect() {
        addProtocol(REDIRECT.REDIRECT_ID, REDIRECT.class);
        add(REDIRECT.REDIRECT_HDR, REDIRECT.RedirectHeader.class);
    }

    /**
     * Thread-safe protocol registration.
     *
     * @param id Protocol ID (must be >= 512)
     * @param clazz Protocol class
     */
    private void addProtocol(short id, Class<? extends Protocol> clazz) {
        ClassConfigurator.addProtocol(id, clazz);
    }

    /**
     * Thread-safe header/message class registration.
     *
     * @param id Magic number (must be > 1024)
     * @param clazz Class to register
     */
    private void add(short id, Class<?> clazz) {
        ClassConfigurator.add(id, clazz);
    }
}
