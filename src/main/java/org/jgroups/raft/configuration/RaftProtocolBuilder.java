package org.jgroups.raft.configuration;

import java.util.ArrayList;
import java.util.List;

import org.jgroups.protocols.raft.RAFT;
import org.jgroups.raft.JGroupsRaft;
import org.jgroups.raft.util.pattern.NestedBuilder;

public final class RaftProtocolBuilder<T> implements NestedBuilder<Void, JGroupsRaft<T>, JGroupsRaft.Builder<T>> {

    private final JGroupsRaft.Builder<T> parent;
    private String raftId;
    private Class<?> logClass;
    private String logArgs;
    private String logDirectory;
    private String logPrefix;
    private Long resendInterval;
    private Boolean sendCommitsImmediately;
    private Long maxLogSize;
    private Boolean useFsync;
    private List<String> members;

    public RaftProtocolBuilder(JGroupsRaft.Builder<T> parent) {
        this.parent = parent;
    }

    public RaftProtocolBuilder<T> withRaftId(String raftId) {
        this.raftId = raftId;
        return this;
    }

    public RaftProtocolBuilder<T> withLogClass(Class<?> logClass) {
        this.logClass = logClass;
        return this;
    }

    public RaftProtocolBuilder<T> withLogArgs(String logArgs) {
        this.logArgs = logArgs;
        return this;
    }

    public RaftProtocolBuilder<T> withLogDirectory(String logDirectory) {
        this.logDirectory = logDirectory;
        return this;
    }

    public RaftProtocolBuilder<T> withLogPrefix(String logPrefix) {
        this.logPrefix = logPrefix;
        return this;
    }

    public RaftProtocolBuilder<T> withResendInterval(long resendInterval) {
        this.resendInterval = resendInterval;
        return this;
    }

    public RaftProtocolBuilder<T> withSendCommitsImmediately(boolean sendCommitsImmediately) {
        this.sendCommitsImmediately = sendCommitsImmediately;
        return this;
    }

    public RaftProtocolBuilder<T> withMaxLogSize(long maxLogSize) {
        this.maxLogSize = maxLogSize;
        return this;
    }

    public RaftProtocolBuilder<T> withUseFsync(boolean useFsync) {
        this.useFsync = useFsync;
        return this;
    }

    public RaftProtocolBuilder<T> withMembers(List<String> members) {
        this.members = new ArrayList<>(members);
        return this;
    }

    public void build(RAFT raft) {
        if (raftId != null) raft.raftId(raftId);
        if (logClass != null) raft.logClass(logClass.getCanonicalName());
        if (logArgs != null) raft.logArgs(logArgs);
        if (logDirectory != null) raft.logDir(logDirectory);
        if (logPrefix != null) raft.logPrefix(logPrefix);
        if (resendInterval != null) raft.resendInterval(resendInterval);
        if (sendCommitsImmediately != null) raft.sendCommitsImmediately(sendCommitsImmediately);
        if (maxLogSize != null) raft.maxLogSize(maxLogSize);
        if (useFsync != null) raft.logUseFsync(useFsync);
        if (members != null && !members.isEmpty()) raft.members(members);
    }

    @Override
    public JGroupsRaft.Builder<T> and() {
        return parent;
    }

    @Override
    public Void build() {
        throw new UnsupportedOperationException("This builder does not produce a RAFT result. Use the build(RAFT raft) method instead.");
    }
}
