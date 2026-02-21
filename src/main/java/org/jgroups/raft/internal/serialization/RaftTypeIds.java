package org.jgroups.raft.internal.serialization;

public final class RaftTypeIds {

    private RaftTypeIds() { }

    private static final int BASE_INTERNAL_ID = 64;

    public static final int RAFT_COMMAND = BASE_INTERNAL_ID + 1;
    public static final int RAFT_RESPONSE = BASE_INTERNAL_ID + 2;
    public static final int USER_COMMAND = BASE_INTERNAL_ID + 3;
    public static final int READ_COMMAND_OPTIONS = BASE_INTERNAL_ID + 4;
    public static final int WRITE_COMMAND_OPTIONS = BASE_INTERNAL_ID + 5;
    public static final int STATE_MACHINE_STATE_HOLDER = BASE_INTERNAL_ID + 6;
}
