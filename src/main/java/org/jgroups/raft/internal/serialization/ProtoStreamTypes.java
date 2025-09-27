package org.jgroups.raft.internal.serialization;

public final class ProtoStreamTypes {

    private ProtoStreamTypes() { }

    public static final int WRAPPED_OBJECT = 1;
    public static final int RAFT_COMMAND = 2;
    public static final int USER_COMMAND = 3;
    public static final int READ_USER_COMMAND = 4;
    public static final int WRITE_USER_COMMAND = 5;
    public static final int CLASS_ADAPTER = 6;
    public static final int READ_COMMAND_OPTIONS = 7;
    public static final int WRITE_COMMAND_OPTIONS = 8;
    public static final int STATE_MACHINE_STATE_HOLDER = 9;
}
