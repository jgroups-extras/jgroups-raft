package org.jgroups.raft.internal.command;

import org.jgroups.raft.exceptions.JRaftException;
import org.jgroups.raft.internal.serialization.RaftTypeIds;
import org.jgroups.raft.internal.serialization.SingleBinarySerializer;
import org.jgroups.raft.serialization.SerializationContextRead;
import org.jgroups.raft.serialization.SerializationContextWrite;

public final class RaftResponse {

    public static final SingleBinarySerializer<RaftResponse> SERIALIZER = RaftResponseSerializer.INSTANCE;

    private final Object response;
    private final Throwable exception;

    private RaftResponse(Object response, Throwable exception) {
        this.response = response;
        this.exception = exception;
    }

    public static RaftResponse success(Object response) {
        return new RaftResponse(response, null);
    }

    public static RaftResponse failure(Throwable exception) {
        return new RaftResponse(null, exception);
    }

    private static RaftResponse failure(String exception) {
        return RaftResponse.failure(JRaftException.stackless(String.format("Failed apply command, see remote for stack trace: %s", exception)));
    }

    public boolean isSuccess() {
        return exception == null;
    }

    public Object response() {
        return response;
    }

    public Throwable exception() {
        return exception;
    }

    private static final class RaftResponseSerializer implements SingleBinarySerializer<RaftResponse> {
        private static final RaftResponseSerializer INSTANCE = new RaftResponseSerializer();

        @Override
        public void write(SerializationContextWrite ctx, RaftResponse target) {
            boolean success = target.isSuccess();
            ctx.writeBoolean(success);
            if (success) {
                ctx.writeObject(target.response);
            } else {
                ctx.writeUTF(target.exception.getMessage());
            }
        }

        @Override
        public RaftResponse read(SerializationContextRead ctx, byte version) {
            boolean success = ctx.readBoolean();
            if (success) {
                return RaftResponse.success(ctx.readObject());
            }
            return RaftResponse.failure(ctx.readUTF());
        }

        @Override
        public Class<RaftResponse> javaClass() {
            return RaftResponse.class;
        }

        @Override
        public int type() {
            return RaftTypeIds.RAFT_RESPONSE;
        }

        @Override
        public byte version() {
            return 0;
        }
    }
}
