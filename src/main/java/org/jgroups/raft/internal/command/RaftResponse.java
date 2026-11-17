package org.jgroups.raft.internal.command;

import org.jgroups.raft.internal.serialization.RaftTypeIds;
import org.jgroups.raft.internal.serialization.SingleBinarySerializer;
import org.jgroups.raft.serialization.SerializationContextRead;
import org.jgroups.raft.serialization.SerializationContextWrite;

/**
 * The result of a state machine method, returned to the caller that submitted the command.
 *
 * <p>
 * Only successful results exist. A state machine method that fails stops the node instead of producing a response,
 * since a deterministic state machine must not fail while applying a committed command.
 * </p>
 *
 * @param response The value returned by the state machine method, possibly {@code null}.
 */
public record RaftResponse(Object response) {

    public static final SingleBinarySerializer<RaftResponse> SERIALIZER = RaftResponseSerializer.INSTANCE;

    private static final class RaftResponseSerializer implements SingleBinarySerializer<RaftResponse> {
        private static final RaftResponseSerializer INSTANCE = new RaftResponseSerializer();

        @Override
        public void write(SerializationContextWrite ctx, RaftResponse target) {
            ctx.writeObject(target.response);
        }

        @Override
        public RaftResponse read(SerializationContextRead ctx, byte version) {
            return new RaftResponse(ctx.readObject());
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
