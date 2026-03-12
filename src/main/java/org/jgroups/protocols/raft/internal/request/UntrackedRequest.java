package org.jgroups.protocols.raft.internal.request;

/**
 * Base for request types that do not participate in latency tracking.
 *
 * <p>
 * All lifecycle hooks are final no-ops.
 * </p>
 *
 * @author José Bolina
 * @since 2.0
 */
abstract sealed class UntrackedRequest implements BaseRequest permits CallableDownRequest, UpRequest {

    @Override
    public final void startTotal() { }

    @Override
    public final void completeTotal() { }

    @Override
    public final void startProcessing() { }

    @Override
    public final void completeProcessing() { }
}
