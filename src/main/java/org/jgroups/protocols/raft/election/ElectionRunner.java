package org.jgroups.protocols.raft.election;

import org.jgroups.Address;
import org.jgroups.util.Runner;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

/**
 * Extends JGroups' {@link Runner} to pass election parameters to the voting process and return a result.
 *
 * <p>
 * The inherited {@link #start()} (no-arg) is used for organic elections triggered by view changes. In this case, no parameters
 * are configured, so the voting process runs with no exclusion and a throwaway future.
 * </p>
 *
 * <p>
 * {@link #start(Address)} is used for forced elections. It configures an excluded address and returns a {@link CompletionStage}
 * that completes when a new leader is elected. If a forced election is already in progress (parameters not yet reset),
 * the existing election's future is returned via CAS -- multiple concurrent callers all receive the same future.
 * </p>
 *
 * @author José Bolina
 * @since 2.0
 */
final class ElectionRunner extends Runner {

    private final InnerRunnable runnable;

    public ElectionRunner(String name, BiConsumer<Address, CompletableFuture<Address>> election) {
        super(name, new InnerRunnable(election), null);
        this.runnable = (InnerRunnable) super.function;
    }

    @Override
    public synchronized Runner start() {
        runnable.define(null);
        return super.start();
    }

    /**
     * Starts a forced election with the given address excluded from candidacy.
     *
     * <p>
     * If an election is already in progress (parameters not yet reset), the existing election's future is returned
     * and the Runner is not restarted.
     * </p>
     *
     * @param exclude the address to exclude from leader candidacy, or {@code null} for no exclusion.
     * @return a stage that completes with the newly elected leader's address.
     */
    public synchronized CompletionStage<Address> start(Address exclude) {
        CompletionStage<Address> cs = runnable.define(exclude);
        super.start();
        return cs;
    }

    @Override
    public synchronized Runner stop() {
        super.stop();
        runnable.stop();
        return this;
    }

    private static final class InnerRunnable implements Runnable {
        private final AtomicReference<ElectionParameters> parameters;
        private final BiConsumer<Address, CompletableFuture<Address>> delegate;

        private InnerRunnable(BiConsumer<Address, CompletableFuture<Address>> delegate) {
            this.parameters = new AtomicReference<>(null);
            this.delegate = delegate;
        }

        @Override
        public void run() {
            // Running an election will always register the parameters.
            ElectionParameters params = parameters.get();
            delegate.accept(params.address(), params.future());
        }

        /**
         * Configures parameters for a forced election.
         *
         * <p>
         * Uses CAS on the {@link AtomicReference}. If parameters is null (no election in progress), the CAS succeeds and
         * the new future is returned. If parameters is non-null (election already in progress), the CAS fails and the
         * existing election's future is returned; concurrent callers join the in-flight election.
         * </p>
         *
         * @param exclude the address to exclude from leader candidacy, or {@code null} for no exclusion.
         * @return a stage that completes with the newly elected leader's address.
         */
        public CompletionStage<Address> define(Address exclude) {
            CompletableFuture<Address> cf = new CompletableFuture<>();
            ElectionParameters newParams = new ElectionParameters(exclude, cf);
            ElectionParameters witness = parameters.compareAndExchange(null, newParams);
            return witness != null ? witness.future() : cf;
        }

        /**
         * Clears parameters so the next {@link #define(Address)} call can succeed.
         */
        public void stop() {
            parameters.set(null);
        }
    }

    private record ElectionParameters(Address address, CompletableFuture<Address> future) { }
}
