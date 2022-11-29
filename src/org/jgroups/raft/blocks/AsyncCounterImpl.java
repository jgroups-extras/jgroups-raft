package org.jgroups.raft.blocks;

import org.jgroups.blocks.atomic.AsyncCounter;
import org.jgroups.blocks.atomic.CounterFunction;
import org.jgroups.raft.Options;
import org.jgroups.util.AsciiString;
import org.jgroups.util.CompletableFutures;
import org.jgroups.util.Streamable;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * RAFT Implementation of {@link AsyncCounter}.
 *
 * @since 1.0.9
 */
public class AsyncCounterImpl implements RaftAsyncCounter {
    private final CounterService counterService;
    private final AsciiString    asciiName;
    private final Sync           sync;
    private Options              options=new Options();


    public AsyncCounterImpl(CounterService counterService, String name) {
        this.counterService=counterService;
        this.asciiName = new AsciiString(Objects.requireNonNull(name));
        this.sync = new Sync();
    }

    @Override
    public String getName() {
        return asciiName.toString();
    }

    @Override
    public CompletionStage<Long> get() {
        return counterService.allowDirtyReads() ? CompletableFuture.completedFuture(getLocal()) : counterService.asyncGet(asciiName);
    }

    public long getLocal() {
        return counterService._get(asciiName.toString());
    }

    @Override
    public CompletionStage<Void> set(long new_value) {
        return counterService.asyncSet(asciiName, new_value);
    }

    @Override
    public CompletionStage<Long> compareAndSwap(long expect, long update) {
        return counterService.asyncCompareAndSwap(asciiName, expect, update, options);
    }

    @Override
    public CompletionStage<Long> addAndGet(long delta) {
        return counterService.asyncAddAndGet(asciiName, delta, options);
    }

    @Override
    public <T extends Streamable> CompletionStage<T> update(CounterFunction<T> updateFunction) {
        return counterService.asyncUpdate(asciiName, updateFunction, options);
    }

    @Override
    public RaftSyncCounter sync() {
        return sync;
    }

    @Override
    public RaftAsyncCounter async() {
        return this;
    }

    @Override
    public RaftAsyncCounter withOptions(Options opts) {
        if(opts != null)
            this.options=opts;
        return this;
    }

    public String toString() {
        return String.valueOf(getLocal());
    }

    private final class Sync implements RaftSyncCounter {

        @Override
        public String getName() {
            return asciiName.toString();
        }

        @Override
        public long get() {
            return CompletableFutures.join(AsyncCounterImpl.this.get());
        }

        public long getLocal() {
            return AsyncCounterImpl.this.getLocal();
        }

        @Override
        public void set(long new_value) {
            CompletableFutures.join(AsyncCounterImpl.this.set(new_value));
        }

        @Override
        public long compareAndSwap(long expect, long update) {
            CompletionStage<Long> f=AsyncCounterImpl.this.compareAndSwap(expect, update);
            Long retval=CompletableFutures.join(f);
            return retval == null? 0 : retval;
        }

        @Override
        public long addAndGet(long delta) {
            CompletionStage<Long> f=AsyncCounterImpl.this.addAndGet(delta);
            Long retval=CompletableFutures.join(f);
            return retval == null? 0 : retval; // 0 as a valid result from the cast of null?
        }

        @Override
        public <T extends Streamable> T update(CounterFunction<T> updateFunction) {
            return CompletableFutures.join(AsyncCounterImpl.this.update(updateFunction));
        }

        @Override
        public RaftAsyncCounter async() {
            return AsyncCounterImpl.this;
        }

        @Override
        public RaftSyncCounter sync() {
            return this;
        }

        @Override
        public RaftSyncCounter withOptions(Options opts) {
            if(opts != null)
                AsyncCounterImpl.this.options=opts;
            return this;
        }

        public String toString() {
            return AsyncCounterImpl.this.toString();
        }
    }
}
