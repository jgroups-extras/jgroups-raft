package org.jgroups.raft.blocks;

import org.jgroups.blocks.atomic.AsyncCounter;
import org.jgroups.blocks.atomic.Counter;
import org.jgroups.blocks.atomic.SyncCounter;
import org.jgroups.raft.Options;
import org.jgroups.util.AsciiString;
import org.jgroups.util.CompletableFutures;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * RAFT Implementation of {@link AsyncCounter}.
 *
 * @since 1.0.9
 */
public class AsyncCounterImpl implements AsyncCounter {
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
        return counterService.allowDirtyReads() ? getLocal() : counterService.asyncGet(asciiName);
    }

    public CompletionStage<Long> getLocal() {
        return CompletableFuture.completedFuture(counterService._get(asciiName.toString()));
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
    public SyncCounter sync() {
        return sync;
    }

    @Override
    public AsyncCounter async() {
        return this;
    }

    @Override
    public <T extends Counter> T withOptions(Options opts) {
        if(opts != null)
            this.options=opts;
        return (T)this;
    }

    public String toString() {
        return String.valueOf(CompletableFutures.join(AsyncCounterImpl.this.getLocal()));
    }

    private final class Sync implements SyncCounter {

        @Override
        public String getName() {
            return asciiName.toString();
        }

        @Override
        public long get() {
            return CompletableFutures.join(AsyncCounterImpl.this.get());
        }

        public long getLocal() {
            return CompletableFutures.join(AsyncCounterImpl.this.getLocal());
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
        public AsyncCounter async() {
            return AsyncCounterImpl.this;
        }

        @Override
        public SyncCounter sync() {
            return this;
        }

        @Override
        public <T extends Counter> T withOptions(Options opts) {
            if(opts != null)
                AsyncCounterImpl.this.options=opts;
            return (T)this;
        }

        public String toString() {
            return AsyncCounterImpl.this.toString();
        }
    }
}
