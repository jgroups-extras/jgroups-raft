package org.jgroups.perf.jmh;

import org.jgroups.raft.util.io.ConcurrentCustomByteBufferPool;
import org.jgroups.raft.util.io.CustomByteBuffer;
import org.jgroups.raft.util.io.CustomByteBufferPool;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@BenchmarkMode({Mode.Throughput})
@Warmup(iterations = 5, time = 5)
@Measurement(iterations = 5, time = 10)
@Fork(value = 2, jvmArgsPrepend = "-Djmh.executor=VIRTUAL")
@State(Scope.Benchmark)
public class BufferPoolBenchmark {

    private static final ThreadLocal<CustomByteBufferPool> TL_POOL =
            ThreadLocal.withInitial(CustomByteBufferPool::new);

    private static final ConcurrentCustomByteBufferPool CONCURRENT_POOL =
            new ConcurrentCustomByteBufferPool(16, 64 * 1024);

    @Benchmark
    @Threads(1)
    public void threadLocal_1Thread(Blackhole bh) {
        CustomByteBufferPool pool = TL_POOL.get();
        CustomByteBuffer buf = pool.acquire(512);
        bh.consume(buf);
        pool.release(buf);
    }

    @Benchmark
    @Threads(16)
    public void threadLocal_16Threads(Blackhole bh) {
        CustomByteBufferPool pool = TL_POOL.get();
        CustomByteBuffer buf = pool.acquire(512);
        bh.consume(buf);
        pool.release(buf);
    }

    @Benchmark
    @Threads(1)
    public void concurrent_1Thread(Blackhole bh) {
        CustomByteBuffer buf = CONCURRENT_POOL.acquire(512);
        bh.consume(buf);
        CONCURRENT_POOL.release(buf);
    }

    @Benchmark
    @Threads(16)
    public void concurrent_16Threads(Blackhole bh) {
        CustomByteBuffer buf = CONCURRENT_POOL.acquire(512);
        bh.consume(buf);
        CONCURRENT_POOL.release(buf);
    }
}
