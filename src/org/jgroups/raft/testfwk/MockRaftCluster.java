package org.jgroups.raft.testfwk;

import org.jgroups.Message;
import org.jgroups.View;

import java.util.concurrent.Executor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public abstract class MockRaftCluster {

    protected final Executor thread_pool=createThreadPool(1000);
    protected boolean        async;

    public abstract void handleView(View view);

    public abstract void send(Message msg);

    @SuppressWarnings("unchecked")
    protected  <T extends MockRaftCluster> T self() {
        return (T) this;
    }

    public boolean     async()                            {return async;}
    public <T extends MockRaftCluster> T async(boolean b) {async=b; return self();}

    protected static Executor createThreadPool(long max_idle_ms) {
        int max_cores=Runtime.getRuntime().availableProcessors();
        return new ThreadPoolExecutor(0, max_cores, max_idle_ms, TimeUnit.MILLISECONDS,
                new SynchronousQueue<>());
    }

    protected void deliverAsync(RaftNode node, Message msg) {
        thread_pool.execute(() -> node.up(msg));
    }
}
