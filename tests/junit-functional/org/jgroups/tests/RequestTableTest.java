package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.raft.util.RequestTable;
import org.testng.annotations.Test;

import java.util.concurrent.CompletableFuture;

/**
 * @author Bela Ban
 * @since  0.1
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true) // single-threaded because of the single CompletableFuture
public class RequestTableTest {
    protected static final CompletableFuture<byte[]> future=new CompletableFuture<>();

    public void testSimple() {
        RequestTable<String> table=new RequestTable<>();
        table.create(1, "A", future, 3);
        table.add(1, "A", 3);
        assert !table.isCommitted(1);
        boolean done=table.add(1, "B", 3);
        assert !done;
        done=table.add(1, "C", 3);
        assert done;
        assert table.isCommitted(1);
    }

    public void testSingleNode() {
        RequestTable<String> table=new RequestTable<>();
        table.create(1, "A", future, 1);
        assert table.isCommitted(1);
        boolean added=table.add(1, "A", 1);
        assert !added : "should only mark as committed once";
    }
}
