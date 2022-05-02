package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.raft.util.RequestTable;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

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

    public void testAdd() {
        RequestTable<String> table=new RequestTable<>();
        table.create(3, "A", future, 2);
        assert table.isCommitted(1);
        assert !table.isCommitted(3);
        table.add(3, "A", 2);
        assert !table.isCommitted(3);
        boolean commited=table.add(3, "B", 2);
        assert commited && table.isCommitted(3);
        for(int i=4; i <= 10; i++)
            table.create(i, "A", future, 2);
        commited=table.add(10, "B", 2);
        assert commited;
        for(int i=4; i <= 10; i++)
            assert table.isCommitted(10);
        assert table.size() == 8;
    }

    public void testNotifyAndRemove() {
        RequestTable<String> table=new RequestTable<>();
        for(int i=1; i <= 5; i++)
            table.create(i, "A", future, 1);
        IntStream.rangeClosed(1,5).parallel().forEach(i -> table.notifyAndRemove(i, "bb".getBytes()));
        Util.waitUntilTrue(5000, 200, () -> table.size() == 0);
        assert table.size() == 0 : String.format("table size should be %d but is %d", 0, table.size());
    }
}
