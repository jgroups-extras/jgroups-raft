package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.CompletableFuture;
import org.jgroups.util.Consumer;
import org.jgroups.util.RequestTable;
import org.testng.annotations.Test;

/**
 * @author Bela Ban
 * @since  0.1
 */
@Test(groups=Global.FUNCTIONAL)
public class RequestTableTest {
    protected static final CompletableFuture<byte[]> future=new CompletableFuture<>(new Consumer<byte[]>() {
        public void apply(byte[] arg) {}
        public void apply(Throwable t) {}
    });

    public void testSimple() {
        RequestTable<String> table=new RequestTable<>();
        table.create(1, "A", future);
        table.add(1, "A", 3);
        assert !table.isCommitted(1);
        boolean done=table.add(1, "B", 3);
        assert !done;
        done=table.add(1, "C", 3);
        assert done;
        assert table.isCommitted(1);
    }

}
