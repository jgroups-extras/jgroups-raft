package org.jgroups.raft;

import static org.assertj.core.api.Assertions.assertThat;

import org.jgroups.Global;
import org.jgroups.raft.util.RequestTable;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.IntStream;

/**
 * @author Bela Ban
 * @since  0.1
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true) // single-threaded because of the single CompletableFuture
public class RequestTableTest {
    protected static final CompletableFuture<byte[]> future=new CompletableFuture<>();

    public void testSimple() {
        Supplier<Integer> majority=() -> 3;
        RequestTable<String> table=new RequestTable<>();
        table.create(1, "A", future, majority);
        table.add(1, "A", majority);
        assertThat(table.isCommitted(1)).isFalse();

        boolean done=table.add(1, "B", majority);
        assertThat(done).isFalse();

        done=table.add(1, "C", majority);
        assertThat(done).isTrue();
        assertThat(table.isCommitted(1)).isTrue();
    }

    public void testSingleNode() {
        RequestTable<String> table=new RequestTable<>();
        table.create(1, "A", future, () -> 1);
        assertThat(table.isCommitted(1)).isTrue();

        boolean added=table.add(1, "A", () -> 1);
        assertThat(added).as("should only mark as committed once").isFalse();
    }

    public void testAdd() {
        RequestTable<String> table=new RequestTable<>();
        table.create(3, "A", future, () -> 2);
        assertThat(table.isCommitted(1)).isTrue();
        assertThat(table.isCommitted(3)).isFalse();

        table.add(3, "A", () -> 2);
        assertThat(table.isCommitted(3)).isFalse();

        boolean commited=table.add(3, "B", () -> 2);
        assertThat(commited).isTrue();
        assertThat(table.isCommitted(3)).isTrue();

        for(int i=4; i <= 10; i++)
            table.create(i, "A", future, () -> 2);

        commited=table.add(10, "B", () -> 2);
        assertThat(commited).isTrue();

        for(int i=4; i <= 10; i++)
            assertThat(table.isCommitted(10)).isTrue();

        assertThat(table.size()).isEqualTo(8);
    }

    public void testNotifyAndRemove() {
        RequestTable<String> table=new RequestTable<>();
        for(int i=1; i <= 5; i++)
            table.create(i, "A", future, () -> 1);

        IntStream.rangeClosed(1,5).parallel().forEach(i -> table.notifyAndRemove(i, "bb".getBytes()));

        Util.waitUntilTrue(5000, 200, () -> table.size() == 0);

        assertThat(table.size())
                .withFailMessage("table size should be %d but is %d", 0, table.size())
                .isZero();
    }
}
