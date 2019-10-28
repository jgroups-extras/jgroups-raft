package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.raft.ELECTION;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.REDIRECT;
import org.jgroups.protocols.raft.StateMachine;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.TimeoutException;

/**
 * Tests the addServer() / removeServer) functionality
 * @author Bela Ban
 * @since  0.2
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class DynamicMembershipTest {
    protected JChannel[]                channels;
    protected RAFT[]                    rafts;
    protected Address                   leader;
    protected static final String       CLUSTER=DynamicMembershipTest.class.getSimpleName();
    protected static final List<String> mbrs  = Arrays.asList("A", "B", "C");
    protected static final List<String> mbrs2 = Arrays.asList("A", "B", "C", "D");
    protected static final List<String> mbrs3 = Arrays.asList("A", "B", "C", "D", "E");

    @AfterMethod protected void destroy() {
        close(true, true, channels);
    }


    /** Start a member not in {A,B,C} -> expects an exception */
    public void testStartOfNonMember() {
        JChannel non_member=null;
        try {
            init("X");
            assert false : "Starting a non-member should throw an exception";
        }
        catch(Exception e) {
            System.out.println("received exception as expected: " + e.toString());
        }
        finally {
            close(true, true, non_member);
        }
    }

    /** Starts only 1 member: no leader. Calling {@link org.jgroups.protocols.raft.RAFT#addServer(String)} must throw an exception */
    public void testMembershipChangeOnNonLeader() throws Exception {
        init("A");
        RAFT raft=raft(channels[0]);
        try {
            raft.addServer("X");
            assert false : "Calling RAFT.addServer() on a non-leader must throw an exception";
        }
        catch(Exception ex) {
            System.out.println("received exception calling RAFT.addServer() on a non-leader (as expected): " + ex);
        }
    }

    /** {A,B,C} +D +E -E -D */
    public void testSimpleAddAndRemove() throws Exception {
        init("A", "B", "C");
        leader=leader(10000, 500, channels);
        System.out.println("leader = " + leader);
        assert leader != null;
        waitUntilAllRaftsHaveLeader(channels);
        assertSameLeader(leader, channels);
        assertMembers(5000, 500, mbrs, 2, channels);

        System.out.println("\nAdding server D");
        RAFT raft=raft(leader);
        raft.addServer("D");
        assertMembers(10000, 500, mbrs2, 3, channels);

        System.out.println("\nAdding server E");
        raft=raft(leader);
        raft.addServer("E");
        assertMembers(10000, 500, mbrs3, 3, channels);

        System.out.println("\nRemoving server E");
        raft=raft(leader);
        raft.removeServer("E");
        assertMembers(10000, 500, mbrs2, 3, channels);

        System.out.println("\nRemoving server D");
        raft=raft(leader);
        raft.removeServer("D");
        assertMembers(10000, 500, mbrs, 2, channels);
    }

    /**
     * {A,B,C} +D +E +F +G +H +I +J
     */
    public void testAddServerSimultaneously()
            throws Exception {
        init("A", "B", "C");
        leader = leader(10000, 500, channels);
        System.out.println("leader = " + leader);
        assert leader != null;
        waitUntilAllRaftsHaveLeader(channels);
        assertSameLeader(leader, channels);
        assertMembers(5000, 500, mbrs, 2, channels);

        final RAFT raft = raft(leader);

        final List<String> newServers = Arrays.asList("D", "E", "F", "G", "H", "I", "J");
        final CountDownLatch addServerLatch = new CountDownLatch(1);
        final CountDownLatch completionLatch = new CountDownLatch(newServers.size());
        final AtomicBoolean success = new AtomicBoolean(true);

        for (String newServer : newServers) {
            new Thread(() -> {
                try {
                    addServerLatch.await();
                    raft.addServer(newServer);
                    if (raft.requestTableSize() > 1){
                        success.compareAndSet(true, false);
                    }
                } catch (IllegalStateException e) {

                } catch (Exception e) {
                    e.printStackTrace();
                }
                completionLatch.countDown();
            }).start();
        }

        addServerLatch.countDown();
        completionLatch.await();

        assert success.get();
    }

    /**
     * {A,B} -> -B -> {A} (A is the leader). Then addServer(D) on A. Then another addServer(E) on A. The second addServer()
     * should fail as A hasn't yet committed the first.
     */
    public void testAddServerOnLeaderWhichCantCommit() throws Exception {
        init("A", "B");
        Util.waitUntilAllChannelsHaveSameView(10000, 500, channels);
        leader=leader(10000, 500, channels);
        System.out.println("leader = " + leader);
        assert leader != null;

        // close all non-leaders
        for(JChannel ch: channels)
            if(!ch.getAddress().equals(leader))
                close(true, true, ch);

        RAFT raft=raft(leader);
        // try to add D. The leader will actually add this to the log, but won't be able to commit it, so the second
        // addServer() below will fail
        raft.addServer("D");
        assertMembers(5000, 500, mbrs2, 3, channels);

        // This will fail as the first addServer(D) has not yet been committed
        try {
            raft.addServer("E");
            assert false : "Adding server E should fail as adding server D has not yet been committed";
        }
        catch(Exception ex) {
            System.out.println("Caught exception (as expected) trying to add another server: " + ex);
        }

        // Now add B and C again, so that the first addServer(D) op can commit (it now needs 3 votes to commit)
        channels=Arrays.copyOf(channels, 3);
        channels[2]=create("C");
        for(int i=0; i < channels.length; i++) {
            if(channels[i].isClosed())
                channels[i]=create(String.valueOf((char)('A' + i)));
        }
        Util.waitUntilAllChannelsHaveSameView(10000, 500, channels);

        // Now wait (and assert) for RAFT.members to be {A,B,C,D}
        assertMembers(10000, 500, mbrs2, 3, channels);

        // wait until everyone has committed the addServer(D) operation
        assertCommitIndex(20000, 500, raft(leader).lastAppended(), channels);

        // Now addServer(E) should work
        raft.addServer("E");
        assertMembers(10000, 500, mbrs3, 3, channels);
    }

    protected void init(String ... nodes) throws Exception {
        channels=new JChannel[nodes.length];
        rafts=new RAFT[nodes.length];
        for(int i=0; i < nodes.length; i++) {
            channels[i]=create(nodes[i]);
            rafts[i]=raft(channels[i]);
        }
    }

    protected JChannel create(String name) throws Exception {
        RAFT raft=new RAFT().members(mbrs).raftId(name).stateMachine(new DummyStateMachine())
          .logClass("org.jgroups.protocols.raft.InMemoryLog").logName(name + "-" + CLUSTER);
        JChannel ch=new JChannel(Util.getTestStack(new ELECTION(), raft, new REDIRECT())).name(name);
        ch.connect(CLUSTER);
        return ch;
    }

    protected static Address leader(long timeout, long interval, JChannel ... channels) {
        long target_time=System.currentTimeMillis() + timeout;
        while(System.currentTimeMillis() <= target_time) {
            for(JChannel ch : channels) {
                if(ch.isConnected() && raft(ch).leader() != null)
                    return raft(ch).leader();
            }
            Util.sleep(interval);
        }
        return null;
    }

    protected void assertSameLeader(Address leader, JChannel ... channels) {
        for(JChannel ch: channels) {
            final Address raftLeader = raft(ch).leader();
            assert leader.equals(raftLeader)
                : String.format("expected leader to be '%s' but was '%s'", leader, raftLeader);
        }
    }

    protected void assertMembers(long timeout, long interval, List<String> members, int expected_majority, JChannel... channels) {
        long target_time=System.currentTimeMillis() + timeout;
        while(System.currentTimeMillis() <= target_time) {
            boolean all_ok=true;
            for(JChannel ch: channels) {
                if(!ch.isConnected())
                    continue;
                RAFT raft=raft(ch);
                if(!new HashSet<>(raft.members()).equals(new HashSet<>(members)))
                    all_ok=false;
            }
            if(all_ok)
                break;
            Util.sleep(interval);
        }
        for(JChannel ch: channels) {
            if(!ch.isConnected())
                continue;
            RAFT raft=raft(ch);
            System.out.printf("%s: members=%s, majority=%d\n", ch.getAddress(), raft.members(), raft.majority());
            assert new HashSet<>(raft.members()).equals(new HashSet<>(members))
              : String.format("expected members=%s, actual members=%s", members, raft.members());

            assert raft.majority() == expected_majority
              : ch.getName() + ": expected majority=" + expected_majority + ", actual=" + raft.majority();
        }
    }

    protected void assertCommitIndex(long timeout, long interval, int expected_commit, JChannel... channels) {
        long target_time=System.currentTimeMillis() + timeout;
        while(System.currentTimeMillis() <= target_time) {
            boolean all_ok=true;
            for(JChannel ch: channels) {
                RAFT raft=raft(ch);
                if(expected_commit != raft.commitIndex())
                    all_ok=false;
            }
            if(all_ok)
                break;
            Util.sleep(interval);
        }
        for(JChannel ch: channels) {
            RAFT raft=raft(ch);
            System.out.printf("%s: members=%s, last-applied=%d, commit-index=%d\n", ch.getAddress(), raft.members(),
                              raft.lastAppended(), raft.commitIndex());
            assert raft.commitIndex() == expected_commit : String.format("%s: last-applied=%d, commit-index=%d",
                                                                         ch.getAddress(), raft.lastAppended(), raft.commitIndex());
        }
    }

    protected void waitUntilAllRaftsHaveLeader(JChannel[] channels) throws TimeoutException {
        Util.waitUntil(5000, 250, () -> Arrays.stream(channels).allMatch(ch -> raft(ch).leader() != null));
    }

    protected RAFT raft(Address addr) {
        return raft(channel(addr));
    }

    protected JChannel channel(Address addr) {
        for(JChannel ch: Arrays.asList(channels)) {
            if(ch.getAddress() != null && ch.getAddress().equals(addr))
                return ch;
        }
        return null;
    }

    protected static RAFT raft(JChannel ch) {
        return (RAFT)ch.getProtocolStack().findProtocol(RAFT.class);
    }

    protected void close(boolean remove_log, boolean remove_snapshot, JChannel ... channels) {
        for(JChannel ch: channels) {
            if(ch == null)
                continue;
            RAFT raft=ch.getProtocolStack().findProtocol(RAFT.class);
            if(remove_log)
                raft.log().delete(); // remove log files after the run
            if(remove_snapshot)
                raft.deleteSnapshot();
            Util.close(ch);
        }
    }

    protected static class DummyStateMachine implements StateMachine {
        public byte[] apply(byte[] data, int offset, int length) throws Exception {return new byte[0];}
        public void readContentFrom(DataInput in) throws Exception {}
        public void writeContentTo(DataOutput out) throws Exception {}
    }
}
