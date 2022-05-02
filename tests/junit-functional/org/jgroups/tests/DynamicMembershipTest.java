package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.raft.ELECTION;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.REDIRECT;
import org.jgroups.raft.util.Utils;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Tests the addServer() / removeServer) functionality
 * @author Bela Ban
 * @since  0.2
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class DynamicMembershipTest {
    protected JChannel[]           channels;
    protected RAFT[]               rafts;
    protected Address              leader;
    protected static final String  CLUSTER=DynamicMembershipTest.class.getSimpleName();
    protected final List<String>   mbrs  = Arrays.asList("A", "B", "C");
    protected final List<String>   mbrs2 = Arrays.asList("A", "B", "C", "D");
    protected final List<String>   mbrs3 = Arrays.asList("A", "B", "C", "D", "E");

    @AfterMethod protected void destroy() throws Exception {
        close(channels);
    }


    /** Start a member not in {A,B,C} -> expects an exception */
    public void testStartOfNonMember() {
        JChannel non_member=null;
        try {
            init("A", "B", "C");
            channels=Arrays.copyOf(channels, channels.length+1);
            channels[channels.length-1]=create("X");
            assert false : "Starting a non-member should throw an exception";
        }
        catch(Exception e) {
            System.out.println("received exception as expected: " + e);
        }
        finally {
            close(non_member);
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
     * {A,B} -> -B -> {A} (A is the leader). Call addServer("C"): this will fail as A is not the leader anymore. Then
     * make B rejoin, and addServer("C") will succeed
     */
    public void testAddServerOnLeaderWhichCantCommit() throws Exception {
        init("A", "B");
        Util.waitUntilAllChannelsHaveSameView(10000, 500, channels);
        leader=leader(10000, 500, channels);
        System.out.println("leader = " + leader);
        assert leader != null;

        // close non-leaders
        for(JChannel ch: channels)
            if(!ch.getAddress().equals(leader))
                close(ch);

        RAFT raft=raft(leader);
        try { // this will fail as leader A stepped down when it found that the view's size dropped below the majority
            raft.addServer("C");
            assert false : "Adding server C should fail as the leader stepped down";
        }
        catch(Exception ex) {
            System.out.println("Caught exception (as expected) trying to add C: " + ex);
        }

        // Now start B again, so that addServer("C") can succeed
        for(int i=0; i < channels.length; i++) {
            if(channels[i].isClosed())
                channels[i]=create(String.valueOf((char)('A' + i)));
        }
        Util.waitUntilAllChannelsHaveSameView(10000, 500, channels);

        leader=leader(10000, 500, channels);
        System.out.println("leader = " + leader);
        assert leader != null;
        raft=raft(leader);
        raft.addServer("C"); // adding C should now succeed, as we have a valid leader again

        // Now create and connect C
        channels=Arrays.copyOf(channels, 3);
        channels[2]=create("C");

        assertMembers(10000, 500, mbrs, 2, channels);

        // wait until everyone has committed the addServer(C) operation
        assertCommitIndex(20000, 500, raft(leader).lastAppended(), channels);
    }

    protected void init(String ... nodes) throws Exception {
        channels=new JChannel[nodes.length];
        rafts=new RAFT[nodes.length];
        for(int i=0; i < nodes.length; i++) {
            channels[i]=create(nodes, i);
            rafts[i]=raft(channels[i]);
        }
    }

    protected JChannel create(String name) throws Exception {
        RAFT raft=new RAFT().members(mbrs).raftId(name).stateMachine(new DummyStateMachine())
          .logClass("org.jgroups.protocols.raft.InMemoryLog").logPrefix(name + "-" + CLUSTER);
        JChannel ch=new JChannel(Util.getTestStack(new ELECTION(), raft, new REDIRECT())).name(name);
        ch.connect(CLUSTER);
        return ch;
    }

    protected static JChannel create(String[] names, int index) throws Exception {
        RAFT raft=new RAFT().members(Arrays.asList(names)).raftId(names[index]).stateMachine(new DummyStateMachine())
          .logClass("org.jgroups.protocols.raft.InMemoryLog").logPrefix(names[index] + "-" + CLUSTER);
        JChannel ch=new JChannel(Util.getTestStack(new ELECTION(), raft, new REDIRECT())).name(names[index]);
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

    protected static void assertSameLeader(Address leader, JChannel... channels) {
        for(JChannel ch: channels) {
            final Address raftLeader = raft(ch).leader();
            assert leader.equals(raftLeader)
                : String.format("expected leader to be '%s' but was '%s'", leader, raftLeader);
        }
    }

    protected static void assertMembers(long timeout, long interval, List<String> members, int expected_majority, JChannel... channels) {
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

    protected static void assertCommitIndex(long timeout, long interval, int expected_commit, JChannel... channels) {
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

    protected static void waitUntilAllRaftsHaveLeader(JChannel[] channels) throws TimeoutException {
        Util.waitUntil(5000, 250, () -> Arrays.stream(channels).allMatch(ch -> raft(ch).leader() != null));
    }

    protected RAFT raft(Address addr) {
        return raft(channel(addr));
    }

    protected JChannel channel(Address addr) {
        for(JChannel ch: channels) {
            if(ch.getAddress() != null && ch.getAddress().equals(addr))
                return ch;
        }
        return null;
    }

    protected static RAFT raft(JChannel ch) {
        return ch.getProtocolStack().findProtocol(RAFT.class);
    }

    protected static void close(JChannel... channels) {
        for(JChannel ch: channels) {
            if(ch == null)
                continue;
            RAFT raft=ch.getProtocolStack().findProtocol(RAFT.class);
            try {
                Utils.deleteLogAndSnapshot(raft);
            }
            catch(Exception ignored) {}
            Util.close(ch);
        }
    }

}
