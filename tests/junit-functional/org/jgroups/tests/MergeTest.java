package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.REDIRECT;
import org.jgroups.protocols.raft.election.BaseElection;
import org.jgroups.raft.util.Utils;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.tests.election.BaseElectionTest;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Tests leader election on a merge (https://github.com/belaban/jgroups-raft/issues/125). Verifies that there can be
 * at most 1 leader during concurrent election rounds run by different coordinators.
 * @author Bela Ban
 * @since  1.0.10
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true, dataProvider = BaseElectionTest.ALL_ELECTION_CLASSES_PROVIDER)
public class MergeTest extends BaseElectionTest {
    protected JChannel            a,b,c,d,e;
    protected static final String CLUSTER=MergeTest.class.getSimpleName();
    protected final List<String>  members=Arrays.asList("A", "B", "C", "D", "E");
    protected RAFT[]              rafts;

    @BeforeMethod
    protected void init() throws Exception {
        a=create("A"); a.connect(CLUSTER);
        b=create("B"); b.connect(CLUSTER);
        c=create("C"); c.connect(CLUSTER);
        d=create("D"); d.connect(CLUSTER);
        e=create("E"); e.connect(CLUSTER);
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a,b,c,d,e);
        rafts=new RAFT[members.size()];
        int index=0;
        for(JChannel ch: Arrays.asList(a,b,c,d,e)) {
            RAFT r=ch.getProtocolStack().findProtocol(RAFT.class);
            rafts[index++]=r;
        }
    }

    @AfterMethod
    protected void destroy() {
        close(e, d, c, b, a);
    }

    /**
     * Members {A,B,C,D,E} are split into {A,B,C,D} and {E,D,C,B}. A and E are coordinators and start leader elections;
     * but only one of them must become leader. Because members B, C and D are part of _both_ subgroups, they can
     * potentially vote for both A and E, leading to 2 leaders.<br/>
     * The fix implemented in https://github.com/belaban/jgroups-raft/issues/125 will ensure that members only vote
     * for A _or_ E, but not both, in a given term.
     */
    //@Test(invocationCount=10)
    public void testMerge(Class<?> ignore) throws TimeoutException {
        long id=a.getView().getViewId().getId() +1;
        View v1=createView(id, a, b), v2=createView(id, c, d), v3=createView(id, e);

        // Create views {A,B}, {C,D} and {E}: no leaders
        injectView(v1, a, b);
        injectView(v2, c, d);
        injectView(v3, e);
        assertView(v1, a, b);
        assertView(v2, c, d);
        assertView(v3, e);

        List<Address> leaders=leaders(a, b, c, d, e);
        assert leaders.isEmpty();

        System.out.printf("-- channels before:\n%s\n", print(a,b,c,d,e));

        // now create overlapping views {A,B,C,D} and {E,D,C,B}
        v1=createView(++id, a,b,c,d);
        v2=createView(++id, e,d,c,b);
        ViewInjecter injecter1=new ViewInjecter(v1, a,b,c,d),
                injecter2=new ViewInjecter(v2, e,d,c,b);
        new Thread(injecter1).start();
        new Thread(injecter2).start();


        Util.waitUntilTrue(3000, 200, () -> Stream.of(rafts).allMatch(r -> r.leader() != null));
        Stream.of(a,e).forEach(ch -> ((BaseElection)ch.getProtocolStack().findProtocol(electionClass)).stopVotingThread());
        assertNoMoreThanOneLeaderInSameTerm(a,b,c,d,e);
        System.out.printf("\n-- channels after:\n%s\n", print(a,b,c,d,e));
    }

    protected static void assertNoMoreThanOneLeaderInSameTerm(JChannel... channels) {
        Map<Long,Set<Address>> m=new HashMap<>();
        for(JChannel ch :channels) {
            RAFT r=ch.getProtocolStack().findProtocol(RAFT.class);
            long current_term=r.currentTerm();
            Address leader=r.leader();
            m.compute(current_term, (k,v) -> {
                Set<Address> set=v == null? new HashSet<>() : v;
                set.add(leader);
                return set;
            });
        }
        for(Map.Entry<Long,Set<Address>> e: m.entrySet()) {
            Set<Address> v=e.getValue();
            assert v.size() <= 1 : String.format("term %d had more than 1 leader: %s", e.getKey(), v);
        }
    }

    protected static void assertView(View v, JChannel... channels) {
        for(JChannel ch: channels) {
            View ch_v=ch.getView();
            System.out.printf("%s: %s\n", ch.getAddress(), ch.getView());
            assert ch_v.equals(v) : String.format("%s: %s", ch.getAddress(), ch.getView());
        }
    }

    protected static JChannel findCoord(JChannel... channels) {
        for(JChannel ch: channels)
            if(ch.getView().getCoord().equals(ch.getAddress()))
                return ch;
        return null;
    }

    protected static String print(JChannel... channels) {
        return Stream.of(channels).map(ch -> {
            ProtocolStack stack=ch.getProtocolStack();
            RAFT r=stack.findProtocol(RAFT.class);
            return String.format("%s: %s [leader: %s, term: %d]", ch.getAddress(), ch.getView(), r.leader(), r.currentTerm());
        }).collect(Collectors.joining("\n"));
    }

    protected JChannel create(String name) throws Exception {
        BaseElection election=instantiate();
        RAFT raft=new RAFT().members(members).raftId(name)
          .logClass("org.jgroups.protocols.raft.InMemoryLog").logPrefix(name + "-" + CLUSTER);
        REDIRECT client=new REDIRECT();
        //noinspection resource
        JChannel ch=new JChannel(Util.getTestStack(election,raft,client)).name(name);
        NAKACK2 nak=ch.getProtocolStack().findProtocol(NAKACK2.class);
        if(nak != null)
            nak.logDiscardMessages(false);
        GMS gms=ch.getProtocolStack().findProtocol(GMS.class);
        gms.printLocalAddress(false).logViewWarnings(false);
        return ch;
    }

    protected static void injectView(View v, JChannel... channels) {
        for(JChannel ch: channels) {
            GMS gms=ch.getProtocolStack().findProtocol(GMS.class);
            gms.installView(v);
        }
    }

    protected static void close(JChannel... channels) {
        for(JChannel ch: channels) {
            if(ch == null)
                continue;
            close(ch);
        }
    }

    protected static void close(JChannel ch) {
        if(ch == null)
            return;
        RAFT raft=ch.getProtocolStack().findProtocol(RAFT.class);
        try {
            Utils.deleteLog(raft);
        }
        catch(Exception ignored) {}
        Util.close(ch);
    }

    protected static View createView(long id, JChannel... mbrs) {
        List<Address> addrs=Stream.of(mbrs).map(JChannel::getAddress).collect(Collectors.toList());
        return View.create(mbrs[0].getAddress(), id, addrs);
    }

    protected static boolean isLeader(JChannel ch) {
        RAFT raft=ch.getProtocolStack().findProtocol(RAFT.class);
        return ch.getAddress().equals(raft.leader());
    }

    protected static boolean hasLeader(JChannel ch) {
        RAFT raft=ch.getProtocolStack().findProtocol(RAFT.class);
        return raft.leader() != null;
    }

    protected static List<Address> leaders(JChannel... channels) {
        List<Address> leaders=new ArrayList<>(channels.length);
        for(JChannel ch: channels) {
            if(isLeader(ch))
                leaders.add(ch.getAddress());
        }
        return leaders;
    }

    /** If expected is null, then any member can be a leader */
    protected static Address assertLeader(int times, long sleep, Address expected, JChannel... channels) {
        // wait until there is 1 leader
        for(int i=0; i < times; i++) {
            List<Address> leaders=leaders(channels);
            if(!leaders.isEmpty()) {
                int size=leaders.size();
                assert size <= 1;
                Address leader=leaders.get(0);
                System.out.println("leader: " + leader);
                assert expected == null || expected.equals(leader);
                break;
            }

            Util.sleep(sleep);
        }
        List<Address> leaders=leaders(channels);
        assert leaders.size() == 1 : "leaders=" + leaders;
        Address leader=leaders.get(0);
        System.out.println("leader = " + leader);
        return leader;
    }


    protected static class ViewInjecter implements Runnable {
        protected final View           v;
        protected final JChannel[]     channels;

        public ViewInjecter(View v, JChannel ... channels) {
            this.v=v;
            this.channels=channels;
        }

        public void run() {
            injectView(v, channels);
        }
    }

}
