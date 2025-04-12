package org.jgroups.protocols.raft;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.raft.testfwk.RaftTestUtils;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.tests.harness.BaseRaftElectionTest;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests leader election on a merge (https://github.com/belaban/jgroups-raft/issues/125). Verifies that there can be
 * at most 1 leader during concurrent election rounds run by different coordinators.
 * @author Bela Ban
 * @since  1.0.10
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true, dataProvider = BaseRaftElectionTest.ALL_ELECTION_CLASSES_PROVIDER)
public class MergeTest extends BaseRaftElectionTest.ChannelBased {
    protected JChannel            a,b,c,d,e;

    {
        // We create nodes `A` to `E`, strip down and setup per method.
        clusterSize = 5;
        recreatePerMethod = true;
    }

    @Override
    protected void afterClusterCreation() {
        a = channel(0);
        b = channel(1);
        c = channel(2);
        d = channel(3);
        e = channel(4);

        waitUntilLeaderElected(10_000, 0, 1, 2, 3, 4);
    }

    /**
     * Members {A,B,C,D,E} are split into {A,B,C,D} and {E,D,C,B}. A and E are coordinators and start leader elections;
     * but only one of them must become leader. Because members B, C and D are part of _both_ subgroups, they can
     * potentially vote for both A and E, leading to 2 leaders.<br/>
     * The fix implemented in https://github.com/belaban/jgroups-raft/issues/125 will ensure that members only vote
     * for A _or_ E, but not both, in a given term.
     */
    //@Test(invocationCount=10)
    public void testMerge(Class<?> ignore) {
        long id=a.getView().getViewId().getId() +1;
        View v1=createView(id, a, b), v2=createView(id, c, d), v3=createView(id, e);

        // Create views {A,B}, {C,D} and {E}: no leaders
        injectView(v1, a, b);
        injectView(v2, c, d);
        injectView(v3, e);
        assertView(v1, a, b);
        assertView(v2, c, d);
        assertView(v3, e);

        List<Address> leaders=leaders().stream().map(RAFT::getAddress).collect(Collectors.toList());
        assert leaders.isEmpty() : dumpLeaderAndTerms();

        System.out.printf("-- channels before:\n%s\n", print(a,b,c,d,e));

        // now create overlapping views {A,B,C,D} and {E,D,C,B}
        v1=createView(++id, a,b,c,d);
        v2=createView(++id, e,d,c,b);
        ViewInjecter injecter1=new ViewInjecter(v1, a,b,c,d),
                injecter2=new ViewInjecter(v2, e,d,c,b);
        new Thread(injecter1).start();
        new Thread(injecter2).start();

        System.out.println("-- waiting election after merge");
        // After the merge, either 'A' or 'E' will never have a leader.
        // We stop waiting after one node sees itself as leader.
        BooleanSupplier bs = () -> Arrays.stream(channels())
                .map(this::raft)
                .anyMatch(RAFT::isLeader);
        assertThat(RaftTestUtils.eventually(bs, 5_000, TimeUnit.MILLISECONDS))
                .as("Waiting leader after merge")
                .isTrue();
        stopVotingThread();
        assertNoMoreThanOneLeaderInSameTerm(a,b,c,d,e);
        System.out.printf("\n-- channels after:\n%s\n", print(a,b,c,d,e));
    }

    protected void assertNoMoreThanOneLeaderInSameTerm(JChannel... channels) {
        Map<Long,Set<Address>> m=new HashMap<>();
        for(JChannel ch :channels) {
            RAFT r=raft(ch);
            long current_term=r.currentTerm();
            Address leader=r.leader();

            // A node can still have a null leader. Meaning it didn't identify the new leader for a term.
            if (leader == null) continue;

            m.compute(current_term, (k,v) -> {
                Set<Address> set=v == null? new HashSet<>() : v;
                set.add(leader);
                return set;
            });
        }
        for(Map.Entry<Long,Set<Address>> e: m.entrySet()) {
            Set<Address> v=e.getValue();
            assert v.size() == 1 : String.format("term %d had more than 1 leader: %s", e.getKey(), v);
        }
    }

    protected static void assertView(View v, JChannel... channels) {
        for(JChannel ch: channels) {
            View ch_v=ch.getView();
            System.out.printf("%s: %s\n", ch.getAddress(), ch.getView());
            assert ch_v.equals(v) : String.format("%s: %s", ch.getAddress(), ch.getView());
        }
    }

    protected static String print(JChannel... channels) {
        return Stream.of(channels).map(ch -> {
            ProtocolStack stack=ch.getProtocolStack();
            RAFT r=stack.findProtocol(RAFT.class);
            return String.format("%s: %s [leader: %s, term: %d]", ch.getAddress(), ch.getView(), r.leader(), r.currentTerm());
        }).collect(Collectors.joining("\n"));
    }

    @Override
    protected void beforeChannelConnection(JChannel ch) throws Exception {
        NAKACK2 nak=ch.getProtocolStack().findProtocol(NAKACK2.class);
        if(nak != null)
            nak.logDiscardMessages(false);
        GMS gms=ch.getProtocolStack().findProtocol(GMS.class);
        gms.printLocalAddress(false).logViewWarnings(false);
    }

    protected static void injectView(View v, JChannel... channels) {
        for(JChannel ch: channels) {
            GMS gms=ch.getProtocolStack().findProtocol(GMS.class);
            gms.installView(v);
        }
    }

    protected static View createView(long id, JChannel... mbrs) {
        List<Address> addrs=Stream.of(mbrs).map(JChannel::getAddress).collect(Collectors.toList());
        return View.create(mbrs[0].getAddress(), id, addrs);
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
