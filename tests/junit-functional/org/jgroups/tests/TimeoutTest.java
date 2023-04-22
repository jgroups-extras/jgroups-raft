package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.raft.ELECTION;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.REDIRECT;
import org.jgroups.raft.blocks.ReplicatedStateMachine;
import org.jgroups.raft.util.Utils;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class TimeoutTest {
    protected static final int                          NUM=200;
    protected JChannel[]                                channels;
    protected ReplicatedStateMachine<Integer,Integer>[] rsms;


    @AfterMethod protected void destroy() throws Exception {
        for(JChannel ch: channels) {
            RAFT raft=ch.getProtocolStack().findProtocol(RAFT.class);
            Utils.deleteLog(raft);
        }
        Util.close(channels);
    }

    public void testAppendWithSingleNode() throws Exception {
        _test(1);
    }

    public void testAppendWith3Nodes() throws Exception {
        _test(3);
    }

    protected void _test(int num) throws Exception {
        channels=createChannels(num);
        rsms=createReplicatedStateMachines(channels);
        for(JChannel ch: channels)
            ch.connect("rsm-cluster");
        Util.waitUntilAllChannelsHaveSameView(10000, 500, channels);

        Util.waitUntil(10000, 500,
                       () -> Stream.of(channels)
                         .map(ch -> ch.getProtocolStack().findProtocol(RAFT.class))
                         .anyMatch(r -> ((RAFT)r).isLeader()));
        ReplicatedStateMachine<Integer,Integer> sm=null;
        System.out.println("-- waiting for leader");
        for(int i=0; i < channels.length; i++) {
            RAFT raft=channels[i].getProtocolStack().findProtocol(RAFT.class);
            if(raft.isLeader()) {
                sm=rsms[i];
                System.out.printf("-- found leader: %s\n", raft.leader());
                break;
            }
        }

        assert sm != null : "No leader found";
        for(int i=1; i <= NUM; i++) {
            try {
                sm.put(i, i);
                System.out.println(i);
                Thread.sleep(10);
            }
            catch(Exception ex) {
                System.err.printf("put(%d): last-applied=%d, commit-index=%d\n", i, sm.lastApplied(), sm.commitIndex());
                throw ex;
            }
        }

        long start=System.currentTimeMillis();
        assert sm.allowDirtyReads(false).get(NUM) == NUM;
        Predicate<ReplicatedStateMachine<Integer, Integer>> converged=r -> {
            try {
                return r.get(NUM)  == NUM;
            } catch (Throwable t) {
                t.printStackTrace();
                return false;
            }
        };
        // After reading correctly from the leader with a quorum read, every node should have the same state.
        for (ReplicatedStateMachine<Integer, Integer> rsm : rsms) {
            assert converged.test(rsm.allowDirtyReads(true));
        }
        long time=System.currentTimeMillis()-start;
        System.out.printf("-- it took %d member(s) %d ms to get consistent caches\n", rsms.length, time);

        System.out.printf("-- contents:\n%s\n\n",
                          Stream.of(rsms).map(r -> String.format("%s: %s", r.channel().getName(), r))
                            .collect(Collectors.joining("\n")));

        System.out.print("-- verifying contents of state machines:\n");
        for(ReplicatedStateMachine<Integer,Integer> rsm: rsms) {
            System.out.printf("%s: ", rsm.channel().getName());
            for(int i=1; i <= NUM; i++)
                assert rsm.get(i) == i;
            System.out.println("OK");
        }
    }

    protected static JChannel createRaftChannel(List<String> members, String name) throws Exception {
        Protocol[] protocols=Util.getTestStack(
          new ELECTION(),
          new RAFT().members(members).raftId(name).resendInterval(1000).logClass("org.jgroups.protocols.raft.InMemoryLog"),
          new REDIRECT());
        return new JChannel(protocols).name(name);
    }

    protected static JChannel[] createChannels(int num) throws Exception {
        List<String> members=IntStream.range(0, num)
          .mapToObj(n -> String.valueOf((char)('A' + n))).collect(Collectors.toList());

        JChannel[] ret=new JChannel[num];
        for(int i=0; i < num; i++) {
            ret[i]=createRaftChannel(members, String.valueOf((char)('A' + i)));
        }
        return ret;
    }

    protected static ReplicatedStateMachine<Integer,Integer>[] createReplicatedStateMachines(JChannel[] chs) {
        ReplicatedStateMachine<Integer,Integer>[] ret=new ReplicatedStateMachine[chs.length];
        for(int i=0; i < ret.length; i++) {
            ret[i]=new ReplicatedStateMachine<>(chs[i]);
            ret[i].timeout(2000).allowDirtyReads(true);
        }
        return ret;
    }
}
