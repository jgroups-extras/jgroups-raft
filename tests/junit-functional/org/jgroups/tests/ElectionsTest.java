 package org.jgroups.tests;

 import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.raft.*;
import org.jgroups.protocols.raft.election.BaseElection;
import org.jgroups.raft.util.Utils;
import org.jgroups.tests.election.BaseElectionTest;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static org.jgroups.tests.election.BaseElectionTest.ALL_ELECTION_CLASSES_PROVIDER;

 /**
  * Tests elections
  * @author Bela Ban
  * @since  0.2
  */
 @Test(groups=Global.FUNCTIONAL,singleThreaded=true, dataProvider=ALL_ELECTION_CLASSES_PROVIDER)
 public class ElectionsTest extends BaseElectionTest {
     protected JChannel            a,b,c;
     protected static final String CLUSTER="ElectionsTest";
     protected final List<String>  members=Arrays.asList("A", "B", "C");
     protected static final byte[] BUF={};

     @BeforeMethod protected void init() throws Exception {
         a=create("A"); a.connect(CLUSTER);
         b=create("B"); b.connect(CLUSTER);
         c=create("C"); c.connect(CLUSTER);
         Util.waitUntilAllChannelsHaveSameView(10000, 500, a,b,c);
     }

     @AfterMethod protected void destroy() {
         close(c, b, a);
     }


     /** All members have the same (initial) logs, so any member can be elected as leader */
     public void testSimpleElection(Class<?> ignore) throws Exception {
         assertLeader(20, 500, null, a,b,c);
     }


     /** B and C have longer logs than A: one of {B,C} must become coordinator, but *not* A */
     public void testElectionWithLongLog(Class<?> ignore) throws Exception {
         setLog(b, 1,1,2);
         setLog(c, 1,1,2);

         JChannel coord=findCoord(a,b,c);
         System.out.printf("\n\n-- starting the voting process on %s:\n", coord.getAddress());
         BaseElection el=coord.getProtocolStack().findProtocol(electionClass);
         el.startVotingThread();
         Util.waitUntilTrue(5000, 500, () -> !el.isVotingThreadRunning());

         Address leader=assertLeader(20, 500, null, a, b, c);
         assert leader.equals(b.getAddress()) || leader.equals(c.getAddress());
         assert !leader.equals(a.getAddress());
     }

     /** ELECTION should look for RAFT or its subclasses */
     public void testRAFTSubclass(Class<?> ignore) throws Exception {
         close(c);
         c=createWithRAFTSubclass("C");
         c.connect(CLUSTER);
     }


     protected static JChannel findCoord(JChannel... channels) {
         for(JChannel ch: channels)
             if(ch.getView().getCoord().equals(ch.getAddress()))
                 return ch;
         return null;
     }

     protected JChannel createWithRAFTSubclass(String name) throws Exception {
         return create(name, () -> new RAFT(){});
     }

     protected JChannel create(String name) throws Exception {
         return create(name, RAFT::new);
     }

     protected JChannel create(String name, Supplier<RAFT> raftSupplier) throws Exception {
         BaseElection election=instantiate();
         RAFT raft=raftSupplier.get().members(members).raftId(name)
           .logClass("org.jgroups.protocols.raft.InMemoryLog").logPrefix(name + "-" + CLUSTER);
         REDIRECT client=new REDIRECT();
         return new JChannel(Util.getTestStack(election, raft, client)).name(name);
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

     protected static void setLog(JChannel ch, int... terms) {
         RAFT raft=ch.getProtocolStack().findProtocol(RAFT.class);
         Log log=raft.log();
         long index=log.lastAppended();
         LogEntries le=new LogEntries();
         for(int term: terms)
             le.add(new LogEntry(term, BUF));
         log.append(index+1, le);
     }


     protected static boolean isLeader(JChannel ch) {
         RAFT raft=ch.getProtocolStack().findProtocol(RAFT.class);
         return ch.getAddress().equals(raft.leader());
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


 }
