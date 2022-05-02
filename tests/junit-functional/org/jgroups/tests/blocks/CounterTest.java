package org.jgroups.tests.blocks;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.blocks.atomic.AsyncCounter;
import org.jgroups.blocks.atomic.SyncCounter;
import org.jgroups.protocols.raft.ELECTION;
import org.jgroups.protocols.raft.FileBasedLog;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.REDIRECT;
import org.jgroups.raft.blocks.CounterService;
import org.jgroups.raft.util.Utils;
import org.jgroups.util.CompletableFutures;
import org.jgroups.util.Util;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * {@link  AsyncCounter} and {@link  SyncCounter} test.
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class CounterTest {

   private static final String CLUSTER = "_counter_test_";

   protected JChannel a, b, c;
   protected CounterService service_a, service_b, service_c;

   @AfterClass(alwaysRun = true)
   public void afterMethod() {
      for (JChannel ch : Arrays.asList(c, b, a)) {
         RAFT raft = ch.getProtocolStack().findProtocol(RAFT.class);
         try {
            Utils.deleteLogAndSnapshot(raft);
         } catch (Exception ignored) {
         }
         Util.close(ch);
      }
   }

   @BeforeClass(alwaysRun = true)
   public void init() throws Exception {
      List<String> members = Arrays.asList("a", "b", "c");

      a = createChannel(0, members).connect(CLUSTER);
      b = createChannel(1, members).connect(CLUSTER);
      c = createChannel(2, members).connect(CLUSTER);

      Util.waitUntilAllChannelsHaveSameView(1000, 500, a, b, c);

      service_a = new CounterService(a).allowDirtyReads(false);
      service_b = new CounterService(b).allowDirtyReads(false);
      service_c = new CounterService(c).allowDirtyReads(false);
   }

   public void testIncrement() {
      List<SyncCounter> counters = createCounters("increment");

      assertEquals(1, counters.get(0).incrementAndGet());
      assertValues(counters, 1);

      assertEquals(6, counters.get(1).addAndGet(5));
      assertValues(counters, 6);

      assertEquals(11, counters.get(2).addAndGet(5));
      assertValues(counters, 11);
   }

   public void testDecrement() {
      List<SyncCounter> counters = createCounters("decrement");

      assertEquals(-1, counters.get(0).decrementAndGet());
      assertValues(counters, -1);

      assertEquals(-8, counters.get(1).addAndGet(-7));
      assertValues(counters, -8);

      assertEquals(-9, counters.get(2).decrementAndGet());
      assertValues(counters, -9);
   }

   public void testSet() {
      List<SyncCounter> counters = createCounters("set");

      counters.get(0).set(10);
      assertValues(counters, 10);

      counters.get(1).set(15);
      assertValues(counters, 15);

      counters.get(2).set(-10);
      assertValues(counters, -10);
   }

   public void testCompareAndSet() {
      List<SyncCounter> counters = createCounters("casb");

      assertTrue(counters.get(0).compareAndSet(0, 2));
      assertValues(counters, 2);

      assertFalse(counters.get(1).compareAndSet(0, 3));
      assertValues(counters, 2);

      assertTrue(counters.get(2).compareAndSet(2, -2));
      assertValues(counters, -2);
   }

   public void testCompareAndSwap() {
      List<SyncCounter> counters = createCounters("casl");

      assertEquals(0, counters.get(0).compareAndSwap(0, 2));
      assertValues(counters, 2);

      assertEquals(2, counters.get(1).compareAndSwap(0, 3));
      assertValues(counters, 2);

      assertEquals(2, counters.get(2).compareAndSwap(2, -2));
      assertValues(counters, -2);
   }

   private static void assertValues(List<SyncCounter> counters, long expectedValue) {
      for (SyncCounter counter : counters) {
         assertEquals(expectedValue, counter.get());
      }
   }

   private List<SyncCounter> createCounters(String name) {
      return Stream.of(service_a, service_b, service_c)
            .map(counterService -> createCounter(name, counterService))
            .collect(Collectors.toList());
   }

   private static SyncCounter createCounter(String name, CounterService counterService) {
      return CompletableFutures.join(counterService.getOrCreateAsyncCounter(name, 0)).sync();
   }


   private static JChannel createChannel(int id, final List<String> members) throws Exception {
      String name = members.get(id);
      ELECTION election = new ELECTION();
      RAFT raft = new RAFT().members(members).raftId(members.get(id)).logClass(FileBasedLog.class.getCanonicalName()).logPrefix(name + "-" + CLUSTER);
      //noinspection resource
      return new JChannel(Util.getTestStack(election, raft, new REDIRECT())).name(name);
   }


}
