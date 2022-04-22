package org.jgroups.tests.utils;

import org.jgroups.Global;
import org.jgroups.raft.util.ArrayRingBuffer;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class ArrayRingBufferTest {

   public void testShouldEnlargeItWithGaps() {
      ArrayRingBuffer<Integer> rb = new ArrayRingBuffer<>(1);
      rb.set(1, 1);
      Assert.assertEquals(rb.size(), 1);
      Assert.assertEquals(rb.get(1), Integer.valueOf(1));
      rb.set(3, 2);
      Assert.assertEquals(rb.size(), 3);
      Assert.assertEquals(rb.get(1), Integer.valueOf(1));
      Assert.assertNull(rb.get(2));
      Assert.assertEquals(rb.get(3), Integer.valueOf(2));
   }

   public void testShouldEnlargeItWithTwiceWrap() {
      ArrayRingBuffer<Integer> rb = new ArrayRingBuffer<>(6);
      for (int i = 6; i < 10; i++) {
         rb.set(i, i);
      }
      Assert.assertEquals(rb.availableCapacityWithoutResizing(), 0);
      rb.set(10, 10);
      Assert.assertEquals(rb.availableCapacityWithoutResizing(), 3);
      for (int i = 6; i < 11; i++) {
         Assert.assertEquals(rb.get(i), Integer.valueOf(i));
      }
   }

   public void testForEach() {
      ArrayRingBuffer<Integer> rb = new ArrayRingBuffer<>(6);
      for (int i = 6; i <= 10; i++) {
         rb.set(i, i);
      }
      Map<Integer,Long> expected_values=new HashMap<>(5);
      rb.forEach(expected_values::put);
      assert expected_values.size() == 5;
      for(Map.Entry<Integer,Long> e: expected_values.entrySet()) {
         Integer key=e.getKey();
         Long val=e.getValue();
         assert key.intValue() == val.longValue();
      }
   }

   public void testShouldUseAvailableCapacity() {
      ArrayRingBuffer<Integer> rb = new ArrayRingBuffer<>(1);
      rb.set(1, 1);
      rb.set(7, 2);
      Assert.assertEquals(rb.size(), 7);
      rb.set(8, 3);
      Assert.assertEquals(rb.get(1), Integer.valueOf(1));
      Assert.assertEquals(rb.get(7), Integer.valueOf(2));
      Assert.assertEquals(rb.get(8), Integer.valueOf(3));
   }

   public void testShouldCopyOldElementsInTheRightOrder() {
      ArrayRingBuffer<Integer> rb = new ArrayRingBuffer<>(1);
      rb.set(1, 1);
      rb.set(7, 2);
      rb.set(8, 3);
      rb.set(15, 4);
      Assert.assertEquals(rb.get(1), Integer.valueOf(1));
      Assert.assertEquals(rb.get(7), Integer.valueOf(2));
      Assert.assertEquals(rb.get(8), Integer.valueOf(3));
      Assert.assertEquals(rb.get(15), Integer.valueOf(4));
   }

   public void testShouldClearAndIncreaseAvailableSpace() {
      ArrayRingBuffer<Integer> rb = new ArrayRingBuffer<>(1);
      rb.set(1, 1);
      rb.set(7, 2);
      rb.set(8, 3);
      final int before = rb.availableCapacityWithoutResizing();
      rb.dropHeadUntil(5);
      final int after = rb.availableCapacityWithoutResizing();
      Assert.assertEquals(after - before, 4);
      Assert.assertEquals(rb.get(7), Integer.valueOf(2));
      Assert.assertEquals(rb.get(8), Integer.valueOf(3));
      rb.set(12, 4);
      Assert.assertEquals(rb.availableCapacityWithoutResizing(), 0);
      Assert.assertEquals(rb.get(12), Integer.valueOf(4));
   }

   public void testShouldEnlargeCapacityByPowerOfTwo() {
      ArrayRingBuffer<Integer> rb = new ArrayRingBuffer<>(1);
      rb.set(1, 1);
      rb.set(4, 2);
      rb.set(7, 3);
      rb.set(8, 4);
      rb.dropHeadUntil(5);
      rb.set(14, 5);
      Assert.assertEquals(rb.availableCapacityWithoutResizing(), 6);
   }

   public void testShouldClearUptoAlthoughWrapped() {
      ArrayRingBuffer<Integer> rb = new ArrayRingBuffer<>(4);
      for (int i = 4; i < 12; i++) {
         rb.set(i, i);
      }
      rb.dropHeadUntil(11);
      Assert.assertEquals(rb.size(), 1);
      Assert.assertEquals(rb.get(11), Integer.valueOf(11));
   }

   public void testShouldClearFromAlthoughWrapped() {
      ArrayRingBuffer<Integer> rb = new ArrayRingBuffer<>(4);
      for (int i = 4; i < 12; i++) {
         rb.set(i, i);
      }
      rb.dropTailTo(7);
      Assert.assertEquals(rb.size(), 3);
      Assert.assertEquals(rb.get(4), Integer.valueOf(4));
      Assert.assertEquals(rb.get(5), Integer.valueOf(5));
      Assert.assertEquals(rb.get(6), Integer.valueOf(6));
   }

   public void testShouldClearUpToAllWrapped() {
      ArrayRingBuffer<Integer> rb = new ArrayRingBuffer<>(4);
      for (int i = 4; i < 12; i++) {
         rb.set(i, i);
      }
      rb.clear();
      Assert.assertEquals(rb.size(), 0);
   }

   public void testShouldClearFromAllWrapped() {
      ArrayRingBuffer<Integer> rb = new ArrayRingBuffer<>(4);
      for (int i = 4; i < 12; i++) {
         rb.set(i, i);
      }
      rb.dropTailTo(rb.getHeadSequence());
      Assert.assertEquals(rb.size(), 0);
   }

   public void testCannotAccessClearedUpData() {
      ArrayRingBuffer<Integer> rb = new ArrayRingBuffer<>(1);
      rb.set(1, 1);
      rb.set(5, 3);
      rb.set(15, 4);
      rb.dropHeadUntil(5);
      Assert.assertThrows(IllegalArgumentException.class, () -> rb.get(4));
   }

   public void testCreatingBackedArrayOfSpecificSize() {
      Assert.assertEquals(new ArrayRingBuffer<Integer>(8,0).availableCapacityWithoutResizing(), 8);
   }

   public void testAddPeekPollIsEmptySizeConsistency() {
      final int initialHead = 10;
      final int size = 10;
      ArrayRingBuffer<Integer> rb = new ArrayRingBuffer<>(initialHead);
      for (int i = 0; i < size; i++) {
         rb.add(i);
      }
      Assert.assertEquals(rb.size(), size);
      for (int i = 0; i < 10; i++) {
         final Integer expected = Integer.valueOf(i);
         Assert.assertEquals(rb.peek(), expected);
         Assert.assertEquals(rb.poll(), expected);
         Assert.assertEquals(rb.size(), size - (i + 1));
         Assert.assertEquals(rb.getHeadSequence(), initialHead + i + 1);
      }
      Assert.assertTrue(rb.isEmpty());
      Assert.assertNull(rb.peek());
      Assert.assertNull(rb.poll());
      Assert.assertEquals(rb.size(), 0);
   }

}
