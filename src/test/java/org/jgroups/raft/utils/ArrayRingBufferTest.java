package org.jgroups.raft.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.jgroups.Global;
import org.jgroups.raft.util.ArrayRingBuffer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class ArrayRingBufferTest {

    private static final Logger LOGGER = LogManager.getLogger(ArrayRingBufferTest.class);

    public void testShouldEnlargeItWithGaps() {
        ArrayRingBuffer<Integer> rb = new ArrayRingBuffer<>(1);
        rb.set(1, 1);
        assertThat(rb.size()).isEqualTo(1);
        assertThat(rb.get(1)).isEqualTo(1);
        rb.set(3, 2);
        assertThat(rb.size()).isEqualTo(3);
        assertThat(rb.get(1)).isEqualTo(1);
        assertThat(rb.get(2)).isNull();
        assertThat(rb.get(3)).isEqualTo(2);
    }

    public void testShouldEnlargeItWithTwiceWrap() {
        ArrayRingBuffer<Integer> rb = new ArrayRingBuffer<>(6);
        for (int i = 6; i < 10; i++) {
            rb.set(i, i);
        }
        assertThat(rb.availableCapacityWithoutResizing()).isZero();
        rb.set(10, 10);
        assertThat(rb.availableCapacityWithoutResizing()).isEqualTo(3);
        for (int i = 6; i < 11; i++) {
            assertThat(rb.get(i)).isEqualTo(i);
        }
    }

    public void testForEach() {
        ArrayRingBuffer<Integer> rb = new ArrayRingBuffer<>(6);
        for (int i = 6; i <= 10; i++) {
            rb.set(i, i);
        }
        Map<Integer,Long> expected_values=new HashMap<>(5);
        rb.forEach(expected_values::put);
        assertThat(expected_values).hasSize(5);
        for(Map.Entry<Integer,Long> e: expected_values.entrySet()) {
            Integer key=e.getKey();
            Long val=e.getValue();
            assertThat(key.intValue()).isEqualTo(val.longValue());
        }
    }

    public void testRemove() {
        ArrayRingBuffer<Integer> rb = new ArrayRingBuffer<>(1);
        for(int i=1; i <= 10; i++)
            rb.set(i, i);
        assertThat(rb.size()).isEqualTo(10);
        assertThat(rb.size(false)).isEqualTo(10);

        Integer ret=rb.remove(0);
        assertThat(ret).isNull();

        ret=rb.remove(1); // head
        assertThat(ret).isEqualTo(1);
        assertThat(rb.size()).isEqualTo(9);
        assertThat(rb.size(false)).isEqualTo(9);

        ret=rb.remove(5);
        assertThat(ret).isEqualTo(5);
        assertThat(rb.size(false))
                .withFailMessage("size should be 8 but is %d", rb.size(false))
                .isEqualTo(8);

        rb.set(5, 5);
        assertThat(rb.size(false)).isEqualTo(9);
    }

    public void testShouldUseAvailableCapacity() {
        ArrayRingBuffer<Integer> rb = new ArrayRingBuffer<>(1);
        rb.set(1, 1);
        rb.set(7, 2);
        assertThat(rb.size()).isEqualTo(7);
        rb.set(8, 3);
        assertThat(rb.get(1)).isEqualTo(1);
        assertThat(rb.get(7)).isEqualTo(2);
        assertThat(rb.get(8)).isEqualTo(3);
    }

    public void testShouldCopyOldElementsInTheRightOrder() {
        ArrayRingBuffer<Integer> rb = new ArrayRingBuffer<>(1);
        rb.set(1, 1);
        rb.set(7, 2);
        rb.set(8, 3);
        rb.set(15, 4);
        assertThat(rb.get(1)).isEqualTo(1);
        assertThat(rb.get(7)).isEqualTo(2);
        assertThat(rb.get(8)).isEqualTo(3);
        assertThat(rb.get(15)).isEqualTo(4);
    }

    public void testShouldClearAndIncreaseAvailableSpace() {
        ArrayRingBuffer<Integer> rb = new ArrayRingBuffer<>(1);
        rb.set(1, 1);
        rb.set(7, 2);
        rb.set(8, 3);
        final int before = rb.availableCapacityWithoutResizing();
        rb.dropHeadUntil(5);
        final int after = rb.availableCapacityWithoutResizing();
        assertThat(after - before).isEqualTo(4);
        assertThat(rb.get(7)).isEqualTo(2);
        assertThat(rb.get(8)).isEqualTo(3);
        rb.set(12, 4);
        assertThat(rb.availableCapacityWithoutResizing()).isZero();
        assertThat(rb.get(12)).isEqualTo(4);
    }

    public void testShouldEnlargeCapacityByPowerOfTwo() {
        ArrayRingBuffer<Integer> rb = new ArrayRingBuffer<>(1);
        rb.set(1, 1);
        rb.set(4, 2);
        rb.set(7, 3);
        rb.set(8, 4);
        rb.dropHeadUntil(5);
        rb.set(14, 5);
        assertThat(rb.availableCapacityWithoutResizing()).isEqualTo(6);
    }

    public void testShouldClearUptoAlthoughWrapped() {
        ArrayRingBuffer<Integer> rb = new ArrayRingBuffer<>(4);
        for (int i = 4; i < 12; i++) {
            rb.set(i, i);
        }
        rb.dropHeadUntil(11);
        assertThat(rb.size()).isEqualTo(1);
        assertThat(rb.get(11)).isEqualTo(11);
    }

    public void testShouldClearFromAlthoughWrapped() {
        ArrayRingBuffer<Integer> rb = new ArrayRingBuffer<>(4);
        for (int i = 4; i < 12; i++) {
            rb.set(i, i);
        }
        rb.dropTailTo(7);
        assertThat(rb.size()).isEqualTo(3);
        assertThat(rb.size(false)).isEqualTo(3);
        assertThat(rb.get(4)).isEqualTo(4);
        assertThat(rb.get(5)).isEqualTo(5);
        assertThat(rb.get(6)).isEqualTo(6);
    }

    public void testShouldClearUpToAllWrapped() {
        ArrayRingBuffer<Integer> rb = new ArrayRingBuffer<>(4);
        for (int i = 4; i < 12; i++) {
            rb.set(i, i);
        }
        rb.clear();
        assertThat(rb.size()).isZero();
    }

    public void testShouldClearFromAllWrapped() {
        ArrayRingBuffer<Integer> rb = new ArrayRingBuffer<>(4);
        for (int i = 4; i < 12; i++) {
            rb.set(i, i);
        }
        rb.dropTailTo(rb.getHeadSequence());
        assertThat(rb.size()).isZero();
    }

    public void testDropTailTo() {
        ArrayRingBuffer<Integer> rb = new ArrayRingBuffer<>(1);
        for (int i = 1; i <= 20; i++)
            rb.set(i, i);
        assertThat(rb.size()).isEqualTo(20);
        for(int i=1; i <= 20; i++)
            assertThat(rb.get(i)).isEqualTo(i);

        rb.dropTailTo(5);
        assertThat(rb.size()).isEqualTo(4);

        // index 5 should not be found
        assertThatThrownBy(() -> rb.get(5))
                .isInstanceOf(IllegalArgumentException.class);

        for(int i=1; i <= 4; i++)
            assertThat(rb.get(i)).isEqualTo(i);
    }

    public void testDropHeadUntil() {
        ArrayRingBuffer<Integer> rb = new ArrayRingBuffer<>(1);
        for (int i = 1; i <= 20; i++)
            rb.set(i, i);
        assertThat(rb.size()).isEqualTo(20);

        rb.dropHeadUntil(6);
        assertThat(rb.size()).isEqualTo(15);

        // index 5 should not be found
        assertThatThrownBy(() -> rb.get(5))
                .isInstanceOf(IllegalArgumentException.class);

        for(int i=6; i <= 20; i++) {
            assertThat(rb.get(i)).isEqualTo(i);
        }
    }

    public void testCannotAccessClearedUpData() {
        ArrayRingBuffer<Integer> rb = new ArrayRingBuffer<>(1);
        rb.set(1, 1);
        rb.set(5, 3);
        rb.set(15, 4);
        rb.dropHeadUntil(5);

        assertThatThrownBy(() -> rb.get(4))
                .isInstanceOf(IllegalArgumentException.class);
    }

    public void testCreatingBackedArrayOfSpecificSize() {
        assertThat(new ArrayRingBuffer<Integer>(8,0).availableCapacityWithoutResizing())
                .isEqualTo(8);
    }

    public void testAddPeekPollIsEmptySizeConsistency() {
        final int initialHead = 10;
        final int size = 10;
        ArrayRingBuffer<Integer> rb = new ArrayRingBuffer<>(initialHead);
        for (int i = 0; i < size; i++) {
            rb.add(i);
        }
        assertThat(rb.size()).isEqualTo(size);
        for (int i = 0; i < 10; i++) {
            final Integer expected = Integer.valueOf(i);
            assertThat(rb.peek()).isEqualTo(expected);
            assertThat(rb.poll()).isEqualTo(expected);
            assertThat(rb.size()).isEqualTo(size - (i + 1));
            assertThat(rb.getHeadSequence()).isEqualTo(initialHead + i + 1);
        }
        assertThat(rb.isEmpty()).isTrue();
        assertThat(rb.peek()).isNull();
        assertThat(rb.poll()).isNull();
        assertThat(rb.size()).isZero();
    }

}
