package org.jgroups.raft.util;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.ObjLongConsumer;

/**
 * It's a growable ring buffer that allows to move tail/head sequences, clear, append, set/replace at specific positions.
 *
 * @author Francesco Nigro
 */
public final class ArrayRingBuffer<T> {

   private static final Object[] EMPTY = new Object[0];
   // it points to the next slot after the last element
   private long tailSequence;
   // it points to the slot of the first element
   private long headSequence;
   private T[] elements;

   public ArrayRingBuffer() {
      this(0, 0);
   }

   public ArrayRingBuffer(final long headSequence) {
      this(0, headSequence);
   }

   @SuppressWarnings("unchecked")
   public ArrayRingBuffer(final int initialSize, final long headSequence) {
      this.elements = (T[]) (initialSize == 0 ? EMPTY : new Object[initialSize]);
      this.headSequence = headSequence;
      this.tailSequence = headSequence;
      if (headSequence < 0) {
         throw new IllegalArgumentException("headSequence cannot be negative");
      }
   }

   public int size() {
      return (int) (tailSequence - headSequence);
   }

   public long getTailSequence() {
      return tailSequence;
   }

   public long getHeadSequence() {
      return headSequence;
   }

   public void forEach(ObjLongConsumer<? super T> consumer) {
      final int size = size();
      final T[] elements = this.elements;
      long sequence = headSequence;
      for (int i = 0; i < size; i++) {
         final T e = elements[bufferOffset(sequence)];
         sequence++;
         if (e == null) {
            continue;
         }
         consumer.accept(e, sequence);
      }
   }

   public int availableCapacityWithoutResizing() {
      return elements.length - size();
   }

   private void validateIndex(final long index) {
      validateIndex(index, true);
   }

   private void validateIndex(final long index, final boolean validateTail) {
      if (index < 0) {
         throw new IllegalArgumentException("index cannot be negative");
      }
      if (validateTail && index >= tailSequence) {
         throw new IllegalArgumentException("index cannot be greater then " + tailSequence);
      }
      if (index < headSequence) {
         throw new IllegalArgumentException("index cannot be less then " + headSequence);
      }
   }

   public boolean contains(final long index) {
      if (index < tailSequence && index >= headSequence) {
         return true;
      }
      return false;
   }

   public void dropTailToHead() {
      dropTailTo(getHeadSequence());
   }

   public void clear() {
      dropHeadUntil(getTailSequence());
   }

   /**
    * Remove all elements from {@code tailSequence} back to {@code indexInclusive}.<br>
    * At the end of this operation {@code tailSequence} is equals to {@code indexInclusive}.
    *
    * <pre>
    * eg:
    * elements = [A, B, C]
    * tail = 3
    * head = 0
    *
    * dropTailTo(1)
    *
    * elements = [A]
    * tail = 1
    * head = 0
    * </pre>
    */
   public int dropTailTo(final long indexInclusive) {
      if (size() == 0 || !contains(indexInclusive)) {
         return 0;
      }
      final int fromIncluded = bufferOffset(indexInclusive);
      final int toExcluded = bufferOffset(tailSequence);
      final T[] elements = this.elements;
      final int clearFrom;
      int removed = 0;
      if (toExcluded <= fromIncluded) {
         Arrays.fill(elements, fromIncluded, elements.length, null);
         removed += elements.length - fromIncluded;
         clearFrom = 0;
      } else {
         clearFrom = fromIncluded;
      }
      Arrays.fill(elements, clearFrom, toExcluded, null);
      removed += (toExcluded - clearFrom);
      tailSequence = indexInclusive;
      return removed;
   }

   /**
    * Remove all elements from {@code headSequence} to {@code indexExclusive}.<br>
    * At the end of this operation {@code headSequence} is equals to {@code indexExclusive}.
    *
    * <pre>
    * eg:
    * elements = [A, B, C]
    * tail = 3
    * head = 0
    *
    * dropHeadUntil(1)
    *
    * elements = [B, C]
    * tail = 3
    * head = 1
    * </pre>
    */
   public int dropHeadUntil(final long indexExclusive) {
      final long indexInclusive = indexExclusive - 1;
      if (size() == 0 || !contains(indexInclusive)) {
         return 0;
      }
      final int fromInclusive = bufferOffset(headSequence);
      final int toExclusive = bufferOffset(indexExclusive);
      final T[] elements = this.elements;
      final int clearFrom;
      int removed = 0;
      if (toExclusive <= fromInclusive) {
         Arrays.fill(elements, fromInclusive, elements.length, null);
         removed += (elements.length - fromInclusive);
         clearFrom = 0;
      } else {
         clearFrom = fromInclusive;
      }
      Arrays.fill(elements, clearFrom, toExclusive, null);
      removed += (toExclusive - clearFrom);
      headSequence = indexExclusive;
      return removed;
   }

   private int bufferOffset(final long index) {
      return bufferOffset(index, elements.length);
   }

   private static int bufferOffset(final long index, final int elementsLength) {
      return (int) (index & (elementsLength - 1));
   }

   public T get(final long index) {
      validateIndex(index);
      return elements[bufferOffset(index)];
   }

   private T replace(final long index, final T e) {
      final int offset = bufferOffset(index);
      final T oldValue = elements[offset];
      elements[offset] = e;
      return oldValue;
   }

   public T set(final long index, final T e) {
      Objects.requireNonNull(e);
      validateIndex(index, false);
      if (index < tailSequence) {
         return replace(index, e);
      }
      final int requiredCapacity = (int) (index - tailSequence) + 1;
      final int missingCapacity = requiredCapacity - availableCapacityWithoutResizing();
      if (missingCapacity > 0) {
         growCapacity(missingCapacity);
      }
      elements[bufferOffset(index)] = e;
      if (index >= tailSequence) {
         tailSequence = index + 1;
      }
      return null;
   }

   public void add(final T e) {
      Objects.requireNonNull(e);
      if (availableCapacityWithoutResizing() == 0) {
         growCapacity(1);
      }
      final long index = tailSequence;
      elements[bufferOffset(index)] = e;
      tailSequence = index + 1;
   }

   public boolean isEmpty() {
      return tailSequence == headSequence;
   }

   public T peek() {
      if (isEmpty()) {
         return null;
      }
      return get(headSequence);
   }

   public T poll() {
      if (isEmpty()) {
         return null;
      }
      final T[] elements = this.elements;
      final int offset = bufferOffset(headSequence);
      final T e = elements[offset];
      elements[offset] = null;
      headSequence++;
      return e;
   }

   private void growCapacity(int delta) {
      assert delta > 0;
      final T[] oldElements = this.elements;
      final int newCapacity = findNextPositivePowerOfTwo(oldElements.length + delta);
      if (newCapacity < 0) {
         // see ArrayList::newCapacity
         throw new OutOfMemoryError();
      }
      @SuppressWarnings("unchecked")
      final T[] newElements = (T[]) new Object[newCapacity];
      final int size = size();
      final long headSequence = this.headSequence;
      long oldIndex = headSequence;
      long newIndex = headSequence;
      int remaining = size;
      while (remaining > 0) {
         final int fromOldIndex = bufferOffset(oldIndex, oldElements.length);
         final int fromNewIndex = bufferOffset(newIndex, newCapacity);
         final int toOldEnd = oldElements.length - fromOldIndex;
         final int toNewEnd = newElements.length - fromNewIndex;
         final int bytesToCopy = Math.min(Math.min(remaining, toOldEnd), toNewEnd);
         System.arraycopy(oldElements, fromOldIndex, newElements, fromNewIndex, bytesToCopy);
         oldIndex += bytesToCopy;
         newIndex += bytesToCopy;
         remaining -= bytesToCopy;
      }
      this.elements = newElements;
   }

   private static int findNextPositivePowerOfTwo(final int value) {
      return 1 << (Integer.SIZE - Integer.numberOfLeadingZeros(value - 1));
   }
}
