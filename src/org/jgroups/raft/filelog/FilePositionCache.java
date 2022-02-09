package org.jgroups.raft.filelog;

import java.util.Arrays;

/**
 * A mapping between the RAFT log index and where the log entry is stored in a file.
 *
 * @author Pedro Ruivo
 * @since 0.5.4
 */
public class FilePositionCache {

   public static final int TOO_OLD = -1;
   public static final int NO_CAPACITY = -2;
   public static final int OK = 1;
   private static final int SIZE_INCREMENT = 128;

   private final long[] position;
   private final int firstLogIndex;

   public FilePositionCache(int firstLogIndex) {
      this.position = new long[SIZE_INCREMENT];
      this.firstLogIndex = firstLogIndex;
      Arrays.fill(position, -1);
   }

   private FilePositionCache(int firstLogIndex, long[] position) {
      this.position = position;
      this.firstLogIndex = firstLogIndex;
   }

   public long getPosition(int index) {
      int arrayIndex = toArrayIndex(index);
      if (arrayIndex < 0) {
         return TOO_OLD;
      }
      return arrayIndex < position.length ? position[arrayIndex] : NO_CAPACITY;
   }

   public int set(int logIndex, long position) {
      int arrayIndex = toArrayIndex(logIndex);
      if (arrayIndex < 0) {
         return TOO_OLD;
      }
      if (arrayIndex >= this.position.length) {
         return NO_CAPACITY;
      }
      this.position[arrayIndex] = position;
      return OK;
   }


   public FilePositionCache expand() {
      long[] pos = Arrays.copyOf(position, position.length + SIZE_INCREMENT);
      Arrays.fill(pos, position.length, pos.length, -1);
      return new FilePositionCache(firstLogIndex, pos);
   }

   private int toArrayIndex(int logIndex) {
      return logIndex - firstLogIndex;
   }

   public int getFirstAppended() {
      return firstLogIndex;
   }

   public void invalidate(int index) {
      int arrayIndex = toArrayIndex(index);
      if (arrayIndex < 0 || arrayIndex >= position.length) {
         return;
      }
      Arrays.fill(position, toArrayIndex(index), position.length, -1);
   }

   public FilePositionCache deleteFrom(int index) {
      int arrayIndex = toArrayIndex(index);
      if (arrayIndex < 0 || arrayIndex >= position.length) {
         throw new IllegalArgumentException();
      }
      long positionToDecrement = position[arrayIndex];
      long[] pos = new long[position.length - arrayIndex];
      for (int i = 0; i < pos.length; ++i) {
         pos[i] = Math.max(position[i + arrayIndex] - positionToDecrement, -1);
      }
      return new FilePositionCache(index, pos);
   }
}
