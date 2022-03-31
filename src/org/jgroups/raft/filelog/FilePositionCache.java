package org.jgroups.raft.filelog;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * A mapping between the RAFT log index and where the log entry is stored in a file.
 *
 * @author Pedro Ruivo, Francesco Nigro
 * @since 0.5.4
 */
public class FilePositionCache {

   public static final int TOO_OLD = -2;
   public static final int EMPTY = -1;

   private static final int PAGE_CAPACITY = 1 << 10;
   private static final int PAGE_SHIFT = Integer.numberOfTrailingZeros(PAGE_CAPACITY);
   private static final int PAGE_MASK = PAGE_CAPACITY - 1;

   {
      assert Integer.bitCount(PAGE_CAPACITY) == 1 : "PAGE_CAPACITY MUST BE A POWER OF TWO";
   }

   private static final class PositionPage {

      private final long[] positions;
      private int lastNotEmptyOffset;

      public PositionPage() {
         positions = new long[PAGE_CAPACITY];
         Arrays.fill(positions, EMPTY);
         lastNotEmptyOffset = -1;
      }

      public boolean clear() {
         return clearFrom(0);
      }

      public boolean clearFrom(final int pageOffset) {
         if (pageOffset > lastNotEmptyOffset) {
            return false;
         }
         if (pageOffset < 0) {
            throw new IllegalArgumentException("pageOffset must be greater then 0");
         }
         if (pageOffset >= PAGE_CAPACITY) {
            throw new IllegalArgumentException("pageOffset must be less then page capacity");
         }
         assert lastNotEmptyOffset >= 0;
         Arrays.fill(positions, pageOffset, lastNotEmptyOffset + 1, EMPTY);
         lastNotEmptyOffset = -1;
         // must search backward for the first not empty slot, to mark it
         for (int i = pageOffset; i >= 0; i--) {
            if (positions[i] != EMPTY) {
               lastNotEmptyOffset = i;
               break;
            }
         }
         return true;
      }

      public long set(final int pageOffset, final long position) {
         if (position < 0) {
            throw new IllegalArgumentException("position must be greater then zero");
         }
         if (pageOffset >= PAGE_CAPACITY) {
            throw new IllegalArgumentException("The required pageOffset is beyond page capacity");
         }
         final long oldValue = positions[pageOffset];
         if (oldValue == EMPTY) {
            if (pageOffset > lastNotEmptyOffset) {
               lastNotEmptyOffset = pageOffset;
            }
         }
         positions[pageOffset] = position;
         return oldValue;
      }

      public long get(final int pageOffset) {
         if (pageOffset >= PAGE_CAPACITY) {
            throw new IllegalArgumentException("The required pageOffset is beyond page capacity");
         }
         if (pageOffset > lastNotEmptyOffset) {
            return EMPTY;
         }
         return positions[pageOffset];
      }
   }

   private final ArrayList<PositionPage> positionPages;
   private final int firstLogIndex;

   public FilePositionCache(int firstLogIndex) {
      this.positionPages = new ArrayList<>();
      this.firstLogIndex = firstLogIndex;
   }

   public FilePositionCache(int firstLogIndex, int requiredPages) {
      this.positionPages = new ArrayList<>(requiredPages);
      this.firstLogIndex = firstLogIndex;
   }

   public int getFirstLogIndex() {
      return firstLogIndex;
   }

   private static int toPageIndex(final int cacheIndex) {
      return cacheIndex >> PAGE_SHIFT;
   }

   private static int toPageOffset(final int cacheIndex) {
      return cacheIndex & PAGE_MASK;
   }

   public long getPosition(int logIndex) {
      final int cacheIndex = toCacheIndex(logIndex);
      if (cacheIndex < 0) {
         return TOO_OLD;
      }
      final int pageIndex = toPageIndex(cacheIndex);
      if (pageIndex >= positionPages.size()) {
         return EMPTY;
      }
      final PositionPage page = positionPages.get(pageIndex);
      if (page == null) {
         return EMPTY;
      }
      return page.get(toPageOffset(cacheIndex));
   }

   private PositionPage getOrCreatePage(int pageIndex) {
      final int pages = positionPages.size();
      if (pageIndex < pages) {
         final PositionPage page = positionPages.get(pageIndex);
         if (page != null) {
            return page;
         }
         final PositionPage positionPage = new PositionPage();
         positionPages.set(pageIndex, positionPage);
         return positionPage;
      }
      final int requiredCapacity = pageIndex + 1;
      positionPages.ensureCapacity(requiredCapacity);
      for (int i = pages; i < pageIndex; i++) {
         positionPages.add(null);
      }
      final PositionPage positionPage = new PositionPage();
      positionPages.add(positionPage);
      assert positionPages.size() == (pageIndex + 1);
      return positionPage;
   }

   public boolean set(int logIndex, long position) {
      if (position < 0) {
         throw new IllegalArgumentException("position must be greater then zero");
      }
      final int cacheIndex = toCacheIndex(logIndex);
      if (cacheIndex < 0) {
         return false;
      }
      getOrCreatePage(toPageIndex(cacheIndex)).set(toPageOffset(cacheIndex), position);
      return true;
   }

   private int toCacheIndex(int logIndex) {
      return logIndex - firstLogIndex;
   }

   public int getFirstAppended() {
      return firstLogIndex;
   }

   public boolean invalidateFrom(final int logIndex) {
      final int cacheIndex = toCacheIndex(logIndex);
      if (cacheIndex < 0) {
         return false;
      }
      final int pageIndex = toPageIndex(cacheIndex);
      if (pageIndex >= positionPages.size()) {
         return false;
      }
      boolean clearSomething = positionPages.get(pageIndex).clearFrom(toPageOffset(cacheIndex));
      for (int i = (pageIndex + 1); i < positionPages.size(); i++) {
         clearSomething |= positionPages.get(i).clear();
      }
      return clearSomething;
   }

   public FilePositionCache createDeleteCopyFrom(int logIndex) {
      final int cacheIndex = toCacheIndex(logIndex);
      if (cacheIndex < 0) {
         throw new IllegalArgumentException();
      }
      final int pageIndex = toPageIndex(cacheIndex);
      if (pageIndex >= positionPages.size()) {
         throw new IllegalArgumentException();
      }
      final int pageOffset = toPageOffset(cacheIndex);
      final PositionPage positionPage = positionPages.get(pageIndex);
      if (positionPage == null) {
         throw new IllegalArgumentException();
      }
      final long positionToDecrement = positionPage.get(pageOffset);
      if (positionToDecrement == EMPTY) {
         throw new IllegalArgumentException();
      }
      // copy from cacheIndex until lastNotEmptyIndex
      final int oldPages = positionPages.size();
      final FilePositionCache newCache = new FilePositionCache(logIndex, oldPages - pageIndex);
      int newLogIndex = logIndex;
      for (int offset = pageOffset; offset <= positionPage.lastNotEmptyOffset; offset++) {
         final long oldPosition = positionPage.get(offset);
         if (oldPosition != EMPTY) {
            newCache.set(newLogIndex, oldPosition - positionToDecrement);
         }
         newLogIndex++;
      }
      newLogIndex += (PAGE_CAPACITY - (positionPage.lastNotEmptyOffset + 1));
      for (int i = (pageIndex + 1); i < oldPages; i++) {
         final PositionPage page = positionPages.get(i);
         if (page != null) {
            final int lastNotEmptyPageOffset = page.lastNotEmptyOffset;
            if (lastNotEmptyPageOffset >= 0) {
               for (int offset = 0; offset <= lastNotEmptyPageOffset; offset++) {
                  final long oldPosition = page.get(offset);
                  if (oldPosition != EMPTY) {
                     newCache.set(newLogIndex + offset, oldPosition - positionToDecrement);
                  }
               }
            }
         }
         newLogIndex += PAGE_CAPACITY;
      }
      return newCache;
   }
}
