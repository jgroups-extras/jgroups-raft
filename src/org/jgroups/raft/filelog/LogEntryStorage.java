package org.jgroups.raft.filelog;

import static org.jgroups.raft.filelog.FilePositionCache.NO_CAPACITY;
import static org.jgroups.raft.filelog.FilePositionCache.OK;
import static org.jgroups.raft.filelog.FilePositionCache.TOO_OLD;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Objects;
import java.util.concurrent.locks.StampedLock;
import java.util.function.ObjIntConsumer;

import org.jgroups.Global;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.raft.LogEntry;

/**
 * Stores the {@link LogEntry} into a file.
 *
 * @author Pedro Ruivo
 * @since 0.5.4
 */
public class LogEntryStorage extends BaseStorage {

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private static final byte MAGIC_NUMBER = 0x01;
   private static final String FILE_NAME = "entries.raft";
   private static final int HEADER_SIZE = Global.INT_SIZE * 4 + 1;

   private FilePositionCache positionCache;
   private volatile int lastAppended;
   private final StampedLock lock = new StampedLock();

   public LogEntryStorage(File parentDir) {
      super(new File(parentDir, FILE_NAME));
      positionCache = new FilePositionCache(0);
   }

   public void reload() throws IOException {
      long stamp = lock.writeLock();
      try {
         FileChannel channel = checkOpen();
         Header header = readHeader(channel, 0);
         if (header == null) {
            positionCache = new FilePositionCache(0);
            lastAppended = 0;
            return;
         }
         positionCache = new FilePositionCache(header.index == 1 ? 0 : header.index);
         setFilePosition(header);
         lastAppended = header.index;

         long position = header.nextPosition();

         while (true) {
            header = readHeader(channel, position);
            if (header == null) {
               return;
            }
            setFilePosition(header);
            position = header.nextPosition();
            lastAppended = header.index;
         }
      } finally {
         lock.unlockWrite(stamp);
      }
   }

   public int getFirstAppended() {
      long stamp = lock.readLock();
      try {
         return positionCache.getFirstAppended();
      } finally {
         lock.unlockRead(stamp);
      }
   }

   public int getLastAppended() {
      return lastAppended;
   }

   public LogEntry getLogEntry(int index) throws IOException {
      long stamp = lock.readLock();
      try {
         FileChannel channel = checkOpen();
         long position = positionCache.getPosition(index);
         if (position < 0) {
            return null;
         }
         Header header = readHeader(channel, position);
         if (header == null) {
            return null;
         }
         return header.readLogEntry(channel);
      } finally {
         lock.unlockRead(stamp);
      }
   }

   public int write(int startIndex, LogEntry[] entries, final boolean overwrite) throws IOException {
      PositionCheck check = overwrite ?
            PositionCheck.OVERWRITE :
            PositionCheck.PUT_IF_ABSENT;
      FileChannel channel = checkOpen();
      long stamp = lock.writeLock();
      try {
         if (startIndex == 1) {
            return appendLocked(channel, entries, 1, 0, check);
         }
         // find previous entry to append
         long previousPosition = positionCache.getPosition(startIndex - 1);
         if (previousPosition < 0) {
            throw new IllegalStateException();
         }
         Header previous = readHeader(channel, previousPosition);
         if (previous == null) {
            throw new IllegalStateException();
         }
         return appendLocked(channel, entries, startIndex, previous.nextPosition(), check);
      } finally {
         lock.unlockWrite(stamp);
      }
   }

   private void expandCapacity() {
      positionCache = positionCache.expand();
   }

   private void setFilePosition(Header header) {
      setFilePosition(header.index, header.position);
   }

   private void setFilePosition(int index, long position) {
      do {
         switch (positionCache.set(index, position)) {
            case NO_CAPACITY:
               expandCapacity();
               break;
            case TOO_OLD:
               log.warn("Unable to set file position for index " + index + ". LogEntry is too old");
            case OK:
            default:
               return;
         }
      } while (true);
   }

   private int appendLocked(FileChannel channel, LogEntry[] entries, int index, long position, PositionCheck check) throws IOException {
      ByteBuffer buffer = null;
      int term = 0;
      channel.position(position);
      for (LogEntry entry : entries) {
         Header header = new Header(position, index, entry);
         if (check.canWrite(channel, header, entry)) {
            if (buffer == null || buffer.capacity() < header.totalLength) {
               buffer = ByteBuffer.allocate(header.totalLength);
            }
            writeLogEntry(channel, header, entry, buffer);
            term = Math.max(entry.term(), term);
         }
         position = header.nextPosition();
         ++index;
      }

      lastAppended = index - 1;
      positionCache.invalidate(index);
      channel.truncate(position);
      return term;
   }

   private static Header readHeader(FileChannel channel, long position) throws IOException {
      ByteBuffer data = ByteBuffer.allocate(HEADER_SIZE);
      channel.read(data, position);
      data.flip();
      if (data.remaining() != HEADER_SIZE) {
         // corrupted data or non-existing data
         return null;
      }

      return new Header(position, data).consistencyCheck();
   }

   private void writeLogEntry(FileChannel channel, Header header, LogEntry entry, ByteBuffer buffer) throws IOException {
      header.writeTo(buffer);
      buffer.put(entry.command(), entry.offset(), entry.length());
      buffer.flip();
      channel.write(buffer);
      buffer.clear();
      setFilePosition(header);
   }

   public void removeOld(int index) throws IOException {
      long stamp = lock.writeLock();
      try {
         long position = positionCache.getPosition(index);
         truncateUntil(position);
         positionCache = positionCache.deleteFrom(index);
      } finally {
         lock.unlockWrite(stamp);
      }
   }

   public int removeNew(int index) throws IOException {
      long stamp = lock.readLock();
      try {
         FileChannel channel = checkOpen();
         // remove all?
         if (index == 1) {
            channel.truncate(0);
            lastAppended = 0;
            return 0;
         }
         long position = positionCache.getPosition(index - 1);
         Header previousHeader = readHeader(channel, position);
         if (previousHeader == null) {
            throw new IllegalStateException();
         }
         channel.truncate(previousHeader.nextPosition());
         positionCache.invalidate(index);
         lastAppended = index - 1;
         return previousHeader.term;
      } finally {
         lock.unlockRead(stamp);
      }
   }

   public void forEach(ObjIntConsumer<LogEntry> consumer, int startIndex, int endIndex) throws IOException {
      startIndex = Math.max(Math.max(startIndex, getFirstAppended()), 1);
      long stamp = lock.readLock();
      try {
         long position = positionCache.getPosition(startIndex);
         if (position < 0) {
            return;
         }
         FileChannel channel = checkOpen();
         while (startIndex <= endIndex) {
            Header header = readHeader(channel, position);
            if (header == null) {
               return;
            }
            consumer.accept(header.readLogEntry(channel), startIndex);
            position = header.nextPosition();
            ++startIndex;
         }
      } finally {
         lock.unlockRead(stamp);
      }
   }

   private enum PositionCheck {
      OVERWRITE {
         @Override
         public boolean canWrite(FileChannel channel, Header header, LogEntry entry) {
            return true;
         }
      },
      PUT_IF_ABSENT {
         @Override
         public boolean canWrite(FileChannel channel, Header header, LogEntry entry) throws IOException {
            Header existing = readHeader(channel, header.position);
            if (existing == null) {
               return true;
            }
            if (existing.equals(header)) { // same entry, skip overwriting
               return false;
            }
            throw new IllegalStateException();
         }
      };

      abstract boolean canWrite(FileChannel channel, Header header, LogEntry entry) throws IOException;
   }

   private static class Header {
      final long position;
      // magic is here in case we need to change the format!
      final byte magic;
      final int totalLength;
      final int term;
      final int index;
      final int dataLength;

      Header(long position, int index, LogEntry entry) {
         Objects.requireNonNull(entry);
         this.position = position;
         this.magic = MAGIC_NUMBER;
         this.index = index;
         this.term = entry.term();
         this.dataLength = entry.length();
         this.totalLength = HEADER_SIZE + dataLength;
      }

      Header(long position, ByteBuffer buffer) {
         this.position = position;
         this.magic = buffer.get();
         this.totalLength = buffer.getInt();
         this.term = buffer.getInt();
         this.index = buffer.getInt();
         this.dataLength = buffer.getInt();
      }

      public void writeTo(ByteBuffer buffer) {
         buffer.put(magic);
         buffer.putInt(totalLength);
         buffer.putInt(term);
         buffer.putInt(index);
         buffer.putInt(dataLength);
      }

      long nextPosition() {
         return HEADER_SIZE + position + dataLength;
      }

      Header consistencyCheck() {
         return magic != MAGIC_NUMBER ||
               term <= 0 ||
               index <= 0 ||
               dataLength < 0 ||
               totalLength < HEADER_SIZE ||
               dataLength + HEADER_SIZE != totalLength ?
               null : this;
      }

      LogEntry readLogEntry(FileChannel channel) throws IOException {
         ByteBuffer data = ByteBuffer.allocate(dataLength);
         channel.read(data, position + HEADER_SIZE);
         data.flip();
         if (data.remaining() != dataLength) {
            return null;
         }
         if (data.hasArray()) {
            return new LogEntry(term, data.array(), data.arrayOffset(), dataLength);
         }
         byte[] bytes = new byte[dataLength];
         data.get(bytes);
         return new LogEntry(term, bytes);
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;
         Header header = (Header) o;
         return position == header.position &&
               magic == header.magic &&
               totalLength == header.totalLength &&
               term == header.term &&
               index == header.index &&
               dataLength == header.dataLength;
      }

      @Override
      public int hashCode() {
         return Objects.hash(position, magic, totalLength, term, index, dataLength);
      }
   }
}
