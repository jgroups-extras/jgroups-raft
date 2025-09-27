package org.jgroups.raft.filelog;

import org.jgroups.Global;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.raft.LogEntries;
import org.jgroups.protocols.raft.LogEntry;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.function.ObjLongConsumer;

/**
 * Stores the {@link LogEntry} into a file.
 *
 * @author Pedro Ruivo
 * @since 0.5.4
 */
public class LogEntryStorage {

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private static final byte MAGIC_NUMBER = 0x01;
   private static final String FILE_NAME = "entries.raft";
   private static final int HEADER_SIZE = Global.INT_SIZE * 2 + Global.LONG_SIZE * 2 + 1 + Global.BYTE_SIZE;
   // this is the typical OS page size and SSD blck_size
   private static final int DEFAULT_WRITE_AHEAD_BYTES = 4096;

   private final FileStorage fileStorage;
   private FilePositionCache positionCache;
   private Header lastAppendedHeader;
   private long lastAppended;
   private boolean fsync;

   public LogEntryStorage(File parentDir, boolean fsync) {
      super();
      this.fsync = fsync;
      positionCache = new FilePositionCache(0);
      fileStorage = new FileStorage(new File(parentDir, FILE_NAME), DEFAULT_WRITE_AHEAD_BYTES);
   }

   public void open() throws IOException {
      fileStorage.open();
   }

   public void close() throws IOException {
      fileStorage.close();
   }

   public void delete() throws IOException {
      fileStorage.delete();
   }

   public void reload() throws IOException {
      Header header = readHeader(0);
      if (header == null) {
         positionCache = new FilePositionCache(0);
         lastAppended = 0;
         return;
      }
      positionCache = new FilePositionCache(header.index == 1 ? 0 : header.index);
      setFilePosition(header.index, header.position);
      lastAppended = header.index;
      long position = header.nextPosition();

      while (true) {
         header = readHeader(position);
         if (header == null) {
            return;
         }
         setFilePosition(header.index, header.position);
         position = header.nextPosition();
         lastAppended = header.index;
      }
   }

   public long getFirstAppended() {
      return positionCache.getFirstAppended();
   }

   public long getLastAppended() {
      return lastAppended;
   }

   public long getCachedFileSize() {
      return fileStorage.getCachedFileSize();
   }

   public LogEntry getLogEntry(long index) throws IOException {
      long position = positionCache.getPosition(index);
      if (position < 0) {
         return null;
      }
      Header header = readHeader(position);
      if (header == null) {
         return null;
      }
      return header.readLogEntry(this);
   }

   public int write(long startIndex, LogEntries entries) throws IOException {
      if (startIndex == 1) {
         return appendWithoutOverwriteCheck(entries, 1, 0);
      }
      // find previous entry to append
      long previousPosition = positionCache.getPosition(startIndex - 1);
      if (previousPosition < 0) {
         throw new IllegalStateException();
      }
      if (lastAppendedHeader == null || lastAppendedHeader.position != previousPosition) {
         lastAppendedHeader = readHeader(previousPosition);
         assert lastAppendedHeader == null || lastAppendedHeader.position == previousPosition;
      }
      if (lastAppendedHeader == null) {
         throw new IllegalStateException();
      }
      return appendWithoutOverwriteCheck(entries, startIndex, lastAppendedHeader.nextPosition());
   }

   private void setFilePosition(long index, long position) {
      if (!positionCache.set(index, position)) {
         log.warn("Unable to set file position for index " + index + ". LogEntry is too old");
      }
   }

   /**
    * This is not used but for testing, hence it's not using any batching optimization as
    * {@link #appendWithoutOverwriteCheck} does.
    */
   private int appendOverwriteCheck(LogEntry[] entries, long index, long position) throws IOException {
      int term = 0;
      for (LogEntry entry : entries) {
         Header header = new Header(position, index, entry);
         if (!entryExists(header)) {
            final ByteBuffer buffer = fileStorage.ioBufferWith(header.totalLength);
            header.writeTo(buffer);
            buffer.put(entry.command(), entry.offset(), entry.length());
            buffer.flip();
            fileStorage.write(header.position);
            buffer.clear();
            setFilePosition(header.index, header.position);
            term =(int)Math.max(entry.term(), term);
         }
         position = header.nextPosition();
         ++index;
      }

      lastAppended = index - 1;
      if (positionCache.invalidateFrom(index)) {
         fileStorage.truncateTo(position);
      }
      if (fsync) {
         fileStorage.flush();
      }
      return term;
   }

   private int appendWithoutOverwriteCheck(LogEntries entries, long index, long position) throws IOException {
      int term = 0;
      int batchBytes = 0;
      for (LogEntry entry : entries) {
         batchBytes += Header.getTotalLength(entry.length());
      }
      final long startPosition = position;
      final ByteBuffer batchBuffer = fileStorage.ioBufferWith(batchBytes);
      int size = entries.size(), i = 0;
      for (LogEntry entry : entries) {
         Header header = new Header(position, index, entry);
         header.writeTo(batchBuffer);
         if (entry.length() > 0) {
            batchBuffer.put(entry.command(), entry.offset(), entry.length());
         }
         setFilePosition(header.index, header.position);
         term =(int)Math.max(entry.term(), term);
         position = header.nextPosition();
         if (i == (size - 1)) {
            lastAppendedHeader = header;
         }
         ++i;
         ++index;
      }
      batchBuffer.flip();
      fileStorage.write(startPosition);
      lastAppended = index - 1;
      if (positionCache.invalidateFrom(index)) {
         fileStorage.truncateTo(position);
      }
      if (fsync) {
         fileStorage.flush();
      }
      return term;
   }

   private static Header readHeader(LogEntryStorage logStorage, long position) throws IOException {
      ByteBuffer data = logStorage.fileStorage.read(position, HEADER_SIZE);
      if (data.remaining() != HEADER_SIZE) {
         // corrupted data or non-existing data
         return null;
      }
      return new Header(position, data).consistencyCheck();
   }

   private Header readHeader(long position) throws IOException {
      return readHeader(this, position);
   }

   public void removeOld(long index) throws IOException {
      long indexToRemove = Math.min(lastAppended, index);
      long pos = positionCache.getPosition(indexToRemove);
      if (pos > 0) {
         // if pos is < 0, means the entry does not exist
         // if pos == 0, means there is nothing to truncate
         fileStorage.truncateFrom(pos);
      }
      positionCache = positionCache.createDeleteCopyFrom(index);
      if (lastAppended < index) {
         lastAppended = index;
      }
   }

   public void reinitializeTo(long index, LogEntry firstEntry) throws IOException {
      // remove the content of the file
      fileStorage.truncateTo(0);
      // first appended is set in the constructor
      positionCache = new FilePositionCache(index);

      int batchBytes = Header.getTotalLength(firstEntry.length());
      final ByteBuffer batchBuffer = fileStorage.ioBufferWith(batchBytes);
      Header header = new Header(0, index, firstEntry);
      header.writeTo(batchBuffer);

      if (firstEntry.length() > 0) {
         batchBuffer.put(firstEntry.command(), firstEntry.offset(), firstEntry.length());
      }

      fileStorage.write(0);
      setFilePosition(index, 0);
      if (fsync) {
         fileStorage.flush();
      }

      // set last appended
      lastAppended = index;
      lastAppendedHeader = header;
   }

   public long removeNew(long index) throws IOException {
      // remove all?
      if (index == 1) {
         fileStorage.truncateTo(0);
         lastAppended = 0;
         return 0;
      }
      long position = positionCache.getPosition(index - 1);
      Header previousHeader = readHeader(position);
      if (previousHeader == null) {
         throw new IllegalStateException();
      }
      fileStorage.truncateTo(previousHeader.nextPosition());
      positionCache.invalidateFrom(index);
      lastAppended = index - 1;
      return previousHeader.term;
   }

   public void forEach(ObjLongConsumer<LogEntry> consumer, long startIndex, long endIndex) throws IOException {
      startIndex = Math.max(Math.max(startIndex, getFirstAppended()), 1);
      long position = positionCache.getPosition(startIndex);
      if (position < 0) {
         return;
      }
      while (startIndex <= endIndex) {
         Header header = readHeader(position);
         if (header == null) {
            return;
         }
         consumer.accept(header.readLogEntry(this), startIndex);
         position = header.nextPosition();
         ++startIndex;
      }
   }

   private boolean entryExists(Header header) throws IOException {
      Header existing = readHeader(this, header.position);
      if (existing == null) {
         return false;
      }
      if (existing.equals(header)) { // same entry, skip overwriting
         return true;
      }
      throw new IllegalStateException();
   }

   public void useFsync(final boolean value) {
      this.fsync = value;
   }

   private static class Header {
      private static final byte SERIALIZED_TRUE = 0b1;
      private static final byte SERIALIZED_FALSE = 0b0;
      final long position;
      // magic is here in case we need to change the format!
      final byte magic;
      final int totalLength;
      final long term;
      final long index;
      final int dataLength;
      final boolean internal;

      Header(long position, long index, LogEntry entry) {
         Objects.requireNonNull(entry);
         this.position = position;
         this.magic = MAGIC_NUMBER;
         this.index = index;
         this.term = entry.term();
         this.dataLength = entry.length();
         this.internal = entry.internal();
         this.totalLength = getTotalLength(dataLength);
      }

      public static int getTotalLength(final int dataLength) {
         return HEADER_SIZE + dataLength;
      }

      Header(long position, ByteBuffer buffer) {
         this.position = position;
         this.magic = buffer.get();
         this.totalLength = buffer.getInt();
         this.term = buffer.getLong();
         this.index = buffer.getLong();
         this.internal = buffer.get() == SERIALIZED_TRUE;
         this.dataLength = buffer.getInt();
      }

      public void writeTo(ByteBuffer buffer) {
         buffer.put(magic);
         buffer.putInt(totalLength);
         buffer.putLong(term);
         buffer.putLong(index);
         buffer.put(internal ? SERIALIZED_TRUE : SERIALIZED_FALSE);
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

      LogEntry readLogEntry(LogEntryStorage storage) throws IOException {
         ByteBuffer data = storage.fileStorage.read(position + HEADER_SIZE, dataLength);
         if (data.remaining() != dataLength) {
            return null;
         }
         assert !data.hasArray();
         byte[] bytes = new byte[dataLength];
         data.get(bytes);
         LogEntry entry = new LogEntry(term, bytes);
         return entry.internal(internal);
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
