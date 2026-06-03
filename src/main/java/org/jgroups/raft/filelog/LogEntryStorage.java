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
import java.util.zip.CRC32C;

/**
 * Stores the {@link LogEntry} into a file.
 *
 * @author Pedro Ruivo
 * @since 0.5.4
 */
public final class LogEntryStorage {

   public static final byte[] FILE_HEADER_MAGIC = {'R', 'A', 'F', 'T'};
   public static final byte FILE_HEADER_VERSION = 2;
   public static final int FILE_HEADER_SIZE = 8;
   public static final int CRC_SIZE = 4;

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   public static final byte MAGIC_NUMBER = 0x01;
   public static final byte MAGIC_NUMBER_CRC = 0x02;
   public static final String FILE_NAME = "entries.raft";
   public static final int HEADER_SIZE = Global.INT_SIZE * 2 + Global.LONG_SIZE * 2 + 1 + Global.BYTE_SIZE;
   // this is the typical OS page size and SSD blck_size
   private static final int DEFAULT_WRITE_AHEAD_BYTES = 4096;

   private final FileStorage fileStorage;
   private FilePositionCache positionCache;
   private Header lastAppendedHeader;
   private long lastAppended;
   private long entryStartOffset;
   private boolean fsync;

   public LogEntryStorage(File parentDir, boolean fsync) {
      super();
      this.fsync = fsync;
      positionCache = new FilePositionCache(0);
      fileStorage = new FileStorage(new File(parentDir, FILE_NAME), DEFAULT_WRITE_AHEAD_BYTES);
   }

   public static void writeFileHeaderTo(ByteBuffer buffer) {
      buffer.put(FILE_HEADER_MAGIC);
      buffer.put(FILE_HEADER_VERSION);

      // We include a few padding bytes for alignment in 8 bytes and if we ever need extra flags in the future.
      while (buffer.position() < FILE_HEADER_SIZE)
         buffer.put((byte) 0);
   }

   public void open() throws IOException {
      fileStorage.open();
      if (fileStorage.getCachedFileSize() == 0) {
         writeFileHeader();
         entryStartOffset = FILE_HEADER_SIZE;
      }
   }

   public void close() throws IOException {
      fileStorage.close();
   }

   public void reload() throws IOException {
      long fileSize = fileStorage.getCachedFileSize();
      if (fileSize == 0) {
         positionCache = new FilePositionCache(0);
         lastAppended = 0;
         return;
      }

      ByteBuffer peek = fileStorage.read(0, 4);
      if (isRaftFile(peek)) {
         ByteBuffer header = fileStorage.read(0, FILE_HEADER_SIZE);
         // The version is after the magic bytes.
         byte version = header.get(FILE_HEADER_MAGIC.length);
         if (version < 1 || version > FILE_HEADER_VERSION) {
            String message = String.format("entries.raft has version %d, but this release only supports up to version %d. " +
                            "Upgrade to a compatible release, or run 'raft log downgrade' to convert the log file.",
                    version, FILE_HEADER_VERSION);
            throw new IOException(message);
         }
         entryStartOffset = FILE_HEADER_SIZE;
      } else if (peek.get(0) == MAGIC_NUMBER || peek.get(0) == MAGIC_NUMBER_CRC) {
         entryStartOffset = 0;
      } else {
         String message = String.format("Unrecognized format in entries.raft: first byte is 0x%02X. " +
                 "Expected file header 'RAFT' or legacy entry magic 0x%02X. The file may be corrupted or is not a Raft log.",
                 peek.get(0), MAGIC_NUMBER);
         throw new IOException(message);
      }

      // Resume scanning the entries at the correct offset.
      Header header = readHeader(entryStartOffset);
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
      if (lastAppended == 0)
         return 0;

      return fileStorage.getCachedFileSize();
   }

   public LogEntry getLogEntry(long index) throws IOException {
      long position = positionCache.getPosition(index);
      if (position < 0) {
         return null;
      }
      Header header = readHeader(position);
      if (header == null) {
         String message = String.format(
                 "Corrupted entry header at index %d (file position %d). " +
                         "The log file may be corrupted. " +
                         "Run 'raft log verify' for diagnostics before taking corrective action.",
                 index, position);
         throw new IOException(message);
      }
      return header.readLogEntry(this);
   }

   public int write(long startIndex, LogEntries entries) throws IOException {
      if (startIndex == 1) {
         return appendWithoutOverwriteCheck(entries, 1, entryStartOffset);
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

   private int appendWithoutOverwriteCheck(LogEntries entries, long index, long position) throws IOException {
      int term = 0;
      int batchBytes = 0;
      for (LogEntry entry : entries) {
         batchBytes += Header.getTotalLength(entry.length());
      }
      final long startPosition = position;
      final ByteBuffer batchBuffer = fileStorage.ioBufferWith(batchBytes);
      int offset = 0;
      int size = entries.size(), i = 0;
      final CRC32C crc = new CRC32C();
      for (LogEntry entry : entries) {
         Header header = new Header(position, index, entry);
         header.writeTo(batchBuffer);
         if (entry.length() > 0) {
            batchBuffer.put(entry.command(), entry.offset(), entry.length());
         }

         writeTrailingCRC(crc, batchBuffer, offset);
         setFilePosition(header.index, header.position);
         term =(int)Math.max(entry.term(), term);
         position = header.nextPosition();
         if (i == (size - 1)) {
            lastAppendedHeader = header;
         }
         ++i;
         ++index;
         offset = batchBuffer.position();
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

   private void writeTrailingCRC(CRC32C crc, ByteBuffer buffer, int offset) {
      int before = buffer.position();
      buffer.position(offset);
      buffer.limit(before);
      crc.update(buffer);
      buffer.limit(buffer.capacity());
      // CRC32, as the name says, it has only 32 bits.
      // Since it returns a long, we get only the first 32 bits and cast to int.
      int checksum = (int) (crc.getValue() & 0xFFFFFFFFL);
      buffer.putInt(checksum);
      crc.reset();
   }

   private static Header readHeader(LogEntryStorage logStorage, long position) throws IOException {
      ByteBuffer data = logStorage.fileStorage.read(position, HEADER_SIZE);
      if (data.remaining() < HEADER_SIZE) {
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
         // We pass the entry offset so we can keep the file header and truncate the content from index.
         fileStorage.truncateFrom(pos, entryStartOffset);
      }
      reload();
      positionCache = positionCache.createDeleteCopyFrom(index, entryStartOffset);
      if (lastAppended < index) {
         lastAppended = index;
      }
   }

   public void reinitializeTo(long index, LogEntry firstEntry) throws IOException {
      // remove the content of the file
      fileStorage.truncateTo(0);
      writeFileHeader();
      entryStartOffset = FILE_HEADER_SIZE;
      // first appended is set in the constructor
      positionCache = new FilePositionCache(index);

      int batchBytes = Header.getTotalLength(firstEntry.length());
      final ByteBuffer batchBuffer = fileStorage.ioBufferWith(batchBytes);
      Header header = new Header(entryStartOffset, index, firstEntry);
      header.writeTo(batchBuffer);

      if (firstEntry.length() > 0) {
         batchBuffer.put(firstEntry.command(), firstEntry.offset(), firstEntry.length());
         writeTrailingCRC(new CRC32C(), batchBuffer, 0);
      }

      batchBuffer.flip();
      fileStorage.write(entryStartOffset);
      setFilePosition(index, entryStartOffset);
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
         writeFileHeader();
         positionCache.invalidateFrom(1);
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

   public void useFsync(final boolean value) {
      this.fsync = value;
   }

   private void writeFileHeader() throws IOException {
      ByteBuffer header = fileStorage.ioBufferWith(FILE_HEADER_SIZE);
      writeFileHeaderTo(header);
      header.flip();
      fileStorage.write(0);
      fileStorage.flush();
   }

   private static boolean isRaftFile(ByteBuffer bb) {
      return bb.remaining() >= 4
              && bb.get(0) == FILE_HEADER_MAGIC[0]
              && bb.get(1) == FILE_HEADER_MAGIC[1]
              && bb.get(2) == FILE_HEADER_MAGIC[2]
              && bb.get(3) == FILE_HEADER_MAGIC[3];
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
         this.magic = MAGIC_NUMBER_CRC;
         this.index = index;
         this.term = entry.term();
         this.dataLength = entry.length();
         this.internal = entry.internal();
         this.totalLength = getTotalLength(dataLength);
      }

      public static int getTotalLength(final int dataLength) {
         return HEADER_SIZE + dataLength + CRC_SIZE;
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
         long curr = HEADER_SIZE + position + dataLength;
         // In version 2 we included CRC in the data trailer.
         if (magic == MAGIC_NUMBER_CRC)
            return CRC_SIZE + curr;
         return curr;
      }

      Header consistencyCheck() throws IOException {
         int expectedLength = 0;
         if (magic != MAGIC_NUMBER && magic != MAGIC_NUMBER_CRC)
            return null;

         if (magic == MAGIC_NUMBER)
            expectedLength = dataLength + HEADER_SIZE;

         if (magic == MAGIC_NUMBER_CRC)
            expectedLength = dataLength + HEADER_SIZE + CRC_SIZE;

         if (term <= 0 || index <= 0 || dataLength < 0 || totalLength < HEADER_SIZE || totalLength != expectedLength) {
            String message = String.format(
                    "Corrupted entry header at file position %d. "
                            + "The log file might be corrupted. Run 'raft log verify' for diagnostics before corrective action.", position);
            throw new IOException(message);
         }

         return this;
      }

      LogEntry readLogEntry(LogEntryStorage storage) throws IOException {
         int length = dataLength;
         ByteBuffer data;
         byte[] header = null;
         if (magic == MAGIC_NUMBER_CRC) {
            length += CRC_SIZE;
            data = storage.fileStorage.read(position, HEADER_SIZE + length);
            header = new byte[HEADER_SIZE];
            data.get(header);
         } else {
            data = storage.fileStorage.read(position + HEADER_SIZE, length);
         }

         if (data.remaining() != length) {
            String message = String.format(
                    "Truncated entry at index %d (file position %d): expected %d bytes, but found %d. " +
                            "The log file may be corrupted. " +
                            "Run 'raft log verify' for diagnostics before taking corrective action.",
                    index, position, length, data.remaining());
            throw new IOException(message);
         }

         assert !data.hasArray();
         byte[] bytes = new byte[dataLength];
         data.get(bytes);
         if (magic == MAGIC_NUMBER_CRC) {
            int expected = data.getInt();
            CRC32C crc = new CRC32C();
            crc.update(header);
            crc.update(bytes);
            int actual = (int) (crc.getValue() & 0xFFFFFFFFL);
            if (expected != actual) {
               String message = String.format(
                       "CRC mismatch for entry at index %d (file position %d): expected CRC 0x%08x but found 0x%08X. "
                               + "The log file might be corrupted. Run 'raft log verify' for diagnostics before taking corrective action.",
                       index, position, expected, actual);
               throw new IOException(message);
            }
         }
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
