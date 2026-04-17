package org.jgroups.raft.filelog;

import org.jgroups.raft.util.pmem.FileProvider;
import org.jgroups.util.Util;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

/**
 * Base class to store data in a file.
 *
 * <p>
 * Durable storage for a single file with buffered positional I/O. Callers prepare data via {@link #ioBufferWith(int)},
 * then issue a positional {@link #write(long)}. Writes are guaranteed to be complete; partial writes from the underlying
 * channel are retried automatically, and zero-progress writes result in an {@link IOException}. Data becomes durable only
 * after an explicit {@link #flush()}.
 * </p>
 *
 * <p>
 * The file may optionally pre-allocate space beyond the written data to reduce metadata updates on subsequent writes
 * (write-ahead allocation).
 * </p>
 *
 * @author Pedro Ruivo
 * @since 0.5.4
 */
final class FileStorage implements Closeable {

   private enum Flush {
      Metadata, Data, None
   }

   private final FileHandleProvider fileHandleProvider;
   private final boolean isWindowsOS;
   private final File storageFile;
   private FileChannel channel;
   private RandomAccessFile raf;
   private long fileSize;
   private final int writeAheadBytes;
   private Flush requiredFlush;
   private ByteBuffer ioBuffer;
   private boolean ioBufferReady;

   public FileStorage(final File storageFile) {
      this(storageFile, 0);
      ioBufferReady = false;
   }

   public FileStorage(final File storageFile, final int writeAheadBytes) {
      this(storageFile, writeAheadBytes, FileHandleProvider.defaultProvider());
   }

   // Package-private for testing purposes.
   FileStorage(final File storageFile, final int writeAheadBytes, FileHandleProvider fileHandleProvider) {
      if (writeAheadBytes < 0) {
         throw new IllegalArgumentException("writeAheadBytes must be greater than or equals to 0");
      }
      this.storageFile = Objects.requireNonNull(storageFile);
      this.writeAheadBytes = writeAheadBytes;
      this.fileSize = -1;
      this.requiredFlush = Flush.None;
      this.isWindowsOS = Util.checkForWindows();
      this.fileHandleProvider = fileHandleProvider;
   }

   /**
    * Returns a direct {@link ByteBuffer} with at least the requested capacity, ready for writing.
    *
    * <p>
    * The returned buffer is reused across calls when its capacity is sufficient, avoiding repeated allocation. The caller
    * fills the buffer and then calls {@link #write(long)}.
    * </p>
    *
    * @param requiredCapacity minimum number of bytes the buffer must hold
    * @return a direct byte buffer positioned at 0 with the given limit
    */
   public ByteBuffer ioBufferWith(final int requiredCapacity) {
      final ByteBuffer availableWriteBuffer = this.ioBuffer;
      if (availableWriteBuffer != null && availableWriteBuffer.capacity() >= requiredCapacity) {
         availableWriteBuffer.position(0).limit(requiredCapacity);
         ioBufferReady = true;
         return availableWriteBuffer;
      }
      final ByteBuffer newBuffer = ByteBuffer.allocateDirect(requiredCapacity);
      this.ioBuffer = newBuffer;
      ioBufferReady = true;
      return newBuffer;
   }

   public long getCachedFileSize() {
      if (channel == null) {
         throw new IllegalStateException("fileSize cannot be retrieved if closed");
      }
      return fileSize;
   }

   public File getStorageFile() {
      return storageFile;
   }

   public void open() throws IOException {
      if (channel == null) {
         FileHandleProvider.Handle handle = fileHandleProvider.open(storageFile);
         this.channel = handle.channel();
         this.raf = handle.raf();
         fileSize = channel.size();
      }
   }

   /**
    * Writes the contents of the I/O buffer to the file at the given position.
    *
    * <p>
    * The buffer must be prepared beforehand via {@link #ioBufferWith(int)}. All remaining bytes in the buffer are guaranteed
    * to be written; partial writes are retried until complete. If the channel makes no progress (zero bytes written), an
    * {@link IOException} is thrown.
    * </p>
    *
    * <p>
    * When the write extends beyond the current file size, the file is grown automatically. If write-ahead allocation is
    * configured, additional space is pre-allocated to reduce future metadata updates.
    * </p>
    *
    * @param position the file offset at which to begin writing
    * @return the total number of bytes written
    * @throws IOException if the write fails or makes no progress
    * @throws IllegalStateException if the I/O buffer was not prepared or the file is not open
    */
   public int write(final long position) throws IOException {
      if (!ioBufferReady) {
         throw new IllegalStateException("must prepare the IO buffer first!");
      }
      final FileChannel channel = checkOpen();
      final int dataLength = ioBuffer.remaining();
      final long nextPosition = position + dataLength;
      final boolean growFile = nextPosition > fileSize;
      if (growFile) {
         if (writeAheadBytes > 0 && dataLength < writeAheadBytes) {
            final long writeAheadSize = nextPosition + writeAheadBytes;
            raf.setLength(writeAheadSize);
            fileSize = writeAheadSize;
         } else {
            fileSize = nextPosition;
         }
         requiredFlush = Flush.Metadata;
      }
      try {
         int totalWritten = 0;
         // Keep writing the full buffer and track for short writes.
         while (ioBuffer.hasRemaining()) {
            final int written = channel.write(ioBuffer, position + totalWritten);
            if (written == 0) {
               String message = String.format("No progress writing to %s at position %d: 0 bytes written, %d remaining",
                       storageFile.getAbsolutePath(), position + totalWritten, ioBuffer.remaining());
               throw new IOException(message);
            }
            totalWritten += written;
         }
         if (totalWritten > 0 && requiredFlush == Flush.None) {
            requiredFlush = Flush.Data;
         }
         return totalWritten;
      } finally {
         ioBufferReady = false;
      }
   }

   public ByteBuffer read(final long position, final int expectedLength) throws IOException {
      final FileChannel channel = checkOpen();
      final ByteBuffer readBuffer = ioBufferWith(expectedLength);
      assert ioBufferReady;
      try {
         channel.read(readBuffer, position);
         return readBuffer.flip();
      } finally {
         ioBufferReady = false;
      }
   }

   public boolean isOpened() {
      return channel != null && channel.isOpen();
   }

   private FileChannel checkOpen() {
      final FileChannel channel = this.channel;
      if (channel == null || !channel.isOpen()) {
         throw new IllegalStateException("File " + storageFile.getAbsolutePath() + " not open!");
      }
      return channel;
   }

   /**
    * Flushes any pending writes to durable storage.
    *
    * <p>
    * Uses {@code fdatasync} semantics ({@code force(false)}) for data-only changes, and {@code fsync} semantics
    * ({@code force(true)}) when file metadata (size) also changed. No-op if no writes have occurred since the last flush.
    * </p>
    *
    * @throws IOException if the flush fails
    */
   public void flush() throws IOException {
      checkOpen();
      if (requiredFlush == Flush.None) {
         return;
      }
      try {
         switch (requiredFlush) {
            case Metadata:
               channel.force(true);
               break;
            case Data:
               channel.force(false);
               break;
            default:
               throw new IllegalStateException("Unknown flush type " + requiredFlush);
         }
      } finally {
         requiredFlush = Flush.None;
      }
   }

   /**
    * Truncates the file to the given size, discarding any data beyond it.
    *
    * @param size the new file size in bytes
    * @throws IOException if the truncation fails
    */
   public void truncateTo(final long size) throws IOException {
      final FileChannel existing = checkOpen();
      existing.truncate(size);
      fileSize = size;
      requiredFlush = Flush.Metadata;
   }

   /**
    * Discards the beginning of the file, keeping only data from the given position onward.
    *
    * <p>
    * The retained data is copied to a temporary file which then replaces the original. The file is reopened after the
    * operation completes.
    * </p>
    *
    * @param position the first byte position to keep
    * @throws IOException if the operation fails
    * @throws IllegalArgumentException if position exceeds the file size
    */
   public void truncateFrom(final long position) throws IOException {
      final FileChannel existing = checkOpen();
      if (position > fileSize) {
         throw new IllegalArgumentException("position must be greater then fileSize");
      }
      final File tmpFile = new File(storageFile.getParentFile(), storageFile.getName() + ".tmp");
      try (FileChannel newChannel = tryCreatePMEMFileChannel(tmpFile, fileSize)) {
         existing.transferTo(position, fileSize, newChannel);
      }
      // TODO: it requires fsync on the parent folder and on the destination file?
      try {
         // If running in Windows, first release other file holder before overwriting.
         if (isWindowsOS)
            close();
         Files.move(tmpFile.toPath(), storageFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
      } catch (IOException failedMove) {
         try {
            Files.deleteIfExists(tmpFile.toPath());
         } catch (IOException deleteTmpEx) {
            failedMove.addSuppressed(deleteTmpEx);
            throw failedMove;
         }
         throw failedMove;
      }
      channel = null;
      raf = null;
      fileSize = -1;
      existing.close();
      open();
      requiredFlush = Flush.Metadata;
   }

   @Override
   public void close() throws IOException {
      if (channel == null) {
         return;
      }
      IOException suppressed = null;
      try {
         if (raf != null) {
            raf.close();
         }
      } catch (IOException rafEx) {
         suppressed = rafEx;
      } finally {
         try {
            channel.close();
         } catch (IOException chEx) {
            if (suppressed != null) {
               chEx.addSuppressed(suppressed);
               throw chEx;
            }
         } finally {
            requiredFlush = Flush.None;
            raf = null;
            channel = null;
            fileSize = -1;
         }
      }
   }

   public boolean hasPendingFlush() {
      return requiredFlush != Flush.None;
   }

   private static FileChannel tryCreatePMEMFileChannel(File tmpFile, long fileSize) throws IOException {
      if (FileProvider.isPMEMAvailable()) {
         final FileChannel pmemChannel = FileProvider.openPMEMChannel(tmpFile, (int) fileSize, true, true);
         if (pmemChannel != null) {
            return pmemChannel;
         }
      }
      return FileChannel.open(tmpFile.toPath(), StandardOpenOption.WRITE, StandardOpenOption.CREATE);
   }

}
