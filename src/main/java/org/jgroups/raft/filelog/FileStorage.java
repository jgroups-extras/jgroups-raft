package org.jgroups.raft.filelog;

import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
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
 * @author Pedro Ruivo
 * @since 0.5.4
 */
public final class FileStorage implements Closeable {

   private enum Flush {
      Metadata, Data, None
   }

   private static final Log LOG = LogFactory.getLog(FileStorage.class);

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
      if (writeAheadBytes < 0) {
         throw new IllegalArgumentException("writeAheadBytes must be greater than or equals to 0");
      }
      this.storageFile = Objects.requireNonNull(storageFile);
      this.writeAheadBytes = writeAheadBytes;
      this.fileSize = -1;
      this.requiredFlush = Flush.None;
      this.isWindowsOS = Util.checkForWindows();
   }

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
         final FileChannel pmemChannel = FileProvider.openPMEMChannel(storageFile, 1024, true, true);
         if (channel == null) {
            final RandomAccessFile raf = new RandomAccessFile(storageFile, "rw");
            this.channel = raf.getChannel();
            this.raf = raf;
         } else {
            this.channel = pmemChannel;
            this.raf = null;
         }
         fileSize = channel.size();
      }
   }

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
         final int written = channel.write(ioBuffer, position);
         if (written < dataLength) {
            if (!growFile) {
               fileSize = position + written;
            }
         }
         if (written > 0 && requiredFlush == Flush.None) {
            requiredFlush = Flush.Data;
         }
         return written;
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
         }
      } finally {
         requiredFlush = Flush.None;
      }
   }

   public void truncateTo(final long size) throws IOException {
      final FileChannel existing = checkOpen();
      existing.truncate(size);
      fileSize = size;
      requiredFlush = Flush.Metadata;
   }

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

   public void delete() throws IOException {
      if (storageFile.exists()) {
         if (!storageFile.delete()) {
            LOG.warn("Failed to delete file " + storageFile.getAbsolutePath());
         }
         close();
      }
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
