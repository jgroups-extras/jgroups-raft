package org.jgroups.perf.jmh;

import com.sun.nio.file.ExtendedOpenOption;
import org.openjdk.jmh.annotations.*;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 10)
@Measurement(iterations = 2, time = 10)
@State(Scope.Benchmark)
@Fork(2)
public class StorageAppenderBenchmark {

   private interface LogStorage extends Closeable {

      ByteBuffer acquireWriteBuffer(int logSize);

      long appendWriteBuffer() throws IOException;

      void flush() throws IOException;
   }

   @Param({"8", "128", "1024"})
   private int logEntrySize;
   @Param({"direct"})
   private String engine;
   @Param({"true"})
   private boolean flush;
   private LogStorage storage;
   @Param({"/home/forked_franz/tmp"})
   private String baseDir;
   @Param({"0", "4096"})
   private int writeAheadBytes;
   private File baseDirFile;
   private byte[] content;

   @Setup(Level.Iteration)
   public void init() throws IOException {
      this.baseDirFile = new File(baseDir);
      baseDirFile.mkdirs();
      final File logFile = new File(baseDirFile, engine);
      logFile.deleteOnExit();
      if (logFile.exists()) {
         throw new AssertionError("logFile shouldn't exists!");
      }
      switch (engine) {
         case "nio":
            storage = new NIOStorage(new File(baseDirFile, engine).toPath(), false, writeAheadBytes);
            break;
         case "direct":
            storage = new NIOStorage(new File(baseDirFile, engine).toPath(), true, writeAheadBytes);
            break;
         case "mapped":
            if (writeAheadBytes == 0) {

            }
            storage = new MappedStorage(new File(baseDirFile, engine).toPath(), writeAheadBytes);
            break;
         default:
            throw new AssertionError("unknown storage type");
      }
      content = new byte[logEntrySize];
      new SplittableRandom(1).nextBytes(content);
   }

   @TearDown(Level.Iteration)
   public void deleteFiles() throws IOException {
      delete(baseDirFile);
   }

   @Benchmark
   public long append() throws IOException {
      ByteBuffer dataBuffer = storage.acquireWriteBuffer(logEntrySize);
      dataBuffer.put(content);
      final long startLogPosition = storage.appendWriteBuffer();
      if (flush) {
         storage.flush();
      }
      return startLogPosition;
   }

   private static void delete(File base) {
      if(base == null)
         return;
      if(base.isDirectory()) {
         File[] files=base.listFiles();
         for(File f: files)
            delete(f);
      }
      base.delete(); // f is empty now
   }

   private static boolean isPowerOfTwo(final long value) {
      return Long.bitCount(value) == 1;
   }

   private static boolean isAligned(final long value, final long alignment) {
      if (!isPowerOfTwo(alignment)) {
         throw new IllegalArgumentException("alignment must be a power of 2: alignment=" + alignment);
      }
      return (value & (alignment - 1)) == 0;
   }

   private static long align(final long value, final long alignment) {
      if (!isPowerOfTwo(alignment)) {
         throw new IllegalArgumentException("alignment must be a power of 2: alignment=" + alignment);
      }
      return (value + (alignment - 1)) & ~(alignment - 1);
   }

   private static ByteBuffer allocateAlignedDirectByteBuffer(final int requiredCapacity, final int blockSize) {
      if (!isAligned(requiredCapacity, blockSize)) {
         throw new IllegalArgumentException("requiredCapacity must be aligned to " + blockSize);
      }
      return ByteBuffer.allocateDirect(requiredCapacity + blockSize - 1).alignedSlice(blockSize).limit(requiredCapacity);
   }

   private enum Flush {
      MetadataSync,
      DataSync,
      None
   }

   private static final class NIOStorage implements LogStorage {

      private final FileChannel fileChannel;
      private final RandomAccessFile raf;
      private final int writeAheadBytes;
      private long fileLength;
      private final int blockSize;
      private long appendPosition;
      private ByteBuffer logBuffer;
      private ByteBuffer dataBuffer;
      private boolean logBufferAcquired;
      private Flush flush;

      public NIOStorage(final Path singleFile, final boolean direct, final int writeAheadBytes) throws IOException {
         if (Files.exists(singleFile)) {
            throw new IllegalStateException("the file should be new!");
         }
         if (direct) {
            fileChannel = FileChannel.open(singleFile, StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE, ExtendedOpenOption.DIRECT);
            blockSize = (int) Files.getFileStore(singleFile).getBlockSize();
            if (blockSize < 0) {
               throw new IllegalStateException("blockSize must be less then " + Integer.MAX_VALUE);
            }
            raf = writeAheadBytes > 0 ? new RandomAccessFile(singleFile.toFile(), "rw") : null;
         } else {
            raf = new RandomAccessFile(singleFile.toFile(), "rw");
            fileChannel = raf.getChannel();
            blockSize = 1;
         }
         this.writeAheadBytes = writeAheadBytes;
         fileLength = fileChannel.size();
         flush = Flush.None;
         logBufferAcquired = false;
      }

      @Override
      public ByteBuffer acquireWriteBuffer(final int requiredDataCapacity) {
         if (!fileChannel.isOpen()) {
            throw new IllegalStateException("appender is closed");
         }
         if (requiredDataCapacity < 0) {
            throw new IllegalStateException("requiredDataCapacity must be > 0");
         }
         final ByteBuffer existingDataBuffer = this.dataBuffer;
         if (existingDataBuffer != null && existingDataBuffer.capacity() >= requiredDataCapacity) {
            logBufferAcquired = true;
            // clear any previously set limit/position due to appendWriteBuffer
            logBuffer.clear();
            logBuffer.putInt(0, requiredDataCapacity);
            return this.dataBuffer.clear().position(0).limit(requiredDataCapacity).order(logBuffer.order());
         }
         final int requiredLogEntryCapacity = (int) align(Integer.BYTES + requiredDataCapacity, blockSize);
         logBuffer = allocateAlignedDirectByteBuffer(requiredLogEntryCapacity, blockSize);
         logBuffer.position(Integer.BYTES);
         dataBuffer = logBuffer.slice();
         // set back position to 0
         logBuffer.position(0);
         logBuffer.putInt(0, requiredDataCapacity);
         logBufferAcquired = true;
         return this.dataBuffer;
      }

      @Override
      public long appendWriteBuffer() throws IOException {
         if (!fileChannel.isOpen()) {
            throw new IllegalStateException("channel is already closed");
         }
         if (!logBufferAcquired) {
            throw new IllegalStateException("must acquireWriteBuffer first!");
         }
         logBufferAcquired = false;
         final int dataLength = logBuffer.getInt(0);
         assert dataLength >= 0;
         final int alignedLogLength = (int) align(Integer.BYTES + dataLength, blockSize);
         logBuffer.limit(alignedLogLength);
         assert logBuffer.position() == 0;
         final long nextAppendPosition = appendPosition + alignedLogLength;
         if (nextAppendPosition > fileLength) {
            if (writeAheadBytes > 0 && dataLength < writeAheadBytes) {
               final long nextLength = nextAppendPosition + writeAheadBytes;
               raf.setLength(nextLength);
               fileLength = nextLength;
            } else {
               fileLength = nextAppendPosition;
            }
            flush = Flush.MetadataSync;
         }
         if (fileChannel.write(logBuffer, appendPosition) != alignedLogLength) {
            final IllegalStateException notAtomicWriteError = new IllegalStateException("failed to write atomically");
            try {
               close();
               throw notAtomicWriteError;
            } catch (IOException closeException) {
               notAtomicWriteError.addSuppressed(closeException);
               throw notAtomicWriteError;
            }
         }
         final long startPosition = appendPosition;
         appendPosition = startPosition + alignedLogLength;
         if (flush != Flush.MetadataSync) {
            flush = Flush.DataSync;
         }
         return startPosition;
      }

      @Override
      public void flush() throws IOException {
         if (!fileChannel.isOpen()) {
            throw new IllegalStateException("channel is already closed");
         }
         if (flush == Flush.None) {
            return;
         }
         switch (flush) {

            case MetadataSync:
               fileChannel.force(true);
               break;
            case DataSync:
               fileChannel.force(false);
               break;
         }
         flush = Flush.None;
      }

      @Override
      public void close() throws IOException {
         if (!fileChannel.isOpen()) {
            return;
         }
         flush = Flush.None;
         IOException suppressed = null;
         try {
            if (raf != null) {
               raf.close();
            }
         } catch (IOException ioException) {
            suppressed = ioException;
         } finally {
            try {
               fileChannel.close();
            } catch (IOException channelCloseEx) {
               channelCloseEx.addSuppressed(suppressed);
               throw channelCloseEx;
            }
         }
      }
   }

   private static final class MappedStorage implements LogStorage {
      private ByteBuffer logBuffer;
      private ByteBuffer dataBuffer;
      private boolean logBufferAcquired;

      private final FileChannel fileChannel;
      private final RandomAccessFile raf;

      private long appendPosition;
      private MappedByteBuffer writeRegion;
      private long regionStart;
      private long regionLimit;

      private final int mappedRegionCapacity;
      private Flush flush;

      public MappedStorage(final Path singleFile, final int mappedRegionCapacity) throws IOException {
         if (Files.exists(singleFile)) {
            throw new IllegalStateException("the file should be new!");
         }
         raf = new RandomAccessFile(singleFile.toFile(), "rw");
         fileChannel = raf.getChannel();
         regionStart = -1;
         regionLimit = -1;

         this.flush = Flush.None;

         this.mappedRegionCapacity = mappedRegionCapacity;
      }

      private MappedByteBuffer mappedWriteRegionFor(final long writePosition,
                                                    final int requiredLogEntrySize) throws IOException {
         if (writeRegion != null && writePosition >= regionStart && (writePosition + requiredLogEntrySize) < regionLimit) {
            return writeRegion;
         }
         final int mappingSize = Math.max(requiredLogEntrySize, mappedRegionCapacity);
         writeRegion = fileChannel.map(FileChannel.MapMode.READ_WRITE, writePosition, mappingSize);
         regionStart = writePosition;
         regionLimit = regionStart + mappingSize;
         raf.setLength(regionLimit);
         flush = Flush.MetadataSync;
         return writeRegion;
      }

      @Override
      public ByteBuffer acquireWriteBuffer(final int requiredDataCapacity) {
         if (!fileChannel.isOpen()) {
            throw new IllegalStateException("appender is closed");
         }
         if (requiredDataCapacity < 0) {
            throw new IllegalStateException("requiredDataCapacity must be > 0");
         }
         final ByteBuffer existingDataBuffer = this.dataBuffer;
         if (existingDataBuffer != null && existingDataBuffer.capacity() >= requiredDataCapacity) {
            logBufferAcquired = true;
            // clear any previously set limit/position due to appendWriteBuffer
            logBuffer.clear();
            logBuffer.putInt(0, requiredDataCapacity);
            return this.dataBuffer.clear().position(0).limit(requiredDataCapacity).order(logBuffer.order());
         }
         final int requiredLogEntryCapacity = Integer.BYTES + requiredDataCapacity;
         logBuffer = allocateAlignedDirectByteBuffer((int) align(requiredLogEntryCapacity, 4096), 4096);
         logBuffer.position(Integer.BYTES);
         dataBuffer = logBuffer.slice();
         // set back position to 0
         logBuffer.position(0);
         logBuffer.putInt(0, requiredDataCapacity);
         logBufferAcquired = true;
         return this.dataBuffer;
      }

      @Override
      public long appendWriteBuffer() throws IOException {
         if (!fileChannel.isOpen()) {
            throw new IllegalStateException("channel is already closed");
         }
         if (!logBufferAcquired) {
            throw new IllegalStateException("must acquireWriteBuffer first!");
         }
         logBufferAcquired = false;
         final int dataLength = logBuffer.getInt(0);
         assert dataLength >= 0;
         final int logLength = Integer.BYTES + dataLength;
         logBuffer.limit(logLength);
         assert logBuffer.position() == 0;
         final MappedByteBuffer region = mappedWriteRegionFor(appendPosition, logLength);
         final int mappedOffset = (int) (appendPosition - regionStart);
         assert mappedOffset >= 0;
         region.position(mappedOffset);
         region.put(logBuffer);
         final long startPosition = appendPosition;
         appendPosition = startPosition + logLength;
         if (flush == Flush.None) {
            flush = Flush.DataSync;
         }
         return startPosition;
      }

      @Override
      public void flush() throws IOException {
         if (!fileChannel.isOpen()) {
            throw new IllegalStateException("channel is already closed");
         }
         if (flush == Flush.None) {
            return;
         }
         switch (flush) {

            case MetadataSync:
               fileChannel.force(true);
               break;
            case DataSync:
               fileChannel.force(false);
               break;
         }
         flush = Flush.None;
      }

      @Override
      public void close() throws IOException {
         if (!fileChannel.isOpen()) {
            return;
         }
         flush = Flush.None;
         fileChannel.close();
      }
   }

}
