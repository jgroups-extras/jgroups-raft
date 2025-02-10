package org.jgroups.raft.filelog;

import net.jcip.annotations.NotThreadSafe;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.util.ByteBufferInputStream;
import org.jgroups.util.Util;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

/**
 * Stores the RAFT log metadata in a file.
 * <p>
 * The metadata includes the commit index, the current term and the last vote. The storage format keeps the fixed-size
 * elements first and the vote last. The format follows:
 * </p>
 *
 * <pre>{@code
 * +----------------+--------------+------------------+
 * |                |              |                  |
 * | COMMIT 8 Bytes | TERM 8 Bytes | VOTE <Var> Bytes |
 * |                |              |                  |
 * +----------------+--------------+------------------+
 * }</pre>
 *
 * <p>
 * The storage mmap the file to avoid syscalls. Since these variables have frequent access, the implementation accesses
 * the direct positions in mmap.
 * </p>
 *
 * <p>
 * <b>Platform Dependent:</b> The implementation does not utilize mmap in Windows. Due to restrictions on managing files
 * in Windows, the data is read and written directly from the file.
 * </p>
 *
 * @author Pedro Ruivo
 * @since 0.5.4
 */
public class MetadataStorage {

   private static final String FILE_NAME = "metadata.raft";

   // page 4 RAft original paper: COMMIT INDEX DOESN'T REQUIRE FDATASYNC
   private static final int COMMIT_INDEX_POS = 0;
   // page 4 RAft original paper: RARE BUT REQUIRES FDATASYNC
   private static final int CURRENT_TERM_POS = COMMIT_INDEX_POS + Global.LONG_SIZE;
   // Check if file length is != from the last FSYNC: this is variable-sized!
   private static final int VOTED_FOR_POS = CURRENT_TERM_POS + Global.LONG_SIZE;
   private final FileStorage fileStorage;
   private ReadWriteWrapper wrapper;
   private boolean fsync;

   public MetadataStorage(File parentDir, boolean fsync) {
      fileStorage = new FileStorage(new File(parentDir, FILE_NAME));
      this.fsync = fsync;
   }

   public void useFsync(boolean value) {
      fsync = value;
   }

   public boolean useFsync() {
      return fsync;
   }

   public void open() throws IOException {
       fileStorage.open();
       // Platform dependant.
       // Windows has more checks for file IO. For example, we can only delete a mmap'ed file after the buffer is collected by GC.
       // Since we don't have a way to control GC and when the contents are released, we avoid mmap on Windows altogether.
       if (Util.checkForWindows()) {
           FileChannelDelegate rafw = new FileChannelDelegate(new RandomAccessFile(fileStorage.getStorageFile(), "rw"));
           wrapper = new ReadWriteWrapper(rafw, rafw, rafw, rafw);
       } else {
           // This won't need a sys-call for frequently accessed data
           MappedByteBuffer mmap = FileChannel.open(fileStorage.getStorageFile().toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE)
                   .map(FileChannel.MapMode.READ_WRITE, 0, VOTED_FOR_POS);
           wrapper = new ReadWriteWrapper(
                   mmap::putLong,
                   mmap::getLong,
                   mmap::force,
                   () -> {}
           ) ;
       }
   }

   public void close() throws IOException {
      fileStorage.close();
      wrapper.close.close();
   }

   public void delete() throws IOException {
      fileStorage.delete();
      wrapper = null;
   }

   public long getCommitIndex() {
      return wrapper.reader.read(COMMIT_INDEX_POS);
   }

   public void setCommitIndex(long commitIndex) throws IOException {
       wrapper.writer.write(COMMIT_INDEX_POS, commitIndex);
   }

   public long getCurrentTerm() {
      return wrapper.reader.read(CURRENT_TERM_POS);
   }

   public void setCurrentTerm(long term) throws IOException {
       wrapper.writer.write(CURRENT_TERM_POS, term);
       if (fsync) {
           wrapper.flush.fsync();
       }
   }

   public Address getVotedFor() throws IOException, ClassNotFoundException {
      // most addresses are quite small
      final ByteBuffer dataLengthBuffer = fileStorage.read(VOTED_FOR_POS, Global.INT_SIZE);
      if (dataLengthBuffer.remaining() != Global.INT_SIZE) {
         // corrupted?
         return null;
      }
      final int addressLength = dataLengthBuffer.getInt(0);
      final ByteBuffer addressLengthBuffer = fileStorage.read(VOTED_FOR_POS + Global.INT_SIZE, addressLength);
      if (addressLengthBuffer.remaining() != addressLength) {
         //uname to read "addressLength" bytes
         return null;
      }
      return readAddress(addressLengthBuffer);
   }

   public void setVotedFor(Address address) throws IOException {
      if (address == null) {
         fileStorage.truncateTo(VOTED_FOR_POS);
         return;
      }
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      Util.writeAddress(address, new DataOutputStream(baos));

      byte[] data = baos.toByteArray();
      ByteBuffer buffer = fileStorage.ioBufferWith(Global.INT_SIZE + data.length);
      buffer.putInt(data.length);
      buffer.put(data);
      buffer.flip();
      fileStorage.write(VOTED_FOR_POS);
   }

   private static Address readAddress(ByteBuffer buffer) throws IOException, ClassNotFoundException {
      return Util.readAddress(new ByteBufferInputStream(buffer));
   }

    /**
     * Wrapper to hide implementation specific details of writing/reading from files.
     *
     * <p>
     * This class delegates each operation to the provided classes.
     * </p>
     */
    private static final class ReadWriteWrapper {
        private final Writer writer;
        private final Reader reader;
        private final Flush flush;
        private final Closeable close;

        private ReadWriteWrapper(Writer writer, Reader reader, Flush flush, Closeable close) {
            this.writer = writer;
            this.reader = reader;
            this.flush = flush;
            this.close = close;
        }
    }

    /**
     * Implement all I/O operations delegating to a {@link FileChannel}.
     *
     * <p>
     * Implements all interfaces and delegates to a {@link FileChannel} instance. All operations utilize the same
     * {@link ByteBuffer} instance for reading and writing to the underlying file. No additional bytes are allocated
     * after initialization.
     * </p>
     *
     * <b>Thread-safety:</b> This class is <b>not</b> thread safe. It utilizes a single {@link ByteBuffer} instance in
     * all operations.
     */
    @NotThreadSafe
    private static final class FileChannelDelegate implements Writer, Reader, Flush, Closeable {
        private final ByteBuffer buffer;
        private final RandomAccessFile raf;
        private final FileChannel channel;

        private FileChannelDelegate(RandomAccessFile raf) {
            this.raf = raf;
            this.channel = raf.getChannel();
            this.buffer = ByteBuffer.allocateDirect(Long.BYTES);
        }

        @Override
        public long read(int position) {
            try {
                buffer.rewind();
                channel.read(buffer, position);
                buffer.rewind();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return buffer.getLong();
        }

        @Override
        public void write(int position, long value) {
            buffer.rewind();
            buffer.putLong(value);
            try {
                buffer.flip();
                int w = channel.write(buffer, position);
                if (w != Long.BYTES)
                    throw new IllegalArgumentException("Unable to write value");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() throws IOException {
            channel.close();
            raf.close();
        }

        @Override
        public void fsync() throws IOException {
            channel.force(true);
        }
    }

    /**
     * Writer interface to specific file position.
     */
    private interface Writer {

        /**
         * Writes the given long value to the specific file position.
         *
         * @param position: File position to write to.
         * @param value: Value to write.
         */
        void write(int position, long value);
    }

    /**
     * Reader interface for specific file position.
     */
    private interface Reader {

        /**
         * Reads a long value starting at the given position. Reads the next {@link Long#BYTES} bytes.
         *
         * @param position:
         * @return a long value stored in the file.
         */
        long read(int position);
    }

    /**
     * Interface to flush contents to disk.
     */
    private interface Flush {

        /**
         * Fsync data to disk.
         *
         * @throws IOException: If a failure happens during the operation.
         */
        void fsync() throws IOException;
    }
}
