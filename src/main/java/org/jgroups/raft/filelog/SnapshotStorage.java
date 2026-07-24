package org.jgroups.raft.filelog;

import org.jgroups.protocols.raft.StagedSnapshotCapability;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32C;
import java.util.zip.CheckedOutputStream;

import net.jcip.annotations.NotThreadSafe;

/**
 * Stores and retrieves state machine snapshots in a versioned file format with CRC-32C integrity checking.
 *
 * <p>
 * New snapshots are written with an 8-byte header ({@code "SNAP"} magic, version, reserved bytes) followed by the snapshot
 * data and a trailing 4-byte CRC-32C checksum. Legacy snapshots (without a header) are read transparently in compatibility
 * mode without CRC validation.
 * </p>
 *
 * <p>
 * <b>Thread-safety:</b> This implementation is <b>NOT</b> thread-safe. Invoking write and read concurrently will lead to
 * undefined behavior.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 */
@NotThreadSafe
public final class SnapshotStorage implements StagedSnapshotCapability {

    public static final byte[] SNAPSHOT_HEADER_MAGIC = {'S', 'N', 'A', 'P'};
    public static final byte SNAPSHOT_HEADER_VERSION = 2;
    public static final int SNAPSHOT_HEADER_SIZE = 8;
    public static final int CRC_SIZE = 4;
    private static final ByteBuffer HEADER_BUFFER = ByteBuffer.allocate(SNAPSHOT_HEADER_SIZE);

    static {
        // First 4 bytes are the magic SNAP.
        // Followed by a single byte for version.
        // For 8 bytes alignment, and future proofing flags, we add 3 bytes for padding.
        HEADER_BUFFER.put(SNAPSHOT_HEADER_MAGIC);
        HEADER_BUFFER.put(SNAPSHOT_HEADER_VERSION);
        HEADER_BUFFER.put((byte) 0);
        HEADER_BUFFER.put((byte) 0);
        HEADER_BUFFER.put((byte) 0);
    }

    public static final String SNAPSHOT_FILE_NAME = "state_snapshot.raft";

    private final File logDir;
    private final CRC32C crc;

    // Initialized lazily on first access for reads.
    private FileChannel readChannel;
    private long dataStart;
    private long dataSize;

    public SnapshotStorage(File logDir) {
        this.logDir = logDir;
        this.crc = new CRC32C();
    }

    /**
     * Stores a snapshot, replacing any previously stored snapshot.
     *
     * <p>
     * The snapshot is written with a file header and a trailing CRC-32C checksum computed over the snapshot data. If a
     * snapshot file already exists, the write goes to a temporary file first and is atomically moved into place.
     * </p>
     *
     * @param snapshot the snapshot data to store
     * @throws IOException if the snapshot cannot be written
     */
    public void writeSnapshot(InputStream snapshot) throws IOException {
        closeReadChannel();
        write(snapshot);
    }

    /**
     * Removes the stored snapshot, if any.
     *
     * @throws IOException if the file cannot be deleted
     */
    public void deleteSnapshot() throws IOException {
        closeReadChannel();
        Files.deleteIfExists(snapshotPath());
    }

    public long snapshotSize() throws IOException {
        openReadChannel();
        return dataSize;
    }

    public int region(long offset, byte[] dst, int dstOffset, int length) throws IOException {
        openReadChannel();
        if (readChannel == null)
            return 0;

        ByteBuffer bb = ByteBuffer.wrap(dst, dstOffset, length);
        return readChannel.read(bb, dataStart + offset);
    }

    private boolean isSnapshotFile(FileChannel channel) throws IOException {
        if (channel.size() < SNAPSHOT_HEADER_SIZE)
            return false;

        ByteBuffer buf = ByteBuffer.allocate(SNAPSHOT_HEADER_SIZE);
        channel.read(buf, 0);
        buf.flip();
        return isSnapshotFile(buf);
    }

    private void openReadChannel() throws IOException {
        if (readChannel != null)
            return;

        Path snapshotPath = snapshotPath();

        // If the snapshot file doesn't exist yet, nothing to read or initialize.
        if (!Files.exists(snapshotPath))
            return;

        // Otherwise, create the channel and identify whether the snapshot file is following the newest version.
        // The new version has a header and a trailing CRC.
        readChannel = FileChannel.open(snapshotPath, StandardOpenOption.READ);
        if (isSnapshotFile(readChannel)) {
            dataStart = SNAPSHOT_HEADER_SIZE;
            dataSize = readChannel.size() - SNAPSHOT_HEADER_SIZE - CRC_SIZE;
        } else {
            dataStart = 0;
            dataSize = readChannel.size();
        }
    }

    private void closeReadChannel() {
        if (readChannel == null)
            return;

        try {
            readChannel.close();
        } catch (IOException ignored) { }

        readChannel = null;
        dataStart = 0;
        dataSize = 0;
    }

    /**
     * Returns the most recently stored snapshot.
     *
     * <p>
     * If the file starts with the {@code "SNAP"} magic, the header version is validated and the trailing CRC-32C checksum
     * is verified against the snapshot data. Legacy snapshots (no header) are returned as-is without CRC validation.
     * </p>
     *
     * <p>
     * The returned stream is backed by an independent {@link FileChannel}, it does not interfere with the shared
     * channel used by {@link #region} and {@link #snapshotSize}. Callers own the stream and must close it
     * (try-with-resources). For versioned snapshots, the stream validates the trailing CRC-32C checksum lazily as data
     * is consumed: a mismatch throws {@link IOException} from {@code read()}, not from {@code close()}.
     * </p>
     *
     * @return an input stream over the snapshot data, or {@code null} if no snapshot file exists
     * @throws IOException if the header version is unsupported or the file is truncated
     */
    public InputStream readSnapshotStream() throws IOException {
        Path path = snapshotPath();
        if (!Files.exists(path))
            return null;

        FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
        try {
            long size = channel.size();

            // Current content utilizes an older version without headers and trailing CRC.
            // We can just return it as is.
            if (size < SNAPSHOT_HEADER_SIZE)
                return Channels.newInputStream(channel);

            ByteBuffer header = ByteBuffer.allocate(SNAPSHOT_HEADER_SIZE);
            channel.read(header, 0);
            header.flip();

            // The current snapshot has enough data to have a header.
            // We check now if the initial bytes identify the file in the new format.
            // If not a file in the new format, we just reset the channel position and return the stream.
            if (!isSnapshotFile(header)) {
                channel.position(0);
                return Channels.newInputStream(channel);
            }

            byte version = header.get(SNAPSHOT_HEADER_MAGIC.length);
            if (version < 1 || version > SNAPSHOT_HEADER_VERSION) {
                String message = String.format("Snapshot has version %d, but this release only supports up to version %d. " +
                        "Upgrade to a compatible release.", version, SNAPSHOT_HEADER_VERSION);
                throw new IOException(message);
            }

            if (size < SNAPSHOT_HEADER_SIZE + CRC_SIZE)
                throw new IOException("Snapshot file is truncated, file too small to contain header and CRC check");

            long dataSize = size - SNAPSHOT_HEADER_SIZE - CRC_SIZE;
            channel.position(SNAPSHOT_HEADER_SIZE);
            return new CrcValidatingInputStream(channel, dataSize);
        } catch (Throwable t) {
            try {
                channel.close();
            } catch (IOException e) {
                t.addSuppressed(e);
            }
            throw t;
        }
    }

    private void write(InputStream snapshot) throws IOException {
        OutputStream os = stage();
        try (os) {
            byte[] buf = new byte[1 << 20];
            int read;
            while (((read = snapshot.read(buf))) > 0) {
                os.write(buf, 0, read);
            }
        }

        commit(os);
    }

    private Path snapshotPath() {
        return logDir.toPath().resolve(SNAPSHOT_FILE_NAME);
    }

    public static boolean isSnapshotFile(ByteBuffer bb) {
        return bb.remaining() >= 4
                && bb.get(0) == SNAPSHOT_HEADER_MAGIC[0]
                && bb.get(1) == SNAPSHOT_HEADER_MAGIC[1]
                && bb.get(2) == SNAPSHOT_HEADER_MAGIC[2]
                && bb.get(3) == SNAPSHOT_HEADER_MAGIC[3];
    }

    @Override
    public OutputStream stage() throws IOException {
        // Cleanup files from failed previous attempts.
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(logDir.toPath(), "staged-snapshot-*.tmp")) {
            for (Path path : ds) {
                Files.deleteIfExists(path);
            }
        }

        Path tempFile = Files.createTempFile(logDir.toPath(), "staged-snapshot-", ".tmp");
        try {
            return new SnapshotOutputStream(tempFile);
        } catch (IOException e) {
            Files.deleteIfExists(tempFile);
            throw e;
        }
    }

    @Override
    public void commit(OutputStream staged) throws IOException {
        if (!(staged instanceof SnapshotOutputStream sos))
            throw new IllegalArgumentException("Unknown output stream type: " + staged);

        closeReadChannel();
        Files.move(sos.path(), snapshotPath(), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private static final class SnapshotOutputStream extends OutputStream {

        private final Path path;
        private final FileChannel channel;
        private final OutputStream fileOut;
        private final CheckedOutputStream checkedOut;
        private boolean closed;

        public SnapshotOutputStream(Path path) throws IOException {
            FileChannel ch = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE);

            // First bytes are the file header.
            ByteBuffer hdr = ByteBuffer.wrap(HEADER_BUFFER.array(), 0, SNAPSHOT_HEADER_SIZE);
            while (hdr.hasRemaining()) {
                ch.write(hdr);
            }

            this.path = path;
            this.channel = ch;
            this.fileOut = Channels.newOutputStream(ch);

            // We utilize the checked stream with a CRC32 mechanism to validate.
            // We validate only the snapshot content, it skips the header.
            this.checkedOut = new CheckedOutputStream(fileOut, new CRC32C());
        }

        @Override
        public void write(int b) throws IOException {
            checkedOut.write(b);
        }

        @Override
        public void write(byte[] b) throws IOException {
            checkedOut.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            checkedOut.write(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            checkedOut.flush();
        }

        @Override
        public void close() throws IOException {
            if (closed)
                return;

            closed = true;

            // The last 4 bytes will be the 32 bits checksum.
            int checksum = (int) (checkedOut.getChecksum().getValue() & 0xFFFFFFFFL);
            byte[] trailer = new byte[CRC_SIZE];
            trailer[0] = (byte) (checksum >>> 24);
            trailer[1] = (byte) (checksum >>> 16);
            trailer[2] = (byte) (checksum >>> 8);
            trailer[3] = (byte) checksum;

            // Trailer bytes goes directly into the file, they are not include through CRC checking.
            fileOut.write(trailer);

            // We ensure everything is flushed to disk on THIS thread.
            // Therefore, we flush manually and we close the backing file channel to ensure everything goes to disk.
            // This ensures that we don't have data in the page cache.
            channel.force(false);
            fileOut.close();
            channel.close();
        }

        public Path path() {
            return path;
        }
    }

    /**
     * Bounds reads to the data region and validates the trailing CRC checksum when the data is exhausted.
     *
     * <p>
     * CRC validation fires from {@link #read()} when all data bytes have been consumed and the stream reads the trailing
     * checksum. Closing without fully consuming the stream does not trigger a complete validation.
     * </p>
     */
    private static final class CrcValidatingInputStream extends InputStream {

        private final FileChannel channel;
        private final InputStream delegate;
        private final CRC32C crc;

        private long remaining;
        private boolean validated;

        public CrcValidatingInputStream(FileChannel channel, long dataSize) {
            this.channel = channel;
            this.delegate = Channels.newInputStream(channel);
            this.crc = new CRC32C();
            this.remaining = dataSize;
        }

        @Override
        public int read() throws IOException {
            if (remaining <= 0)
                return validateAndTerminate();

            int b = delegate.read();
            if (b < 0)
                return validateAndTerminate();

            crc.update(b);
            remaining--;
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (len == 0)
                return 0;

            if (remaining <= 0)
                return validateAndTerminate();

            int toRead = (int) Math.min(len, remaining);
            int n = delegate.read(b, off, toRead);
            if (n <= 0)
                return validateAndTerminate();

            crc.update(b, off, n);
            remaining -= n;
            return n;
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }

        private int validateAndTerminate() throws IOException {
            if (validated)
                return -1;

            validated = true;
            byte[] trailer = delegate.readNBytes(CRC_SIZE);
            if (trailer.length < CRC_SIZE)
                throw new IOException("Snapshot file is truncated, unable to read CRC trailer");

            int stored = ByteBuffer.wrap(trailer).getInt();
            int computed = (int) (crc.getValue() & 0xFFFFFFFFL);

            if (stored != computed) {
                String message = String.format("CRC mismatch in snapshot file: expected CRC 0x%08X, but found 0x%08X. " +
                                "The snapshot may be corrupted. " +
                                "Run 'raft log verify' for diagnostics before taking corrective action.",
                        computed, stored);
                throw new IOException(message);
            }
            return -1;
        }
    }
}
