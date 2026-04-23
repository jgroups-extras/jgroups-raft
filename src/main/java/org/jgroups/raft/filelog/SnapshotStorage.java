package org.jgroups.raft.filelog;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32C;

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
public final class SnapshotStorage {

    private static final byte[] SNAPSHOT_HEADER_MAGIC = {'S', 'N', 'A', 'P'};
    private static final byte SNAPSHOT_HEADER_VERSION = 2;
    private static final int SNAPSHOT_HEADER_SIZE = 8;
    private static final int CRC_SIZE = 4;
    private static final ByteBuffer HEADER_BUFFER = ByteBuffer.allocate(SNAPSHOT_HEADER_SIZE);
    private static final ByteBuffer CRC_BUFFER = ByteBuffer.allocate(CRC_SIZE);

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

    private static final String SNAPSHOT_FILE_NAME = "state_snapshot.raft";

    private final File logDir;
    private final CRC32C crc;

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
    public void writeSnapshot(ByteBuffer snapshot) throws IOException {
        Path snapshotPath = snapshotPath();
        if (Files.exists(snapshotPath)) {
            // Since the file already exist, we keep it as is.
            // We write to a temporary file first.
            Path tmp = Files.createTempFile(logDir.toPath(), null, null);
            write(snapshot, tmp);
            // Then we perform an atomic move to replace the existing file.
            Files.move(tmp, snapshotPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            return;
        }

        // If this is a new file, we just write it.
        write(snapshot, snapshotPath);
    }

    /**
     * Returns the most recently stored snapshot.
     *
     * <p>
     * If the file starts with the {@code "SNAP"} magic, the header version is validated and the trailing CRC-32C checksum
     * is verified against the snapshot data. Legacy snapshots (no header) are returned as-is without CRC validation.
     * </p>
     *
     * @return the snapshot data, or {@code null} if no snapshot file exists
     * @throws IOException if the file has an unsupported version, a CRC mismatch, or cannot be read
     */
    public ByteBuffer readSnapshot() throws IOException {
        Path snapshotPath = snapshotPath();
        if (!Files.exists(snapshotPath)) {
            return null;
        }

        // Dangerously read the full snapshot into memory.
        // This might contain the header and trailer information.
        byte[] fileBytes = Files.readAllBytes(snapshotPath);
        ByteBuffer buf = ByteBuffer.wrap(fileBytes);

        if (isSnapshotFile(buf)) {
            // The file version is right after the magic bytes.
            byte version = buf.get(SNAPSHOT_HEADER_MAGIC.length);
            if (version < 1 || version > SNAPSHOT_HEADER_VERSION) {
                String message = String.format("Snapshot has version %d, but this release only supports up to version %d. " +
                        "Upgrade to a compatible release, or run 'raft snapshot downgrade' to convert the snapshot file.",
                        version, SNAPSHOT_HEADER_VERSION);
                throw new IOException(message);
            }

            if (fileBytes.length < SNAPSHOT_HEADER_SIZE + CRC_SIZE)
                throw new IOException("Snapshot file is truncated, file too small to contain the header and CRC check");

            // Regenerate the checksum for the written snapshot data.
            int dataLength = fileBytes.length - SNAPSHOT_HEADER_SIZE - CRC_SIZE;
            crc.update(fileBytes, SNAPSHOT_HEADER_SIZE, dataLength);
            int checksum = (int) (crc.getValue() & 0xFFFFFFFFL);

            // Read the written checksum written at the end of file.
            buf.position(fileBytes.length - CRC_SIZE);
            int stored = buf.getInt();

            // Reset the CRC before performing any checks to avoid leaving it dirty.
            crc.reset();

            if (stored != checksum) {
                String message = String.format(
                        "CRC mismatch in snapshot file: expected CRC 0x%08X, but found 0x%08X. " +
                                "The snapshot may be corrupted. " +
                                "Run 'raft log verify' for diagnostics before taking corrective action.",
                        checksum, stored);
                throw new IOException(message);
            }

            // Re-wrap the buffer to contain only the actual data.
            // Discards the header 8 bytes, and trim the last 4 bytes.
            buf = ByteBuffer.wrap(fileBytes, SNAPSHOT_HEADER_SIZE, dataLength);
        }

        return buf;
    }

    private void write(ByteBuffer snapshot, Path path) throws IOException {
        try (ByteChannel ch = Files.newByteChannel(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            // The buffer will always be at the end.
            // We can just flip it to consume the content when writing.
            HEADER_BUFFER.flip();

            // First bytes are the file header.
            ch.write(HEADER_BUFFER);

            // We calculate the CRC from the provided snapshot.
            // CRC will "consume" the buffer, so we need to return to the expected position.
            int position = snapshot.position();
            crc.update(snapshot);

            // We restart the position and write the content to the underlying file.
            snapshot.position(position);
            ch.write(snapshot);

            // The last 4 bytes will be the 32 bits checksum.
            int checksum = (int) (crc.getValue() & 0xFFFFFFFFL);
            CRC_BUFFER.position(0);
            CRC_BUFFER.putInt(checksum);
            CRC_BUFFER.flip();
            ch.write(CRC_BUFFER);
        } finally {
            // Reset the CRC to be utilized again in the next call.
            crc.reset();
        }
    }

    private Path snapshotPath() {
        return logDir.toPath().resolve(SNAPSHOT_FILE_NAME);
    }

    private static boolean isSnapshotFile(ByteBuffer bb) {
        return bb.remaining() >= 4
                && bb.get(0) == SNAPSHOT_HEADER_MAGIC[0]
                && bb.get(1) == SNAPSHOT_HEADER_MAGIC[1]
                && bb.get(2) == SNAPSHOT_HEADER_MAGIC[2]
                && bb.get(3) == SNAPSHOT_HEADER_MAGIC[3];
    }
}
