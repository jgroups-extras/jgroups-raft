package org.jgroups.raft.cli.commands.log;

import org.jgroups.raft.filelog.SnapshotStorage;
import org.jgroups.util.Util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.CRC32C;

/**
 * Validates the {@code state_snapshot.raft} file in a log directory.
 *
 * <p>
 * This rule has no dependencies on other rules and does not enrich the {@link ValidationContext} with domain facts.
 * Snapshot files are self-contained: they carry their own integrity checksum and do not participate in cross-file
 * consistency checks.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 */
final class SnapshotFileRule extends BaseFileRule {

    private static final byte MAX_SUPPORTED_VERSION = 2;
    private static final int MIN_FILE_SIZE = SnapshotStorage.SNAPSHOT_HEADER_SIZE + SnapshotStorage.CRC_SIZE;

    SnapshotFileRule() {
        super(SnapshotStorage.SNAPSHOT_FILE_NAME);
    }

    @Override
    ValidationContext proceedValidation(Path path, ValidationContext context) throws IOException {
        // Read the full file in-memory for assertion.
        // If this turns to be a problem, we could load the header for parsing, and CRC the snapshot content in chunks.
        // Taking the simple approach for now.
        byte[] bytes = Files.readAllBytes(path);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        if (!SnapshotStorage.isSnapshotFile(buffer)) {
            String message = String.format("Unrecognized file format for snapshot at %s", path.toAbsolutePath());
            ValidationResult vr = FileValidationResult.builder(path.toAbsolutePath().toString())
                    .field("Status", FileValidationResult.ValidationField.error("UNRECOGNIZED"))
                    .violation(new Violation(message, Violation.Severity.INVALID))
                    .build();
            return context.append(vr);
        }

        byte version = buffer.get(SnapshotStorage.SNAPSHOT_HEADER_MAGIC.length);
        if (version < 1 || version > MAX_SUPPORTED_VERSION) {
            String message = String.format("Snapshot version %d is not supported. " +
                    "This CLI release supports up to version %d. Upgrade to a compatible release.", version, MAX_SUPPORTED_VERSION);
            ValidationResult vr = FileValidationResult.builder(path.toAbsolutePath().toString())
                    .field("Format", String.format("v%d", version))
                    .violation(new Violation(message, Violation.Severity.INVALID))
                    .build();
            return context.append(vr);
        }

        if (bytes.length < MIN_FILE_SIZE) {
            String message = String.format("Snapshot file is truncated: %d bytes present, minimum %d required " +
                            "for header and checksum. The file may have been cut short during a write. Do not delete " +
                            "the snapshot file without verifying another node in the cluster holds a valid copy. " +
                            "If a healthy leader is available, deletion through the repair tool should be safe as " +
                            "the node will replicate the state on start. Otherwise, data could be lost.",
                    bytes.length, MIN_FILE_SIZE);
            ValidationResult vr = FileValidationResult.builder(path.toAbsolutePath().toString())
                    .field("Status", FileValidationResult.ValidationField.error("TRUNCATED"))
                    .violation(new Violation(message, Violation.Severity.ERROR))
                    .build();
            return context.append(vr);
        }

        int dataLength = bytes.length - SnapshotStorage.SNAPSHOT_HEADER_SIZE - SnapshotStorage.CRC_SIZE;
        CRC32C crc = new CRC32C();
        crc.update(bytes, SnapshotStorage.SNAPSHOT_HEADER_SIZE, dataLength);
        int computedChecksum = (int) (crc.getValue() & 0xFFFFFFFFL);

        buffer.position(bytes.length - SnapshotStorage.CRC_SIZE);
        int storedChecksum = buffer.getInt();

        FileValidationResult.ValidationResultBuilder builder = FileValidationResult.builder(path.toAbsolutePath().toString())
                .field("Format", String.format("v%d (with checksums)", version))
                .field("Size", Util.printBytes(dataLength));

        if (storedChecksum != computedChecksum) {
            String message = String.format("CRC mismatch in snapshot: stored checksum 0x%08X, computed 0x%08X. " +
                    "The snapshot data may have been corrupted after write. " +
                    "Do not delete the snapshot file without verifying another node in the cluster holds a valid copy. " +
                    "If a healthy leader is currently available, deletion through the repair tool should be safe as " +
                    "the node will replicate the state on start. Otherwise, data could be lost.",
                    storedChecksum, computedChecksum);
            builder.field("Status", FileValidationResult.ValidationField.error("Corrupted"));
            builder.violation(new Violation(message, Violation.Severity.ERROR));

        } else {
            builder.field("Status", FileValidationResult.ValidationField.info("OK (checksum valid)"));
        }

        return context.append(builder.build());
    }
}
