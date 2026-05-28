package org.jgroups.raft.cli.commands.log;

import org.jgroups.raft.filelog.LogEntryStorage;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;

/**
 * Validates the {@code entries.raft} file in a log directory.
 *
 * <p>
 * This rule has no dependencies on other rules and should execute first in the chain. It enriches the {@link ValidationContext}
 * with three domain facts consumed by downstream rules:
 *
 * <ul>
 *   <li>{@link ValidationContext#firstLogIndex()}: first entry index in the log</li>
 *   <li>{@link ValidationContext#lastLogIndex()}: last entry index in the log</li>
 *   <li>{@link ValidationContext#highestTerm()}: highest term across all entries</li>
 * </ul>
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 */
final class EntriesFileRule implements LogValidatorRule {

    private static final String ENTRIES_FILE = "entries.raft";

    @Override
    public ValidationContext validate(File directory, ValidationContext context) throws IOException {
        Path entriesPath = directory.toPath().resolve(ENTRIES_FILE);
        FormatDetector.Format format = FormatDetector.detect(entriesPath);

        return switch (format) {
            case MISSING -> {
                String message = String.format("File not found at %s. " +
                        "The entry directory exists but contains no entry file. If this is a fresh node that has not " +
                        "yet joined the cluster, this is expected. Otherwise, compare with other nodes in the cluster " +
                        "to determine whether the file was mistakenly deleted.", entriesPath.toAbsolutePath());
                ValidationResult vr = FileValidationResult.builder(entriesPath.toAbsolutePath().toString())
                        .field("Status", FileValidationResult.ValidationField.warn("MISSING"))
                        .violation(new Violation(message, Violation.Severity.WARNING))
                        .build();
                yield context.append(vr);
            }
            case EMPTY -> {
                String message = String.format("File is empty at %s. If this is a fresh node, this is expected. " +
                        "Otherwise, compare with other nodes to determine whether entries were lost.", entriesPath.toAbsolutePath());
                ValidationResult vr = FileValidationResult.builder(entriesPath.toAbsolutePath().toString())
                        .field("Status", FileValidationResult.ValidationField.warn("EMPTY"))
                        .violation(new Violation(message, Violation.Severity.WARNING))
                        .build();
                yield context.append(vr);
            }
            case UNRECOGNIZED -> {
                String message = String.format("Unrecognized file format for file at %s. " +
                        "The file does not match any known Raft log format. Verify this is the correct log directory. " +
                        "If the file is corrupted beyond recognition, delete and restart the node. " +
                        "The node should join as a fresh member and replicate the state again.", entriesPath.toAbsolutePath());
                ValidationResult vr = FileValidationResult.builder(entriesPath.toAbsolutePath().toString())
                        .field("Status", FileValidationResult.ValidationField.error("UNRECOGNIZED"))
                        .violation(new Violation(message, Violation.Severity.INVALID))
                        .build();
                yield context.append(vr);
            }
            case V1 -> {
                String message = "The log is in v1 format. The CLI tool requires v2 format with checksums " +
                        "for integrity verification. " +
                        "Start the node with a 2.x release to upgrade the log format automatically. New entries will " +
                        "be written with checksums.";
                ValidationResult vr = FileValidationResult.builder(entriesPath.toAbsolutePath().toString())
                        .field("Format", FileValidationResult.ValidationField.warn("v1 (no checksums)"))
                        .violation(new Violation(message, Violation.Severity.INVALID))
                        .build();
                yield context.append(vr);
            }
            case V2 -> scanEntries(entriesPath, context);
        };
    }

    private ValidationContext scanEntries(Path entriesPath, ValidationContext context) throws IOException {
        ScanResult scan = Scanner.scan(entriesPath, context.options());

        FileValidationResult.ValidationResultBuilder builder = FileValidationResult.builder(entriesPath.toAbsolutePath().toString())
                .field("Format", "v2 (with checksums)");

        if (scan.entryCount() > 0) {
            builder.field("Entries", String.format("%d - %d (%d entries)",
                    scan.firstIndex(), scan.lastIndex(), scan.entryCount()));

            if (scan.legacyCount() > 0) {
                builder.field("Legacy", String.format("%d entries (no checksums)", scan.legacyCount()));
            }

            context = context
                    .withLogRange(scan.firstIndex(), scan.lastIndex())
                    .withHighestTerm(scan.highestTerm());
        }

        if (scan.violations().isEmpty()) {
            builder.field("Status", FileValidationResult.ValidationField.info("OK (all checksums valid)"));
        } else {
            builder.field("Status", FileValidationResult.ValidationField.error("CORRUPTED"));
            builder.violations(scan.violations());
        }

        return context.append(builder.build());
    }

    /**
     * Identifies the file format of {@code entries.raft} by reading the first bytes.
     *
     * <p>
     * Does not validate file contents beyond the initial bytes needed for format identification. Returns a {@link Format}
     * that the caller uses to decide how to proceed.
     * </p>
     */
    private static final class FormatDetector {

        enum Format {
            MISSING,
            EMPTY,
            V1,
            V2,
            UNRECOGNIZED,
        }

        private FormatDetector() { }

        /**
         * Detects the format of the given entries file.
         *
         * @param entriesPath path to the {@code entries.raft} file
         * @return the detected format
         * @throws IOException if the file exists but cannot be read
         */
        public static Format detect(Path entriesPath) throws IOException {
            File file = entriesPath.toFile();
            if (!file.isFile())
                return Format.MISSING;

            if (file.length() == 0)
                return Format.EMPTY;

            try (FileChannel ch = FileChannel.open(entriesPath, StandardOpenOption.READ)) {
                ByteBuffer buf = ByteBuffer.allocate(4);
                int read = ch.read(buf);

                // It doesn't even contain the possible header file.
                if (read < 4)
                    return Format.UNRECOGNIZED;

                buf.flip();

                if (buf.get(0) == LogEntryStorage.MAGIC_NUMBER)
                    return Format.V1;

                if (buf.get(0) == LogEntryStorage.FILE_HEADER_MAGIC[0] && buf.get(1) == LogEntryStorage.FILE_HEADER_MAGIC[1]
                        && buf.get(2) == LogEntryStorage.FILE_HEADER_MAGIC[2] && buf.get(3) == LogEntryStorage.FILE_HEADER_MAGIC[3])
                    return Format.V2;

                // There is file with content and it is neither legacy nor the new format.
                // Let's assume it is a corrupted file already instead of doing work.
                return Format.UNRECOGNIZED;
            }
        }
    }

    /**
     * Sequentially scans all entries in a v2 {@code entries.raft} file.
     *
     * <p>
     * Reads entries from the first position after the 8-byte file header to end of file.
     * For each entry, validates the header consistency and verifies the CRC-32C checksum
     * (v2 entries only; legacy entries are counted but not checksum-verified).
     * </p>
     *
     * <p>
     * The scanner continues past CRC mismatches (the entry header is still readable,
     * so the next entry position is known). It stops at invalid headers or incomplete
     * entries where the next entry position cannot be determined.
     * </p>
     */
    private static final class Scanner {

        private Scanner() { }

        /**
         * Scans all entries in the given v2 entries file.
         *
         * <p>
         * The method assumes the provided file is in the V2 format and skips any extra validation.
         * </p>
         *
         * @param entriesPath path to the {@code entries.raft} file
         * @param options runtime options to customize the parsing behavior
         * @return the scan results including entry range, counts, and any violations found
         * @throws IOException if the file cannot be read
         */
        public static ScanResult scan(Path entriesPath, LogValidationOptions options) throws IOException {
            long firstIndex = -1;
            long lastIndex = -1;
            long highestTerm = 0;
            int entryCount = 0;
            int legacyCount = 0;
            List<Violation> violations = new ArrayList<>();

            try (FileChannel ch = FileChannel.open(entriesPath, StandardOpenOption.READ)) {
                long fileSize = ch.size();
                long position = LogEntryStorage.FILE_HEADER_SIZE;

                ByteBuffer buffer = ByteBuffer.allocate(LogEntryStorage.HEADER_SIZE);
                while (position < fileSize) {
                    // File is truncated at the last entry.
                    if (position + LogEntryStorage.HEADER_SIZE > fileSize) {
                        String message = String.format("Truncated entry at offset %d: only %d of %d header bytes present. " +
                                        "The file may have been cut short during a write. " +
                                        "The incomplete trailing bytes can be removed with the repair command.",
                                position, fileSize - position, LogEntryStorage.HEADER_SIZE);
                        violations.add(new Violation(message, Violation.Severity.ERROR));
                        break;
                    }

                    // Read only the entry header at this point.
                    ch.read(buffer, position);
                    buffer.flip();

                    byte magic = buffer.get();
                    int totalLength = buffer.getInt();
                    long term = buffer.getLong();
                    long index = buffer.getLong();
                    boolean internal = buffer.get() != 0;
                    int dataLength = buffer.getInt();

                    // The FileBasedLog allocates the data in chunks, with 0x00 padding after the last actual entry.
                    // If after reading the next chunk, the magic byte, which is the first byte of the chunk, is 0x00,
                    // it means everything else is just padding.
                    // We can exit now because the file is effectively parsed.
                    if (magic == 0x00)
                        break;

                    if (magic != LogEntryStorage.MAGIC_NUMBER && magic != LogEntryStorage.MAGIC_NUMBER_CRC) {
                        String message = String.format("Unrecognized entry magic byte 0x%02X at offset %d (expected 0x01 or 0x02). " +
                                        "This may indicate file corruption or a misaligned read. " +
                                        "Entries before this offset (%d scanned) appear intact. " +
                                        "Run the repair tool to truncate the log at this offset, preserving the %d intact " +
                                        "entries. The node will replicate the entries from the leader on restart.",
                                magic, position, entryCount, entryCount);
                        violations.add(new Violation(message, Violation.Severity.ERROR));
                        break;
                    }

                    // Legacy entries are not verified with CRC.
                    int expectedLength = magic == LogEntryStorage.MAGIC_NUMBER_CRC
                            ? LogEntryStorage.HEADER_SIZE + dataLength + LogEntryStorage.CRC_SIZE
                            : LogEntryStorage.HEADER_SIZE + dataLength;

                    // Verify the actual header for "consistency".
                    // These are more as invariant on the expected values contained in an entry header.
                    if (totalLength != expectedLength || term <= 0 || index <= 0
                            || dataLength < 0 || totalLength < LogEntryStorage.HEADER_SIZE) {
                        String message = String.format("Invalid entry header at offset %d: totalLength=%d (expected %d), " +
                                        "term=%d, index=%d, dataLength=%d. " +
                                        "One or more header fields are out of range, indicating corruption. " +
                                        "Entries before this offset (%d scanned) appear intact.",
                                position, totalLength, expectedLength, term, index, dataLength, entryCount);
                        violations.add(new Violation(message, Violation.Severity.ERROR));
                        break;
                    }

                    // Verify if the entry was truncated.
                    // The file has finished without the full entry + CRC checksum.
                    if (position + totalLength > fileSize) {
                        String message = String.format("Incomplete entry %d (term %d) at offset %d: header declares %d bytes " +
                                        "but only %d remain in the file. The last write was likely interrupted. " +
                                        "The incomplete trailing bytes can be removed with the repair command.",
                                index, term, position, totalLength, fileSize - position);
                        violations.add(new Violation(message, Violation.Severity.ERROR));
                        break;
                    }

                    // Make it ready to be consumed by the next iteration.
                    buffer.flip();

                    byte[] payload = new byte[dataLength];

                    // We can skip extra reads in case the entry was empty.
                    // But we still continue to verify the CRC for an empty entry anyway.
                    if (dataLength > 0) {
                        ByteBuffer data = ByteBuffer.wrap(payload);
                        ch.read(data, position + LogEntryStorage.HEADER_SIZE);
                    }

                    EntryCallback.CrcStatus status;
                    if (magic == LogEntryStorage.MAGIC_NUMBER_CRC) {
                        ByteBuffer crcBuff = ByteBuffer.allocate(LogEntryStorage.CRC_SIZE);
                        ch.read(crcBuff, position + LogEntryStorage.HEADER_SIZE + dataLength);
                        crcBuff.flip();

                        int storedChecksum = crcBuff.getInt();

                        CRC32C crc = new CRC32C();
                        crc.update(buffer);
                        crc.update(payload);

                        int computedChecksum = (int) crc.getValue();

                        // After consuming in CRC, we reset the buffer to use in the next iteration.
                        buffer.flip();

                        if (storedChecksum != computedChecksum) {
                            status = new EntryCallback.CrcStatus.Mismatch(storedChecksum, computedChecksum);
                            String message = String.format("CRC mismatch on entry %d (term %d) at offset %d: " +
                                            "stored checksum 0x%08X, computed 0x%08X. " +
                                            "The entry data may have been corrupted after write. " +
                                            "Run the repair command to truncate the log from this entry onward. " +
                                            "The node will replicate the missing entries from the leader.",
                                    index, term, position, storedChecksum, computedChecksum);
                            violations.add(new Violation(message, Violation.Severity.ERROR));
                        } else {
                            status = new EntryCallback.CrcStatus.Ok();
                        }
                    } else {
                        status = new EntryCallback.CrcStatus.Legacy();
                        legacyCount++;
                    }

                    options.callback().onEntry(index, term, internal, dataLength, magic, status, payload);

                    if (firstIndex < 0) firstIndex = index;
                    lastIndex = index;
                    if (term > highestTerm) highestTerm = term;
                    entryCount++;

                    position += totalLength;
                }
            }

            return new ScanResult(firstIndex, lastIndex, highestTerm, entryCount, legacyCount, violations);
        }
    }

    /**
     * Aggregated results from scanning all entries in the file.
     *
     * @param firstIndex  the first entry index found, or {@code -1} if no entries
     * @param lastIndex   the last entry index found, or {@code -1} if no entries
     * @param highestTerm the highest term across all scanned entries
     * @param entryCount  total number of entries scanned (including corrupted)
     * @param legacyCount number of legacy (v1) entries without CRC
     * @param violations  problems found during scanning
     */
    private record ScanResult(
            long firstIndex,
            long lastIndex,
            long highestTerm,
            int entryCount,
            int legacyCount,
            List<Violation> violations
    ) { }
}
