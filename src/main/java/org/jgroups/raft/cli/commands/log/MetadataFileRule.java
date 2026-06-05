package org.jgroups.raft.cli.commands.log;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.raft.filelog.MetadataStorage;
import org.jgroups.util.ByteBufferInputStream;
import org.jgroups.util.Util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.OptionalLong;

/**
 * Validates the {@code metadata.raft} file in a log directory.
 *
 * <p>
 * Performs structural checks (minimum file size, votedFor deserialization) and cross-file consistency checks against domain
 * facts recorded by earlier rules in the {@link ValidationContext}. Metadata files have no checksums, so integrity validation
 * is limited to structure and cross-file invariants.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 */
final class MetadataFileRule extends BaseFileRule {

    private static final int MIN_FILE_SIZE = Global.LONG_SIZE + Global.LONG_SIZE;

    MetadataFileRule() {
        super(MetadataStorage.FILE_NAME);
    }

    @Override
    ValidationContext proceedValidation(Path path, ValidationContext context) throws IOException {
        // Metadata file is only a few bytes.
        byte[] bytes = Files.readAllBytes(path);

        if (bytes.length < MIN_FILE_SIZE) {
            String message = String.format("Metadata file is truncated: %d bytes present, minimum %d required " +
                            "for commit index and current term. The file may have been cut short during a write. " +
                            "Use the repair tool to reconstruct the metadata from the entries file.",
                    bytes.length, MIN_FILE_SIZE);
            ValidationResult vr = FileValidationResult.builder(path.toAbsolutePath().toString())
                    .field("Status", FileValidationResult.ValidationField.error("TRUNCATED"))
                    .violation(new Violation(message, Violation.Severity.ERROR))
                    .metadataInfo(new ValidationResult.MetadataInfo.Truncated())
                    .build();
            return context.append(vr);
        }

        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        long commitIndex = buffer.getLong();
        long currentTerm = buffer.getLong();
        boolean hasErrors = false;

        FileValidationResult.ValidationResultBuilder builder =
                FileValidationResult.builder(path.toAbsolutePath().toString())
                        .field("Commit index", String.valueOf(commitIndex))
                        .field("Current term", String.valueOf(currentTerm));

        // The last field has a variable size.
        // It contains the address of the last voted for node, and it could be empty.
        ValidationResult.MetadataInfo.VoteStatus vs = ValidationResult.MetadataInfo.VoteStatus.VALID;
        if (bytes.length > MIN_FILE_SIZE) {
            try {
                int addressLength = buffer.getInt();
                if (addressLength < 0 || MIN_FILE_SIZE + Global.INT_SIZE + addressLength > bytes.length) {
                    builder.field("Vote for", FileValidationResult.ValidationField.error("UNREADABLE"));
                    String message = String.format("Voted for address length is invalid or extends beyond file boundary. " +
                            "Expected additional %d bytes, but there is only %d bytes remaining. " +
                            "The metadata file may have been partially written or corrupted. Run the repair tool to " +
                            "clear the voted field. Clearing the voted field in a stable cluster is a safe operation.",
                            addressLength, buffer.remaining());
                    builder.violation(new Violation(message, Violation.Severity.ERROR));
                    vs = ValidationResult.MetadataInfo.VoteStatus.CORRUPT;
                    hasErrors = true;
                } else {
                    ByteBuffer addressBuffer = ByteBuffer.wrap(bytes, MIN_FILE_SIZE + Global.INT_SIZE, addressLength);
                    Address votedFor = Util.readAddress(new ByteBufferInputStream(addressBuffer));
                    builder.field("Voted for", Objects.toString(votedFor));
                }
            } catch (Exception e) {
                builder.field("Voted for", FileValidationResult.ValidationField.error("UNREADABLE"));
                String message = String.format("Failed to deserialize voted for address: %s", e);
                builder.violation(new Violation(message, Violation.Severity.ERROR));
                vs = ValidationResult.MetadataInfo.VoteStatus.CORRUPT;
                hasErrors = true;
            }
        } else {
            builder.field("Voted for", "none");
            vs = ValidationResult.MetadataInfo.VoteStatus.ABSENT;
        }

        // Utilize information for the entries file and perform a cross-check validation.
        // Metadata files should automatically reflect the information of replicated entries.
        OptionalLong lastLogIndex = context.lastLogIndex();
        OptionalLong highestTerm = context.highestTerm();

        if (lastLogIndex.isPresent() && commitIndex > lastLogIndex.getAsLong()) {
            String message = String.format("Commit index %d is beyond the last log entry index %d. " +
                            "This may indicate the log file was truncated or it was modified outside of normal operation. " +
                            "It is unknown whether the metadata is incorrect or the log file is truncated. " +
                            "Compare with the log directories of other nodes in the cluster to determine whether " +
                            "entries are missing from this log or the commit index in metadata is stale.",
                    commitIndex, lastLogIndex.getAsLong());
            builder.violation(new Violation(message, Violation.Severity.ERROR));
            hasErrors = true;
        }

        if (highestTerm.isPresent() && currentTerm < highestTerm.getAsLong()) {
            String message = String.format("Current term %d in metadata is lower than the highest term %d found in the " +
                            "log entries. The term values should be at least as high as the terms found in the entries. " +
                            "This indicates issues when flushing the metadata after entries were written. An outdated " +
                            "metadata could disrupt leader election. Run the repair command to update the metadata to " +
                            "match the entries file.",
                    currentTerm, highestTerm.getAsLong());
            builder.violation(new Violation(message, Violation.Severity.ERROR));
            hasErrors = true;
        }

        builder.metadataInfo(new ValidationResult.MetadataInfo.Readable(commitIndex, currentTerm, vs));
        if (!hasErrors) {
            builder.field("Consistency", FileValidationResult.ValidationField.info("OK"));
        } else {
            builder.field("Consistency", FileValidationResult.ValidationField.error("Failures found"));
        }

        return context.append(builder.build());
    }
}
