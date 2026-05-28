package org.jgroups.raft.cli.commands.log;

import java.io.PrintWriter;
import java.util.Optional;

/**
 * The outcome of validating a single file in the log directory.
 *
 * <p>
 * Each result carries both discovered facts (as labeled fields) and any problems found (as {@link Violation}s). The
 * {@link #formatTo(PrintWriter)} method renders a human-readable summary section for the operator. The caller has access
 * to:
 *
 * <ul>
 *   <li>{@link #isValid()}: check for a healthy directory.</li>
 *   <li>{@link #exitCode()}: process exit code reflecting the worst severity found: {@code 0} (clean), {@code 1} (corruption),
 *       or {@code 2} (invalid input).</li>
 *   <li>{@link #formatTo(PrintWriter)}: renders a human-readable summary to the given writer, including file status, field
 *       values, and problem descriptions.</li>
 * </ul>
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 */
public sealed interface ValidationResult permits CompositeValidationResult, FileValidationResult {

    /**
     * Returns {@code true} if no violations were found.
     *
     * @return {@code true} when the validated file is healthy
     */
    boolean isValid();

    int exitCode();

    /**
     * Writes a human-readable summary of this validation to the given writer.
     *
     * <p>
     * The output should include the file name, discovered fields, and any violations. The written information should be
     * as descriptive as possible and actionable. The goal is for operators to know and take an informed decision.
     * </p>
     *
     * @param out the writer to print the summary to
     */
    void formatTo(PrintWriter out);

    /**
     * Returns the first corruption point found in the entries file, if any.
     *
     * @return the first corruption, or empty if all entries are intact
     */
    Optional<CorruptionPoint> firstCorruption();

    /**
     * Returns the log entry range and counts from the entries file, if scanned.
     *
     * @return the log info, or empty if the entries file was not scanned
     */
    Optional<LogInfo> logInfo();

    /**
     * Returns the metadata values read from the metadata file, if available.
     *
     * @return the metadata info, or empty if the metadata file was not readable
     */
    Optional<MetadataInfo> metadataInfo();

    /**
     * A corruption finding at a known file offset.
     *
     * @param offset file offset where the corruption starts
     * @param index  Raft log index of the corrupt entry, or {@code -1} if the header was unreadable
     * @param term   term of the corrupt entry, or {@code -1} if the header was unreadable
     * @param type   the kind of corruption detected
     */
    record CorruptionPoint(long offset, long index, long term, Type type) {
        public enum Type {
            CRC_MISMATCH,
            INCOMPLETE_ENTRY,
            TRUNCATED_HEADER,
            INVALID_MAGIC,
            INVALID_HEADER,
        }
    }

    /**
     * Log entry range and counts from the entries file scan.
     *
     * @param firstIndex  first Raft log index in the file
     * @param lastIndex   last Raft log index in the file
     * @param entryCount  total entries scanned
     * @param highestTerm highest term across all entries
     */
    record LogInfo(long firstIndex, long lastIndex, int entryCount, long highestTerm) { }

    /**
     * Values read from the metadata file.
     *
     * @param commitIndex the stored commit index
     * @param currentTerm the stored current term
     */
    record MetadataInfo(long commitIndex, long currentTerm) { }
}
