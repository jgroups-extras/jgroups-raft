package org.jgroups.raft.cli.commands.log;

import java.io.PrintWriter;

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
}
