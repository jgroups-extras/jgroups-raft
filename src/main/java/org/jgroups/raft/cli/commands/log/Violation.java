package org.jgroups.raft.cli.commands.log;

/**
 * An immutable description of a problem found during log validation.
 *
 * <p>
 * Violations are display-oriented: they carry a human-readable message for the operator. Machine-readable data needed by
 * subsequent validation rules or repair logic belongs in {@link ValidationContext}, not here.
 * </p>
 *
 * <p>
 * The {@link Severity} level determines the process exit code when the violation is the most severe finding in the result:
 *
 * <ul>
 *   <li>{@link Severity#WARNING}: informational, does not affect the exit code.</li>
 *   <li>{@link Severity#ERROR}: corruption or inconsistency detected (exit code 1).</li>
 *   <li>{@link Severity#INVALID}: unsupported format or input that prevents validation from proceeding (exit code 2).</li>
 * </ul>
 * </p>
 *
 * @param message descriptive, actionable message for the operator
 * @param severity the severity level of this violation
 */
record Violation(String message, Severity severity) {

    enum Severity {
        WARNING,
        ERROR,
        INVALID,
    }
}
