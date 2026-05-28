package org.jgroups.raft.cli.commands.log;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.List;

/**
 * Orchestrates a sequence of {@link LogValidatorRule} checks against a log directory.
 *
 * <p>
 * Rules execute in registration order, threading an immutable {@link ValidationContext} through the chain. Each rule receives
 * the context produced by its predecessor, allowing downstream rules to read domain facts recorded by earlier rules. If a
 * rule throws an {@link IOException}, the engine records the failure as a {@link Violation} and continues with the remaining
 * rules. This ensures that a single unreadable file does not prevent inspection of the others.
 * </p>
 *
 * <p>
 * Callers interact only with {@link #validate(File, LogValidationOptions)}}. The rule set, execution order, and context
 * threading are internal concerns. Usage:
 *
 * <pre>{@code
 * LogValidationOptions options = ...; // LogValidationOptions.simple() or with a custom callback.
 * ValidationResult result = LogValidation.validate(directory, options);
 * result.formatTo(out);
 * return result.exitCode();
 * }</pre>
 * </p>
 *
 * <p>
 * The returned result exposes three things the caller needs:
 * <ul>
 *   <li>{@link ValidationResult#isValid()} — whether all files are healthy.</li>
 *   <li>{@link ValidationResult#exitCode()} — the appropriate process exit code:
 *       {@code 0} when clean, {@code 1} for corruption or inconsistency, {@code 2} for invalid input such as an unsupported
 *       file format.</li>
 *   <li>{@link ValidationResult#formatTo(PrintWriter)} — a human-readable summary of all findings, suitable for direct
 *       display to the operator.</li>
 * </ul>
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 */
public final class LogValidation {

    private final File directory;
    private final List<LogValidatorRule> rules;

    private LogValidation(File directory, List<LogValidatorRule> rules) {
        this.directory = directory;
        this.rules = Collections.unmodifiableList(rules);
    }

    /**
     * Validates the log files existing the given directory.
     *
     * <p>
     * The rule set and execution order are chosen to satisfy cross-file dependencies: the entries file is scanned first
     * so that its log range and highest term are available for metadata consistency checks.
     * </p>
     *
     * <p>
     * Validation only applies to instances utilizing {@link org.jgroups.protocols.raft.FileBasedLog} as the persistent
     * log for Raft. Legacy files that don't contain checksums are also skipped.
     * </p>
     *
     * @param directory the log directory to validate
     * @param options options to customize the validation behavior
     * @return a configured engine ready to run
     */
    public static ValidationResult validate(File directory, LogValidationOptions options) {
        LogValidation lv = new LogValidation(directory, List.of(
                new EntriesFileRule(),
                new SnapshotFileRule(),
                new MetadataFileRule()
        ));
        return lv.validate(ValidationContext.withOptions(options));
    }

    /**
     * Runs all registered rules and returns a single composite result.
     *
     * @return the overall validation result covering all checked files
     */
    private ValidationResult validate(ValidationContext context) {
        for (LogValidatorRule rule : rules) {
            try {
                context = rule.validate(directory, context);
            } catch (IOException e) {
                String message = String.format("Failure happened while parsing the file %s. " +
                        "The existing validation was unable to perform the step for rule %s, it was skipped. " +
                        "The failure is: %s", directory.getAbsolutePath(), rule.getClass(), e);
                ValidationResult failure = FileValidationResult.builder(directory.getAbsolutePath())
                        .field("Status", FileValidationResult.ValidationField.error("ERROR"))
                        .violation(new Violation(message, Violation.Severity.ERROR))
                        .build();
                context = context.append(failure);
            }
        }

        return new CompositeValidationResult(context.results());
    }
}
