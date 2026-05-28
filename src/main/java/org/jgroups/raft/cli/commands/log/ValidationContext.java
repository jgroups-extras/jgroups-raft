package org.jgroups.raft.cli.commands.log;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;

/**
 * Immutable accumulator of validation results and cross-file domain facts.
 *
 * <p>
 * As each {@link LogValidatorRule} executes, it returns a new context with its {@link ValidationResult} appended and any
 * discovered domain facts recorded. Later rules read these facts for cross-file consistency checks without depending on
 * earlier rules' concrete result types. The context is enough to transmit information forward the chain.
 * </p>
 *
 * <p>
 * Domain facts are intentionally coarse-grained: only data required by a downstream rule is promoted to a context field.
 * The context contains only internal information needed for processing. Display-only data aiming operators stays in the
 * {@link ValidationResult}.
 * </p>
 *
 * @param results       validation results collected so far, in rule execution order
 * @param firstLogIndex first entry index in {@code entries.raft}, empty if not yet scanned
 * @param lastLogIndex  last entry index in {@code entries.raft}, empty if not yet scanned
 * @param highestTerm   highest term found across all log entries, empty if not yet scanned
 */
record ValidationContext(
        List<ValidationResult> results,
        OptionalLong firstLogIndex,
        OptionalLong lastLogIndex,
        OptionalLong highestTerm,
        LogValidationOptions options
) {

    /**
     * Creates an empty context with no results or facts.
     *
     * @return a fresh context
     */
    static ValidationContext empty() {
        return withOptions(LogValidationOptions.simple());
    }

    /**
     * Creates an empty context with the given options.
     *
     * @param options the operator-selected options to carry through the rule chain
     * @return a context with no results, no domain facts, and the given options
     */
    static ValidationContext withOptions(LogValidationOptions options) {
        return new ValidationContext(Collections.emptyList(), OptionalLong.empty(), OptionalLong.empty(), OptionalLong.empty(), options);
    }

    /**
     * Returns a new context with the given result appended.
     *
     * @param result the result to append
     * @return a new context containing all prior results plus the new one
     */
    ValidationContext append(ValidationResult result) {
        List<ValidationResult> next = new ArrayList<>(results);
        next.add(result);
        return new ValidationContext(Collections.unmodifiableList(next), firstLogIndex, lastLogIndex, highestTerm, options);
    }

    /**
     * Returns a new context with the log entry range recorded.
     *
     * @param first the first entry index in the log
     * @param last  the last entry index in the log
     * @return a new context with the log range set
     */
    ValidationContext withLogRange(long first, long last) {
        return new ValidationContext(results, OptionalLong.of(first), OptionalLong.of(last), highestTerm, options);
    }

    /**
     * Returns a new context with the highest term recorded.
     *
     * @param term the highest term found in log entries
     * @return a new context with the highest term set
     */
    ValidationContext withHighestTerm(long term) {
        return new ValidationContext(results, firstLogIndex, lastLogIndex, OptionalLong.of(term), options);
    }
}
