package org.jgroups.raft.cli.commands.log;

import java.io.File;
import java.io.IOException;

/**
 * A single read-only validation check against one aspect of a log directory.
 *
 * <p>
 * Each rule inspects one file (or cross-file relationship), appends a {@link ValidationResult} to the context, and records
 * any domain-level facts that subsequent rules may need. Rules must never modify files.
 * </p>
 *
 * <p>
 * Rules execute in registration order within a {@link LogValidation} engine. A rule may read domain facts from the incoming
 * context that were recorded by earlier rules (e.g., the metadata rule reads the log entry range recorded by the entries
 * rule). This ordering dependency should be documented on each concrete rule.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 */
@FunctionalInterface
interface LogValidatorRule {

    /**
     * Validates one aspect of the log directory and returns an enriched context.
     *
     * <p>
     * The returned context must preserve all results and facts from the input context, plus this rule's own result and
     * any newly discovered facts.
     * </p>
     *
     * @param directory the log directory to validate
     * @param context   the accumulated context from previous rules
     * @return a new context with this rule's contributions appended
     * @throws IOException if the validation cannot complete due to an I/O error
     */
    ValidationContext validate(File directory, ValidationContext context) throws IOException;
}
