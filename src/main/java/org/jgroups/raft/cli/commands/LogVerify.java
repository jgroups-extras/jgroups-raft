package org.jgroups.raft.cli.commands;

import org.jgroups.raft.cli.commands.log.LogValidation;
import org.jgroups.raft.cli.commands.log.ValidationResult;

import java.io.File;
import java.io.IOException;

import picocli.CommandLine.Command;

/**
 * Scans a Raft log directory and reports integrity status without modifying any files.
 *
 * @since 2.0
 * @author José Bolina
 */
@Command(name = "verify", description = "Scan the log directory for corruption and report findings.")
final class LogVerify extends BaseLogCommand {

    /**
     * Scans all files in the log directory and reports integrity status.
     *
     * <p>
     * Delegates to {@link LogValidation#validate(File, org.jgroups.raft.cli.commands.log.LogValidationOptions)} which runs
     * the full rule chain (format detection, entry scanning, snapshot and metadata checks) and returns a composite result.
     * The result is printed directly to the operator without extra formatting and its exit code determines the process
     * exit status. Users are allow to adjust the output verbosity with CLI flags -v/-vv.
     * </p>
     *
     * @return the exit code from the validation result
     * @throws IOException if an unrecoverable I/O error prevents validation
     */
    @Override
    protected int execute() throws IOException {
        ValidationResult result = LogValidation.validate(directory(), validationOptions());
        result.formatTo(out());
        return result.exitCode();
    }
}
