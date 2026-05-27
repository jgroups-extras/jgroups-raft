package org.jgroups.raft.cli.commands.log;

import java.io.PrintWriter;
import java.util.List;

/**
 * Aggregates multiple {@link ValidationResult}s into a single result.
 *
 * <p>
 * {@link #isValid()} returns {@code true} only when every constituent result is valid. {@link #formatTo(PrintWriter)} delegates
 * to each result in order, producing the complete validation summary.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 */
final class CompositeValidationResult implements ValidationResult {

    private final List<ValidationResult> results;

    CompositeValidationResult(List<ValidationResult> results) {
        this.results = results;
    }

    @Override
    public boolean isValid() {
        return results.stream().allMatch(ValidationResult::isValid);
    }

    @Override
    public int exitCode() {
        int code = 0;
        for (ValidationResult result : results) {
            int curr = result.exitCode();
            if (curr == 2)
                return 2;

            code = curr;
        }
        return code;
    }

    @Override
    public void formatTo(PrintWriter out) {
        for (ValidationResult result : results) {
            result.formatTo(out);
        }
    }
}
