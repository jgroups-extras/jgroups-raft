package org.jgroups.raft.cli.commands.log;

import java.io.PrintWriter;
import java.util.List;
import java.util.Optional;

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
    public ParseType fileParsed() {
        ParseType pt = ParseType.SUCCESS;
        for (ValidationResult result : results) {
            if (result.fileParsed() == ParseType.UNRECOGNIZED)
                return ParseType.UNRECOGNIZED;

            if (result.fileParsed() != ParseType.SUCCESS)
                pt = result.fileParsed();
        }
        return pt;
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

    @Override
    public Optional<CorruptionPoint> firstCorruption() {
        for (ValidationResult result : results) {
            if (result.firstCorruption().isPresent())
                return result.firstCorruption();
        }
        return Optional.empty();
    }

    @Override
    public Optional<LogInfo> logInfo() {
        for (ValidationResult result : results) {
            if (result.logInfo().isPresent())
                return result.logInfo();
        }
        return Optional.empty();
    }

    @Override
    public Optional<MetadataInfo> metadataInfo() {
        for (ValidationResult result : results) {
            if (result.metadataInfo().isPresent())
                return result.metadataInfo();
        }
        return Optional.empty();
    }
}
