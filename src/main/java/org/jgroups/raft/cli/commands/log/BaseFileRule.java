package org.jgroups.raft.cli.commands.log;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

abstract class BaseFileRule implements LogValidatorRule {

    private final String filename;

    protected BaseFileRule(String filename) {
        this.filename = filename;
    }

    @Override
    public final ValidationContext validate(File directory, ValidationContext context) throws IOException {
        Path path = directory.toPath().resolve(filename);
        File file = path.toFile();

        if (!file.isFile()) {
            String message = String.format("File not found at %s. " +
                    "If this is a fresh node, this is expected. Otherwise, compare with other nodes in the cluster.",
                    path.toAbsolutePath());
            ValidationResult vr = FileValidationResult.builder(path.toAbsolutePath().toString())
                    .field("Status", FileValidationResult.ValidationField.warn("MISSING"))
                    .violation(new Violation(message, Violation.Severity.WARNING))
                    .build();
            return context.append(vr);
        }

        if (file.length() == 0) {
            String message = String.format("File is empty at %s", path.toAbsolutePath());
            ValidationResult vr = FileValidationResult.builder(path.toAbsolutePath().toString())
                    .field("Status", FileValidationResult.ValidationField.warn("EMPTY"))
                    .violation(new Violation(message, Violation.Severity.WARNING))
                    .build();
            return context.append(vr);
        }

        return proceedValidation(path, context);
    }

    abstract ValidationContext proceedValidation(Path path, ValidationContext context) throws IOException;
}
