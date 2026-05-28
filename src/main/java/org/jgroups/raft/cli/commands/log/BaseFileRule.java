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
                    "The node may never have been started, the file was deleted, or not written yet", path.toAbsolutePath());
            ValidationResult vr = FileValidationResult.builder(path.toAbsolutePath().toString())
                    .field("Status", "MISSING")
                    .violation(new Violation(message, Violation.Severity.WARNING))
                    .build();
            return context.append(vr);
        }

        if (file.length() == 0) {
            String message = String.format("File is empty at %s", path.toAbsolutePath());
            ValidationResult vr = FileValidationResult.builder(path.toAbsolutePath().toString())
                    .field("Status", "EMPTY")
                    .violation(new Violation(message, Violation.Severity.WARNING))
                    .build();
            return context.append(vr);
        }

        return proceedValidation(path, context);
    }

    abstract ValidationContext proceedValidation(Path path, ValidationContext context) throws IOException;
}
