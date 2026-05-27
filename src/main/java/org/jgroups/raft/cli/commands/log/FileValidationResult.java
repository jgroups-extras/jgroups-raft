package org.jgroups.raft.cli.commands.log;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Default {@link ValidationResult} backed by ordered fields and violations.
 *
 * <p>
 * The label-column width is derived from the longest label so that values align consistently within each file section.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 */
final class FileValidationResult implements ValidationResult {

    private final String filename;
    private final List<Map.Entry<String, String>> fields;
    private final List<Violation> violations;

    private FileValidationResult(String filename, List<Map.Entry<String, String>> fields, List<Violation> violations) {
        this.filename = filename;
        this.fields = fields;
        this.violations = violations;
    }

    /**
     * Creates a builder for a result associated with the given file.
     *
     * @param filename the name of the file being validated (e.g., {@code "entries.raft"})
     * @return a new builder
     */
    static ValidationResultBuilder builder(String filename) {
        return new ValidationResultBuilder(filename);
    }

    @Override
    public boolean isValid() {
        return exitCode() == 0;
    }

    @Override
    public int exitCode() {
        int code = 0;
        for (Violation violation : violations) {
            switch (violation.severity()) {
                case ERROR -> code = 1;
                case INVALID -> {
                    // Invalid takes precedence and we return directly from the method.
                    return 2;
                }
                case WARNING -> {
                    // Empty block by default.
                    // Warning does not change the exit code.
                }
            }
        }
        return code;
    }

    @Override
    public void formatTo(PrintWriter out) {
        out.println(filename);

        // Calculate the width to output everything aligned.
        // These are additional information for Raft to help understand the current file state.
        int labelWidth = fields.stream()
                .mapToInt(f -> f.getKey().length())
                .max().orElse(0);
        String format = "  %-" + (labelWidth + 1) + "s %s%n";

        for (Map.Entry<String, String> field : fields) {
            out.printf(format, field.getKey() + ":", field.getValue());
        }


        // Output all the user-facing messages identified during verification.
        // All the messages should be clear and actionable for operators.
        if (!violations.isEmpty()) {
            out.println();
            out.println("  Problems:");
            for (Violation violation : violations) {
                out.printf("    %s%n", violation.message());
            }
        }

        out.println();
    }

    /**
     * Assembles a {@link ValidationResult} incrementally.
     *
     * <p>
     * Fields are displayed in insertion order; label-column width is computed automatically from the longest label.
     * </p>
     */
    static final class ValidationResultBuilder {
        private final String filename;
        private final List<Map.Entry<String, String>> fields = new ArrayList<>();
        private final List<Violation> violations = new ArrayList<>();

        private ValidationResultBuilder(String filename) {
            this.filename = filename;
        }

        /**
         * Adds a labeled field to the result.
         *
         * @param label the field label (e.g., {@code "Format"})
         * @param value the field value (e.g., {@code "v2 (checksummed)"})
         * @return this builder
         */
        ValidationResultBuilder field(String label, String value) {
            fields.add(Map.entry(label, value));
            return this;
        }

        /**
         * Appends a single violation.
         *
         * @param violation the violation to add
         * @return this builder
         */
        ValidationResultBuilder violation(Violation violation) {
            violations.add(violation);
            return this;
        }

        /**
         * Appends all violations from the given list.
         *
         * @param violations the violations to add
         * @return this builder
         */
        ValidationResultBuilder violations(List<Violation> violations) {
            this.violations.addAll(violations);
            return this;
        }

        /**
         * Builds an immutable validation result.
         *
         * @return the validation result
         */
        ValidationResult build() {
            return new FileValidationResult(filename, List.copyOf(fields), List.copyOf(violations));
        }
    }
}
