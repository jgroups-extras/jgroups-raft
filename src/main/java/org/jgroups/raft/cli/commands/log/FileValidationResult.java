package org.jgroups.raft.cli.commands.log;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import picocli.CommandLine;

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
    private final List<Map.Entry<String, ValidationField>> fields;
    private final List<Violation> violations;
    private final CorruptionPoint corruptionPoint;
    private final LogInfo logInfo;
    private final MetadataInfo metadataInfo;

    private FileValidationResult(
            String filename,
            List<Map.Entry<String, ValidationField>> fields,
            List<Violation> violations,
            CorruptionPoint corruptionPoint,
            LogInfo logInfo,
            MetadataInfo metadataInfo
    ) {
        this.filename = filename;
        this.fields = fields;
        this.violations = violations;
        this.corruptionPoint = corruptionPoint;
        this.logInfo = logInfo;
        this.metadataInfo = metadataInfo;
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
        CommandLine.Help.Ansi ansi = CommandLine.Help.Ansi.AUTO;

        out.println(ansi.string("@|bold " + filename + "|@"));

        // Calculate the width to output everything aligned.
        // These are additional information for Raft to help understand the current file state.
        int labelWidth = fields.stream()
                .mapToInt(f -> f.getKey().length())
                .max().orElse(0);
        String format = "  %-" + (labelWidth + 1) + "s %s%n";

        for (Map.Entry<String, ValidationField> field : fields) {
            out.printf(format, field.getKey() + ":", colorizeValue(ansi, field.getValue()));
        }


        // Output all the user-facing messages identified during verification.
        // All the messages should be clear and actionable for operators.
        if (!violations.isEmpty()) {
            out.println();
            out.println(ansi.string("  @|bold Problems:|@"));
            for (Violation violation : violations) {
                String prefix = switch (violation.severity()) {
                    case WARNING -> "@|yellow ";
                    case ERROR -> "@|red,bold ";
                    case INVALID -> "@|bold ";
                };
                out.printf("    %s%n", ansi.string(prefix + violation.message() + "|@"));
            }
        }

        out.println();
    }

    @Override
    public Optional<CorruptionPoint> firstCorruption() {
        return Optional.ofNullable(corruptionPoint);
    }

    @Override
    public Optional<LogInfo> logInfo() {
        return Optional.ofNullable(logInfo);
    }

    @Override
    public Optional<MetadataInfo> metadataInfo() {
        return Optional.ofNullable(metadataInfo);
    }

    private String colorizeValue(CommandLine.Help.Ansi ansi, ValidationField value) {
        if (value instanceof ValidationField.Info)
            return ansi.string("@|green " + value.text() + "|@");

        if (value instanceof ValidationField.Error)
            return ansi.string("@|bold,red " + value.text() + "|@");

        if (value instanceof ValidationField.Warn)
            return ansi.string("@|yellow " + value.text() + "|@");

        return value.text();
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
        private final List<Map.Entry<String, ValidationField>> fields = new ArrayList<>();
        private final List<Violation> violations = new ArrayList<>();
        private CorruptionPoint corruptionPoint = null;
        private LogInfo logInfo = null;
        private MetadataInfo metadataInfo = null;

        private ValidationResultBuilder(String filename) {
            this.filename = filename;
        }

        /**
         * Adds a labeled field to the result.
         *
         * @param label the field label (e.g., {@code "Format"})
         * @param value the field value wrapped by a custom informational level
         * @return this builder
         */
        ValidationResultBuilder field(String label, ValidationField value) {
            fields.add(Map.entry(label, value));
            return this;
        }

        /**
         * Adds a labeled field to the result.
         *
         * @param label the field label (e.g., {@code "Format"})
         * @param value the field value (e.g., {@code "v2 (checksummed)"})
         * @return this builder
         */
        ValidationResultBuilder field(String label, String value) {
            fields.add(Map.entry(label, ValidationField.plain(value)));
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
         * Sets the first corruption point found during the entries scan.
         *
         * @param corruption the corruption point
         * @return this builder
         */
        ValidationResultBuilder firstCorruption(ValidationResult.CorruptionPoint corruption) {
            this.corruptionPoint = corruption;
            return this;
        }

        /**
         * Sets the log entry range information from the entries scan.
         *
         * @param logInfo the log info
         * @return this builder
         */
        ValidationResultBuilder logInfo(ValidationResult.LogInfo logInfo) {
            this.logInfo = logInfo;
            return this;
        }

        /**
         * Sets the metadata values read from the metadata file.
         *
         * @param metadataInfo the metadata info
         * @return this builder
         */
        ValidationResultBuilder metadataInfo(ValidationResult.MetadataInfo metadataInfo) {
            this.metadataInfo = metadataInfo;
            return this;
        }


        /**
         * Builds an immutable validation result.
         *
         * @return the validation result
         */
        ValidationResult build() {
            return new FileValidationResult(filename, List.copyOf(fields), List.copyOf(violations), corruptionPoint, logInfo, metadataInfo);
        }
    }

    /**
     * A display value for a labeled field in a validation result.
     *
     * @since 2.0
     * @author José Bolina
     */
    sealed interface ValidationField {

        /**
         * Returns the display text for this field value.
         *
         * @return the text to render after the field label
         */
        String text();

        /**
         * An informational text.
         *
         * @param text the display text
         * @return a validation field with the proper informational level.
         */
        static ValidationField plain(String text) {
            return new Plain(text);
        }

        /**
         * A healthy or informational value with no severity implication.
         *
         * @param text the display text
         * @return a validation field with the proper informational level.
         */
        static ValidationField info(String text) {
            return new Info(text);
        }

        /**
         * A value indicating a non-fatal condition the operator should be aware of.
         *
         * @param text the display text
         * @return a validation field with the proper informational level.
         */
        static ValidationField warn(String text) {
            return new Warn(text);
        }

        /**
         * A value indicating a problem that requires attention.
         *
         * @param text the display text
         * @return a validation field with the proper informational level.
         */
        static ValidationField error(String text) {
            return new Error(text);
        }

        record Plain(String text) implements ValidationField { }

        record Info(String text) implements ValidationField { }

        record Warn(String text) implements ValidationField { }

        record Error(String text) implements ValidationField { }
    }
}
