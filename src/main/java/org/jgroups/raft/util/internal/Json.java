package org.jgroups.raft.util.internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Minimal JSON serialization and deserialization for internal use only.
 *
 * <p>
 * This utility converts between {@code Map<String, ?>} and compact JSON strings without whitespace or pretty-printing.
 * It is designed for internal data interchange where both serialization and deserialization are controlled by
 * this implementation. For example, this implementation does not parse an array JSON, only complete JSON objects.
 * </p>
 *
 * <p>
 * <b>Supported types:</b>
 * <ul>
 *   <li>Primitives: String, Number (Integer, Long, Double, Float), Boolean, Character</li>
 *   <li>null values</li>
 *   <li>Nested Map instances (must have String keys)</li>
 *   <li>Collections (List, Set, etc.)</li>
 * </ul>
 * </p>
 *
 * <p>
 * <b>This is NOT a full JSON implementation and should NOT be used for:</b>
 * <ul>
 *   <li>Parsing arbitrary external JSON</li>
 *   <li>User-facing or public APIs</li>
 *   <li>Interoperability with other systems</li>
 * </ul>
 * </p>
 *
 * <p>
 * The serializer outputs compact JSON (no whitespace or line breaks) and handles proper escaping of special characters
 * including control characters (e.g., \n, \t) and Unicode sequences. The parser is optimized to read only the compact
 * format produced by this serializer, it does not parse an arbitrary JSON string.
 * </p>
 *
 * @see #toJson(Map)
 * @see #fromJson(String)
 * @since 2.0
 * @author José Bolina
 */
public final class Json {

    public static final String NULL = "null";
    public static final char LCB = '{';
    public static final char RCB = '}';
    public static final char LSB = '[';
    public static final char RSB = ']';
    public static final char COMMA = ',';
    public static final char COLON = ':';
    public static final char QUOTE = '"';

    private Json() { }

    /**
     * Serializes a map to a compact JSON string without whitespace.
     *
     * <p>
     * The output is a valid JSON object with no spaces, newlines, or indentation. All string values and keys are properly
     * escaped according to JSON rules. Characters are serialized as JSON strings.
     * </p>
     *
     * @param input the map to serialize, or {@code null}
     * @return compact JSON string representation, or "null" if input is {@code null}
     * @throws IllegalArgumentException if the map contains unsupported value types
     */
    public static String toJson(Map<String, ?> input) {
        if (input == null) return NULL;

        StringBuilder output = new StringBuilder();
        output.append(LCB);
        boolean first = true;
        for (Map.Entry<String, ?> entry : input.entrySet()) {
            if (!first) output.append(COMMA);
            first = false;

            // Creates JSON key: "<key>":
            output.append(QUOTE)
                    .append(escape(entry.getKey()))
                    .append(QUOTE)
                    .append(COLON);

            writeValue(output, entry.getValue());
        }

        output.append(RCB);
        return output.toString();
    }

    /**
     * Deserializes a JSON string into a {@link Map}.
     *
     * <p>
     * This parser is optimized for the compact JSON format produced by {@link #toJson(Map)} method. It expects no
     * whitespace and will fail on malformed input.
     * </p>
     *
     * @param input the JSON string to parse, or null
     * @return the deserialized map, or null if input is null or "null"
     * @throws IllegalStateException if the JSON is malformed
     */
    public static Map<String, Object> fromJson(String input) {
        if (input == null || input.equals(NULL))
            return null;

        return Parser.parse(input);
    }

    public static Map<String, String> flatten(Map<String, ?> input) {
        if (input == null)
            return null;

        // Use LinkedHashMap to maintain insertion order.
        // It ensures that once we start handling a Collection, all the elements are next to each other in the output.
        // The same applies to nested objects.
        Map<String, String> flat = new LinkedHashMap<>();
        flatten("", input, flat);
        return flat;
    }

    private static void flatten(String prefix, Object value, Map<String, String> output) {
        if (value instanceof Map<?,?> m) {
            for (Map.Entry<?, ?> entry : m.entrySet()) {
                String newKey = prefix.isEmpty()
                        ? String.valueOf(entry.getKey())
                        : prefix + "." + entry.getKey();

                flatten(newKey, entry.getValue(), output);
            }
            return;
        }

        if (value instanceof Iterable<?> iterable) {
            int i = 0;
            for (Object o : iterable) {
                flatten(String.format("%s[%d]", prefix, i), o, output);
                i++;
            }
            return;
        }

        String v = value == null ? NULL : String.valueOf(value);
        output.put(prefix, v);
    }

    /**
     * Writes a single value to the output buffer in JSON format.
     * Recursively handles nested maps and collections.
     *
     * @param output the string builder to append to
     * @param value the value to serialize
     * @throws IllegalArgumentException if the value is of an unsupported type
     */
    private static void writeValue(StringBuilder output, Object value) {
        if (value == null) {
            output.append(NULL);
            return;
        }

        if (value instanceof String s) {
            output.append(QUOTE).append(escape(s)).append(QUOTE);
            return;
        }

        if (value instanceof Character c) {
            // Must transform to String and escape to avoid scenarios like: '\'.
            output.append(QUOTE).append(escape(Character.toString(c))).append(QUOTE);
            return;
        }

        if (value instanceof Number || value instanceof Boolean) {
            output.append(value);
            return;
        }

        if (value instanceof Map<?,?> map) {
            @SuppressWarnings("unchecked")
            Map<String, ?> m = (Map<String, ?>) map;
            output.append(toJson(m));
            return;
        }

        if (value instanceof Collection<?> c) {
            output.append(LSB);
            int i = 0;
            for (Object o : c) {
                if (i > 0) output.append(COMMA);
                writeValue(output, o);
                i++;
            }
            output.append(RSB);
            return;
        }

        throw new IllegalArgumentException("Unknown object of type: " + value.getClass());
    }

    /**
     * Escapes special characters in a string for JSON encoding.
     * <p>
     * Handles standard JSON escape sequences (\", \\, \n, \r, \t, \b, \f)
     * and encodes control characters (&lt; 0x20) as Unicode escape sequences (\\uXXXX).
     * </p>
     *
     * @param s the string to escape
     * @return the escaped string
     */
    private static String escape(String s) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"': sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\b': sb.append("\\b"); break;
                case '\f': sb.append("\\f"); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                case '\t': sb.append("\\t"); break;
                default:
                    if (c < ' ') { // Control characters
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
            }
        }
        return sb.toString();
    }

    /**
     * Internal parser for deserializing compact JSON strings.
     *
     * <p>
     * The parser maintains a position cursor that advances through the input string as tokens are consumed. The parser
     * correctly report the position where failures happen when parsing an input.
     * </p>
     */
    private static final class Parser {
        private final String input;
        private int position;

        private Parser(String input) {
            this.input = input;
            this.position = 0;
        }

        /**
         * Parse a JSON string into a map.
         *
         * @param input the JSON string to parse
         * @return the parsed map
         * @throws IllegalStateException if the input string is malformed
         */
        public static Map<String, Object> parse(String input) {
            Parser p = new Parser(input);
            return p.parse();
        }

        public Map<String, Object> parse() {
            // Start of JSON object: {
            expect(LCB);

            // If the object already closes, it is an empty map.
            if (peek() == RCB) {
                position++;
                return Collections.emptyMap();
            }

            Map<String, Object> result = new HashMap<>();
            while (true) {
                String key = string();
                expect(COLON);
                Object value = value();
                result.put(key, value);

                char next = peek();
                if (next == RCB) {
                    position++;
                    break;
                }

                expect(COMMA);
            }

            return result;
        }

        /**
         * Parses any JSON value based on the next character.
         * Delegates to specialized parsers for each type.
         *
         * @return the parsed value (String, Number, Boolean, Map, Collection, or {@code null})
         * @throws IllegalStateException if the value cannot be parsed
         */
        private Object value() {
            char c = peek();
            return switch (c) {
                case QUOTE -> string();
                case LCB -> parse();
                case LSB -> array();
                case 't', 'f' -> bool();
                case 'n' -> nil();
                default -> number();
            };
        }

        private Collection<Object> array() {
            expect(LSB);

            if (peek() == RSB) {
                position++;
                return Collections.emptyList();
            }

            List<Object> array = new ArrayList<>();

            while (true) {
                array.add(value());

                char next = peek();
                if (next == RSB) {
                    position++;
                    break;
                }

                expect(COMMA);
            }
            return array;
        }

        private boolean bool() {
            if (input.startsWith("true", position)) {
                position += 4;
                return Boolean.TRUE;
            }
            if (input.startsWith("false", position)) {
                position += 5;
                return Boolean.FALSE;
            }
            throw new IllegalStateException("Invalid boolean at position " + position);
        }

        /**
         * Parses a JSON number.
         *
         * <p>
         * Returns Integer for integers that fit, Long for larger integers, and Double for floating-point numbers.
         * Observe it might not keep the exact object type utilized to serialize into a String.
         * </p>
         *
         * @return the parsed number
         * @throws NumberFormatException if the number format is invalid
         */
        private Number number() {
            int start = position;
            while (position < input.length() && isNumberChar(input.charAt(position))) {
                position++;
            }

            String numberString = input.substring(start, position);
            if (numberString.contains(".") || numberString.contains("e") || numberString.contains("E")) {
                return Double.parseDouble(numberString);
            }

            try {
                return Integer.parseInt(numberString);
            } catch (NumberFormatException ignore) {
                return Long.parseLong(numberString);
            }
        }

        public Void nil() {
            if (input.startsWith(NULL, position)) {
                position += 4;
                return null;
            }

            throw new IllegalStateException("Unknown JSON element at position: " + position);
        }

        private String string() {
            expect(QUOTE);
            StringBuilder result = new StringBuilder();

            while (position < input.length()) {
                char c = input.charAt(position++);
                if (c == QUOTE)
                    return result.toString();

                if (c == '\\') {
                    char escaped = input.charAt(position++);
                    switch (escaped) {
                        case 'n': result.append('\n'); break;
                        case 'r': result.append('\r'); break;
                        case 't': result.append('\t'); break;
                        case 'b': result.append('\b'); break;
                        case 'f': result.append('\f'); break;
                        case '\\': result.append('\\'); break;
                        case '"': result.append('"'); break;
                        case 'u':
                            String hex = input.substring(position, position + 4);
                            position += 4;
                            result.append((char) Integer.parseInt(hex, 16));
                            break;
                        default:
                            result.append(escaped);
                    }
                } else {
                    result.append(c);
                }
            }

            throw new IllegalStateException("Unterminated string");
        }

        /**
         * Peeks at the current character without consuming it.
         *
         * @return the current character
         * @throws IllegalStateException if the end of input is reached
         */
        private char peek() {
            if (position >= input.length())
                throw new IllegalStateException("Unexpected end of input");

            return input.charAt(position);
        }

        /**
         * Expects a specific character at the current position and advances.
         * Throws if the character doesn't match.
         *
         * @param expected the expected character
         * @throws IllegalStateException if the expected character is not found
         */
        private void expect(char expected) {
            char actual = peek();
            if (actual != expected)
                throw new IllegalStateException("Expected '" + expected + "' but found '" + actual + "' at position " + position);

            position++;
        }

        /**
         * Checks if a character is valid within a JSON number.
         *
         * @param c the character to check
         * @return {@code true} if the character is valid in a number, {@code false} otherwise
         */
        private boolean isNumberChar(char c) {
            return c == '-' || c == '+' || c == '.' || c == 'e' || c == 'E' || (c >= '0' && c <= '9');
        }
    }
}
