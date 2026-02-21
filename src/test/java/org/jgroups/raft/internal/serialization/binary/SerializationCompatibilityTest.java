package org.jgroups.raft.internal.serialization.binary;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import org.jgroups.Global;
import org.jgroups.raft.command.JGroupsRaftReadCommandOptions;
import org.jgroups.raft.command.JGroupsRaftWriteCommandOptions;
import org.jgroups.raft.internal.command.JRaftCommand;
import org.jgroups.raft.internal.command.JRaftReadCommand;
import org.jgroups.raft.internal.command.JRaftWriteCommand;
import org.jgroups.raft.internal.command.RaftCommand;
import org.jgroups.raft.internal.command.RaftResponse;
import org.jgroups.raft.internal.serialization.Serializer;
import org.jgroups.raft.internal.serialization.SingleBinarySerializer;
import org.jgroups.raft.internal.serialization.binary.serializers.InternalSerializers;
import org.jgroups.raft.internal.statemachine.StateMachineStateHolder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Compatibility tests for internal serializers.
 *
 * <p>
 * This test class ensures that the binary serialization format for built-in types remains
 * stable across versions. Each test case serializes a known object and compares the output
 * against a pre-recorded expected byte array. If the format changes, these tests will fail,
 * alerting developers to potential compatibility issues.
 * </p>
 *
 * <p>
 * <b>If a test fails:</b>
 * </p>
 * <ul>
 *   <li>If the change was intentional (e.g., bug fix or optimization), increment the serializer's
 *       {@link SingleBinarySerializer#version()} and update the expected bytes.</li>
 *   <li>If the change was accidental, revert it to maintain compatibility.</li>
 * </ul>
 *
 * <p>
 * To update expected bytes after an intentional format change:
 * </p>
 * <ol>
 *   <li>Run the test and copy the "actual" byte array from the failure message</li>
 *   <li>Replace the expected bytes in {@link #compatibilityTestCases()}</li>
 *   <li>Document the change in the commit message</li>
 * </ol>
 *
 * @author José Bolina
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class SerializationCompatibilityTest {

    private Serializer serializer;

    // ========== Collection Factory Methods ==========
    private static ArrayList<String> createArrayList(String... elements) {
        return new ArrayList<>(Arrays.asList(elements));
    }

    private static LinkedList<String> createLinkedList(String... elements) {
        return new LinkedList<>(Arrays.asList(elements));
    }

    private static HashSet<String> createHashSet(String... elements) {
        return new HashSet<>(Arrays.asList(elements));
    }

    private static LinkedHashSet<String> createLinkedHashSet(String... elements) {
        return new LinkedHashSet<>(Arrays.asList(elements));
    }

    // ========== Helper Methods ==========

    private static TreeSet<String> createTreeSet(String... elements) {
        return new TreeSet<>(Arrays.asList(elements));
    }

    private static HashMap<String, String> createHashMap(String key, String value) {
        HashMap<String, String> map = new HashMap<>();
        map.put(key, value);
        return map;
    }

    private static LinkedHashMap<String, String> createLinkedHashMap(String key, String value) {
        LinkedHashMap<String, String> map = new LinkedHashMap<>();
        map.put(key, value);
        return map;
    }

    private static TreeMap<String, Integer> createTreeMap(String key, Integer value) {
        TreeMap<String, Integer> map = new TreeMap<>();
        map.put(key, value);
        return map;
    }

    @BeforeMethod
    public void setUp() {
        SerializationRegistry registry = SerializationRegistry.create();
        serializer = Serializer.create(registry);
    }

    /**
     * Provides test cases for all internal serializers.
     *
     * <p>
     * Each test case contains a value to serialize and its expected byte representation.
     * The expected bytes include the full wire format: [type:4][version:1][length:4][data:N]
     * </p>
     *
     * @return Array of test cases: [Class, value, expectedBytes, description]
     */
    @DataProvider(name = "compatibilityTestCases")
    public Object[][] compatibilityTestCases() {
        return new Object[][]{
                // ========== Primitive Types (Fast Path) ==========
                {Byte.class, (byte) 42,
                        hexToBytes("00 00 00 00 2A"),
                        "Byte: 42"},

                {Byte.class, Byte.MIN_VALUE,
                        hexToBytes("00 00 00 00 80"),
                        "Byte: MIN_VALUE"},

                {Byte.class, Byte.MAX_VALUE,
                        hexToBytes("00 00 00 00 7F"),
                        "Byte: MAX_VALUE"},

                {Short.class, (short) 1000,
                        hexToBytes("00 00 00 01 03 E8"),
                        "Short: 1000"},

                {Short.class, Short.MIN_VALUE,
                        hexToBytes("00 00 00 01 80 00"),
                        "Short: MIN_VALUE"},

                {Short.class, Short.MAX_VALUE,
                        hexToBytes("00 00 00 01 7F FF"),
                        "Short: MAX_VALUE"},

                {Integer.class, 123456,
                        hexToBytes("00 00 00 02 00 01 E2 40"),
                        "Integer: 123456"},

                {Integer.class, Integer.MIN_VALUE,
                        hexToBytes("00 00 00 02 80 00 00 00"),
                        "Integer: MIN_VALUE"},

                {Integer.class, Integer.MAX_VALUE,
                        hexToBytes("00 00 00 02 7F FF FF FF"),
                        "Integer: MAX_VALUE"},

                {Long.class, 123456789L,
                        hexToBytes("00 00 00 03 00 00 00 00 07 5B CD 15"),
                        "Long: 123456789"},

                {Long.class, Long.MIN_VALUE,
                        hexToBytes("00 00 00 03 80 00 00 00 00 00 00 00"),
                        "Long: MIN_VALUE"},

                {Long.class, Long.MAX_VALUE,
                        hexToBytes("00 00 00 03 7F FF FF FF FF FF FF FF"),
                        "Long: MAX_VALUE"},

                {Float.class, 3.14159f,
                        hexToBytes("00 00 00 04 40 49 0F D0"),
                        "Float: 3.14159"},

                {Float.class, 0.0f,
                        hexToBytes("00 00 00 04 00 00 00 00"),
                        "Float: 0.0"},

                {Double.class, 3.141592653589793,
                        hexToBytes("00 00 00 05 40 09 21 FB 54 44 2D 18"),
                        "Double: PI"},

                {Double.class, 0.0,
                        hexToBytes("00 00 00 05 00 00 00 00 00 00 00 00"),
                        "Double: 0.0"},

                {Boolean.class, true,
                        hexToBytes("00 00 00 06 01"),
                        "Boolean: true"},

                {Boolean.class, false,
                        hexToBytes("00 00 00 06 00"),
                        "Boolean: false"},

                {Character.class, 'A',
                        hexToBytes("00 00 00 07 00 41"),
                        "Character: 'A'"},

                {Character.class, 'z',
                        hexToBytes("00 00 00 07 00 7A"),
                        "Character: 'z'"},

                {Character.class, '中',
                        hexToBytes("00 00 00 07 4E 2D"),
                        "Character: Unicode '中'"},

                // ========== String (Fast Path) ==========
                {String.class, "Hello",
                        hexToBytes("00 00 00 08 00 05 48 65 6C 6C 6F"),
                        "String: 'Hello'"},

                {String.class, "",
                        hexToBytes("00 00 00 08 00 00"),
                        "String: empty"},

                {String.class, "日本語",
                        hexToBytes("00 00 00 08 00 09 E6 97 A5 E6 9C AC E8 AA 9E"),
                        "String: Unicode Japanese"},

                // ========== byte[] (Fast Path) ==========
                {byte[].class, new byte[]{1, 2, 3, 4, 5},
                        hexToBytes("00 00 00 09 00 00 00 05 01 02 03 04 05"),
                        "byte[]: {1,2,3,4,5}"},

                {byte[].class, new byte[]{},
                        hexToBytes("00 00 00 09 00 00 00 00"),
                        "byte[]: empty"},

                // ========== Primitive Arrays (Complex Types) ==========
                {short[].class, new short[]{1, 2, 3},
                        hexToBytes("00 00 00 20 00 00 00 00 0A 00 00 00 03 00 01 00 02 00 03"),
                        "short[]: {1,2,3}"},

                {int[].class, new int[]{1, 2, 3},
                        hexToBytes("00 00 00 21 00 00 00 00 10 00 00 00 03 00 00 00 01 00 00 00 02 00 00 00 03"),
                        "int[]: {1,2,3}"},

                {int[].class, new int[]{},
                        hexToBytes("00 00 00 21 00 00 00 00 04 00 00 00 00"),
                        "int[]: empty"},

                {long[].class, new long[]{100L, 200L},
                        hexToBytes("00 00 00 22 00 00 00 00 14 00 00 00 02 00 00 00 00 00 00 00 64 00 00 00 00 00 00 00 C8"),
                        "long[]: {100,200}"},

                {float[].class, new float[]{1.0f, 2.0f},
                        hexToBytes("00 00 00 23 00 00 00 00 0C 00 00 00 02 3F 80 00 00 40 00 00 00"),
                        "float[]: {1.0,2.0}"},

                {double[].class, new double[]{1.0, 2.0},
                        hexToBytes("00 00 00 24 00 00 00 00 14 00 00 00 02 3F F0 00 00 00 00 00 00 40 00 00 00 00 00 00 00"),
                        "double[]: {1.0,2.0}"},

                {boolean[].class, new boolean[]{true, false, true},
                        hexToBytes("00 00 00 25 00 00 00 00 07 00 00 00 03 01 00 01"),
                        "boolean[]: {true,false,true}"},

                {char[].class, new char[]{'a', 'b', 'c'},
                        hexToBytes("00 00 00 26 00 00 00 00 0A 00 00 00 03 00 61 00 62 00 63"),
                        "char[]: {'a','b','c'}"},

                // ========== Mutable Collections ==========
                {ArrayList.class, createArrayList("one", "two"),
                        hexToBytes("00 00 00 27 00 00 00 00 16 00 00 00 02 00 00 00 08 00 03 6F 6E 65 00 00 00 08 00 03 74 77 6F"),
                        "ArrayList: [\"one\", \"two\"]"},

                {LinkedList.class, createLinkedList("a", "b"),
                        hexToBytes("00 00 00 28 00 00 00 00 12 00 00 00 02 00 00 00 08 00 01 61 00 00 00 08 00 01 62"),
                        "LinkedList: [\"a\", \"b\"]"},

                {HashSet.class, createHashSet("single"),
                        hexToBytes("00 00 00 29 00 00 00 00 10 00 00 00 01 00 00 00 08 00 06 73 69 6E 67 6C 65"),
                        "HashSet: [\"single\"]"},

                {LinkedHashSet.class, createLinkedHashSet("first", "second"),
                        hexToBytes("00 00 00 2A 00 00 00 00 1B 00 00 00 02 00 00 00 08 00 05 66 69 72 73 74 00 00 00 08 00 06 73 65 63 6F 6E 64"),
                        "LinkedHashSet: [\"first\", \"second\"]"},

                {TreeSet.class, createTreeSet("alpha", "beta"),
                        hexToBytes("00 00 00 2B 00 00 00 00 19 00 00 00 02 00 00 00 08 00 05 61 6C 70 68 61 00 00 00 08 00 04 62 65 74 61"),
                        "TreeSet: [\"alpha\", \"beta\"]"},

                // ========== Maps ==========
                {HashMap.class, createHashMap("k1", "v1"),
                        hexToBytes("00 00 00 36 00 00 00 00 14 00 00 00 01 00 00 00 08 00 02 6B 31 00 00 00 08 00 02 76 31"),
                        "HashMap: {\"k1\": \"v1\"}"},

                {LinkedHashMap.class, createLinkedHashMap("key1", "val1"),
                        hexToBytes("00 00 00 37 00 00 00 00 18 00 00 00 01 00 00 00 08 00 04 6B 65 79 31 00 00 00 08 00 04 76 61 6C 31"),
                        "LinkedHashMap: {\"key1\": \"val1\"}"},

                {TreeMap.class, createTreeMap("a", 1),
                        hexToBytes("00 00 00 38 00 00 00 00 13 00 00 00 01 00 00 00 08 00 01 61 00 00 00 02 00 00 00 01"),
                        "TreeMap: {\"a\": 1}"},

                // ========== Immutable Collections ==========
                {Collections.emptyList().getClass(), Collections.emptyList(),
                        hexToBytes("00 00 00 2C 00 00 00 00 00"),
                        "Collections.emptyList()"},

                {Collections.emptySet().getClass(), Collections.emptySet(),
                        hexToBytes("00 00 00 2D 00 00 00 00 00"),
                        "Collections.emptySet()"},

                {Collections.emptyMap().getClass(), Collections.emptyMap(),
                        hexToBytes("00 00 00 39 00 00 00 00 00"),
                        "Collections.emptyMap()"},

                {Collections.singletonList("x").getClass(), Collections.singletonList("test"),
                        hexToBytes("00 00 00 2E 00 00 00 00 0A 00 00 00 08 00 04 74 65 73 74"),
                        "Collections.singletonList(\"test\")"},

                {Collections.singleton("x").getClass(), Collections.singleton("item"),
                        hexToBytes("00 00 00 2F 00 00 00 00 0A 00 00 00 08 00 04 69 74 65 6D"),
                        "Collections.singleton(\"item\")"},

                {Collections.singletonMap("k", "v").getClass(), Collections.singletonMap("key", "value"),
                        hexToBytes("00 00 00 3A 00 00 00 00 14 00 00 00 08 00 03 6B 65 79 00 00 00 08 00 05 76 61 6C 75 65"),
                        "Collections.singletonMap(\"key\", \"value\")"},

                {Collections.unmodifiableList(List.of()).getClass(),
                        Collections.unmodifiableList(List.of("a", "b")),
                        hexToBytes("00 00 00 30 00 00 00 00 12 00 00 00 02 00 00 00 08 00 01 61 00 00 00 08 00 01 62"),
                        "Collections.unmodifiableList([\"a\", \"b\"])"},

                {Collections.unmodifiableSet(Set.of()).getClass(),
                        Collections.unmodifiableSet(Set.of("item")),
                        hexToBytes("00 00 00 31 00 00 00 00 0E 00 00 00 01 00 00 00 08 00 04 69 74 65 6D"),
                        "Collections.unmodifiableSet([\"item\"])"},

                {Collections.unmodifiableMap(Map.of()).getClass(),
                        Collections.unmodifiableMap(Map.of("k", "v")),
                        hexToBytes("00 00 00 3B 00 00 00 00 12 00 00 00 01 00 00 00 08 00 01 6B 00 00 00 08 00 01 76"),
                        "Collections.unmodifiableMap({\"k\": \"v\"})"},

                {List.of().getClass(), List.of(),
                        hexToBytes("00 00 00 32 00 00 00 00 04 00 00 00 00"),
                        "List.of() - empty"},

                {List.of("x").getClass(), List.of("single"),
                        hexToBytes("00 00 00 33 00 00 00 00 10 00 00 00 01 00 00 00 08 00 06 73 69 6E 67 6C 65"),
                        "List.of(\"single\") - 1 element"},

                {Set.of().getClass(), Set.of(),
                        hexToBytes("00 00 00 34 00 00 00 00 04 00 00 00 00"),
                        "Set.of() - empty"},

                {Set.of("x").getClass(), Set.of("single"),
                        hexToBytes("00 00 00 35 00 00 00 00 10 00 00 00 01 00 00 00 08 00 06 73 69 6E 67 6C 65"),
                        "Set.of(\"single\") - 1 element"},

                {Map.of().getClass(), Map.of(),
                        hexToBytes("00 00 00 3D 00 00 00 00 04 00 00 00 00"),
                        "Map.of() - empty"},

                {Map.of("k", "v").getClass(), Map.of("key", "value"),
                        hexToBytes("00 00 00 3C 00 00 00 00 14 00 00 00 08 00 03 6B 65 79 00 00 00 08 00 05 76 61 6C 75 65"),
                        "Map.of(\"key\", \"value\") - 1 entry"},

                // ========== Raft Types ==========
                {JRaftCommand.UserCommand.class, JRaftReadCommand.create(123L, 1),
                        hexToBytes("00 00 00 43 00 00 00 00 0D 00 00 00 00 00 00 00 7B 00 00 00 01 01"),
                        "UserCommand: read command (id=123, version=1)"},

                {JRaftCommand.UserCommand.class, JRaftWriteCommand.create(456L, 2),
                        hexToBytes("00 00 00 43 00 00 00 00 0D 00 00 00 00 00 00 01 C8 00 00 00 02 00"),
                        "UserCommand: write command (id=456, version=2)"},

                {RaftResponse.class, RaftResponse.success("result"),
                        // Success response with String "result"
                        // Format: [boolean:success=true][object:"result"]
                        hexToBytes("00 00 00 42 00 00 00 00 0D 01 00 00 00 08 00 06 72 65 73 75 6C 74"),
                        "RaftResponse: success with 'result'"},

                {RaftResponse.class, RaftResponse.success(null),
                        // Success response with null
                        hexToBytes("00 00 00 42 00 00 00 00 05 01 FF FF FF FF"),
                        "RaftResponse: success with null"},

                {RaftResponse.class, RaftResponse.failure(new Exception("error message")),
                        // Failure response with error message
                        hexToBytes("00 00 00 42 00 00 00 00 10 00 00 0D 65 72 72 6F 72 20 6D 65 73 73 61 67 65"),
                        "RaftResponse: failure with error"},

                {JGroupsRaftReadCommandOptions.ReadImpl.class,
                        JGroupsRaftReadCommandOptions.options().linearizable(true).build(),
                        hexToBytes("00 00 00 44 00 00 00 00 02 01 00"),
                        "ReadCommandOptions: linearizable=true, ignoreReturnValue=false"},

                {JGroupsRaftReadCommandOptions.ReadImpl.class,
                        JGroupsRaftReadCommandOptions.options().ignoreReturnValue(true).build(),
                        hexToBytes("00 00 00 44 00 00 00 00 02 01 01"),
                        "ReadCommandOptions: linearizable=false, ignoreReturnValue=true"},

                {JGroupsRaftWriteCommandOptions.WriteImpl.class,
                        JGroupsRaftWriteCommandOptions.options().build(),
                        hexToBytes("00 00 00 45 00 00 00 00 01 00"),
                        "WriteCommandOptions: ignoreReturnValue=false"},

                {JGroupsRaftWriteCommandOptions.WriteImpl.class,
                        JGroupsRaftWriteCommandOptions.options().ignoreReturnValue(true).build(),
                        hexToBytes("00 00 00 45 00 00 00 00 01 01"),
                        "WriteCommandOptions: ignoreReturnValue=true"},

                {RaftCommand.class,
                        new RaftCommand(
                                JRaftReadCommand.create(1L, 1),
                                new Object[]{"arg1", 42},
                                JGroupsRaftReadCommandOptions.options().linearizable(true).build()
                        ),
                        // RaftCommand with UserCommand, two arguments, and options
                        hexToBytes("00 00 00 41 00 00 00 00 37 00 00 00 43 00 00 00 00 0D 00 00 00 00 00 00 00 01 00 00 00 01 01 00 00 00 02 00 00 00 08 00 04 61 72 67 31 00 00 00 02 00 00 00 2A 00 00 00 44 00 00 00 00 02 01 00"),
                        "RaftCommand: with arguments and options"},

                {StateMachineStateHolder.class,
                        new StateMachineStateHolder(Map.of(1, "value1")),
                        hexToBytes("00 00 00 46 00 00 00 00 14 00 00 00 01 00 00 00 01 00 00 00 08 00 06 76 61 6C 75 65 31"),
                        "StateMachineStateHolder: with state map"},

                // ========== Null ==========
                {Object.class, null,
                        hexToBytes("FF FF FF FF"),
                        "null"},
        };
    }

    /**
     * Tests serialization compatibility for each internal serializer.
     *
     * <p>
     * This test ensures that:
     * </p>
     * <ul>
     *   <li>Serialization produces the exact expected byte sequence</li>
     *   <li>Deserialization of expected bytes produces the original value</li>
     * </ul>
     *
     * @param type          The Java class being tested
     * @param value         The value to serialize
     * @param expectedBytes The expected wire format bytes (null to print actual bytes)
     * @param description   Human-readable description of the test case
     */
    @Test(dataProvider = "compatibilityTestCases")
    public void testSerializationCompatibility(Class<?> type, Object value, byte[] expectedBytes, String description) {
        // If expectedBytes is null, this is a new test case - print the bytes
        if (expectedBytes == null) {
            byte[] actualBytes = serializer.serialize(value);
            String message = String.format("%s:%n\thexToBytes(\"%s\"),", description, bytesToHex(actualBytes));

            // Still verify round-trip works
            Object deserialized = serializer.deserialize(actualBytes, type);
            assertDeserializedEquals(value, deserialized, description);
            fail(message);
        }

        // Test serialization produces expected bytes
        byte[] actualBytes = serializer.serialize(value);
        assertThat(actualBytes)
                .withFailMessage("""
                                Serialization format has changed for %s!
                                Expected: %s
                                Actual:   %s

                                If this change was intentional:
                                1. Increment the serializer's version() method
                                2. Update the expected bytes in compatibilityTestCases()
                                3. Document the change in your commit message""",
                        description,
                        bytesToHex(expectedBytes),
                        bytesToHex(actualBytes))
                .isEqualTo(expectedBytes);

        // Test deserialization of expected bytes produces expected value
        Object deserialized = serializer.deserialize(expectedBytes, type);
        assertDeserializedEquals(value, deserialized, description);
    }

    /**
     * Verifies that all internal serializers have compatibility test cases.
     *
     * <p>
     * This test prevents developers from adding new internal serializers without
     * also adding compatibility tests for them.
     * </p>
     */
    @Test
    public void testAllSerializersHaveCompatibilityTests() {
        // Collect all tested types from the data provider
        Set<Class<?>> testedTypes = new HashSet<>();
        for (Object[] testCase : compatibilityTestCases()) {
            Class<?> type = (Class<?>) testCase[0];
            if (type != Object.class) { // Exclude the null test case
                testedTypes.add(type);
            }
        }

        // Collect all internal serializer types from both sources
        Set<Class<?>> serializerTypes = new HashSet<>();

        // Add from InternalSerializers.serializers()
        for (SingleBinarySerializer<?> serializer : InternalSerializers.serializers()) {
            serializerTypes.add(serializer.javaClass());
        }

        // Add from BinarySerializer.INTERNAL_SERIALIZERS
        for (SingleBinarySerializer<?> serializer : BinarySerializer.INTERNAL_SERIALIZERS) {
            serializerTypes.add(serializer.javaClass());
        }

        // Find serializers without test cases
        Set<Class<?>> missingTests = new HashSet<>(serializerTypes);
        missingTests.removeAll(testedTypes);

        assertThat(missingTests)
                .withFailMessage("""
                                The following serializers are missing compatibility tests:
                                %s

                                Please add test cases in compatibilityTestCases() for these types.""",
                        missingTests)
                .isEmpty();
    }

    /**
     * Asserts that the deserialized value equals the expected value.
     * Handles special cases like arrays and floating point comparisons.
     */
    private void assertDeserializedEquals(Object expected, Object actual, String description) {
        if (expected == null) {
            assertThat(actual).as(description).isNull();
            return;
        }

        Class<?> type = expected.getClass();

        if (type.isArray()) {
            // Handle different array types
            if (type == byte[].class) {
                assertThat((byte[]) actual).as(description).isEqualTo(expected);
            } else if (type == short[].class) {
                assertThat((short[]) actual).as(description).isEqualTo(expected);
            } else if (type == int[].class) {
                assertThat((int[]) actual).as(description).isEqualTo(expected);
            } else if (type == long[].class) {
                assertThat((long[]) actual).as(description).isEqualTo(expected);
            } else if (type == float[].class) {
                assertThat((float[]) actual).as(description).isEqualTo(expected);
            } else if (type == double[].class) {
                assertThat((double[]) actual).as(description).isEqualTo(expected);
            } else if (type == boolean[].class) {
                assertThat((boolean[]) actual).as(description).isEqualTo(expected);
            } else if (type == char[].class) {
                assertThat((char[]) actual).as(description).isEqualTo(expected);
            } else {
                throw new AssertionError("Unsupported array type: " + type);
            }
        } else if (expected instanceof RaftResponse exp) {
            // Special handling for RaftResponse
            RaftResponse act = (RaftResponse) actual;
            assertThat(act.isSuccess()).as(description + " - isSuccess").isEqualTo(exp.isSuccess());
            if (exp.isSuccess()) {
                assertThat(act.response()).as(description + " - response").isEqualTo(exp.response());
            } else {
                assertThat(act.exception().getMessage()).as(description + " - exception message")
                        .contains(exp.exception().getMessage());
            }
        } else {
            assertThat(actual).as(description).isEqualTo(expected);
        }
    }

    /**
     * Converts a hex string to a byte array.
     *
     * <p>
     * Accepts formats with or without spaces: "00 01 FF" or "0001FF"
     * </p>
     *
     * @param hex The hex string
     * @return The byte array
     */
    private byte[] hexToBytes(String hex) {
        hex = hex.replaceAll("\\s+", ""); // Remove all whitespace
        int len = hex.length();
        if (len % 2 != 0) {
            throw new IllegalArgumentException("Hex string must have even length: " + hex);
        }
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
                    + Character.digit(hex.charAt(i + 1), 16));
        }
        return data;
    }

    /**
     * Converts a byte array to a hex string for display.
     *
     * @param bytes The byte array
     * @return Hex string formatted with spaces between bytes
     */
    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < bytes.length; i++) {
            if (i > 0) sb.append(' ');
            sb.append(String.format("%02X", bytes[i]));
        }
        return sb.toString();
    }
}
