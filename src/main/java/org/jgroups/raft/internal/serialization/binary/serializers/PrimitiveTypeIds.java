package org.jgroups.raft.internal.serialization.binary.serializers;

/**
 * Centralized registry of type IDs for built-in serializers.
 *
 * <p>
 * This class defines the type IDs used by the framework's internal serializers for Java primitive wrapper types and common
 * Java classes. All IDs are reserved in the range 0-999.
 * </p>
 *
 * <p>
 * Type IDs 0-31 are reserved for fast-path primitive types that use an optimized wire format without version and length fields.
 * Type IDs 32+ are used for complex types that include version and length fields for forward compatibility.
 * </p>
 *
 * <p>
 * <b>User-defined serializers must use type IDs starting from 1000.</b> Type IDs below 1000 are reserved for internal use
 * and attempting to register a user serializer with an ID in this range will throw an exception.
 * </p>
 *
 * @author José Bolina
 * @since 2.0
 */
public final class PrimitiveTypeIds {

    private PrimitiveTypeIds() { }

    /**
     * Fast-path primitive types (0-31).
     * These types use optimized wire format: [type-id][data]
     * No version or length fields for maximum performance.
     */
    public static final int BYTE_TYPE_ID = 0;
    public static final int SHORT_TYPE_ID = 1;
    public static final int INTEGER_TYPE_ID = 2;
    public static final int LONG_TYPE_ID = 3;
    public static final int FLOAT_TYPE_ID = 4;
    public static final int DOUBLE_TYPE_ID = 5;
    public static final int BOOLEAN_TYPE_ID = 6;
    public static final int CHARACTER_TYPE_ID = 7;
    public static final int STRING_TYPE_ID = 8;
    public static final int BYTE_ARRAY_TYPE_ID = 9;

    // Reserved: 10-31 for future fast-path types

    /**
     * Maximum type ID for fast-path primitives.
     * Type IDs &gt; FAST_PATH_MAX_TYPE_ID use the full wire format with version and length.
     */
    public static final int FAST_PATH_MAX_TYPE_ID = 31;

    /**
     * Complex types start at 32.
     * These use full wire format: [type-id][version][length][data]
     */
    private static final int COMPLEX_TYPE_BASE = 32;

    /**
     * Primitive array types.
     */
    public static final int SHORT_ARRAY_TYPE_ID = COMPLEX_TYPE_BASE;
    public static final int INT_ARRAY_TYPE_ID = COMPLEX_TYPE_BASE + 1;
    public static final int LONG_ARRAY_TYPE_ID = COMPLEX_TYPE_BASE + 2;
    public static final int FLOAT_ARRAY_TYPE_ID = COMPLEX_TYPE_BASE + 3;
    public static final int DOUBLE_ARRAY_TYPE_ID = COMPLEX_TYPE_BASE + 4;
    public static final int BOOLEAN_ARRAY_TYPE_ID = COMPLEX_TYPE_BASE + 5;
    public static final int CHAR_ARRAY_TYPE_ID = COMPLEX_TYPE_BASE + 6;

    /**
     * Collection types.
     */
    public static final int ARRAY_LIST_TYPE_ID = COMPLEX_TYPE_BASE + 7;
    public static final int LINKED_LIST_TYPE_ID = COMPLEX_TYPE_BASE + 8;
    public static final int HASH_SET_TYPE_ID = COMPLEX_TYPE_BASE + 9;
    public static final int LINKED_HASH_SET_TYPE_ID = COMPLEX_TYPE_BASE + 10;
    public static final int TREE_SET_TYPE_ID = COMPLEX_TYPE_BASE + 11;

    /**
     * Immutable and internal collections.
     */
    public static final int EMPTY_LIST_TYPE_ID = COMPLEX_TYPE_BASE + 12;
    public static final int EMPTY_SET_TYPE_ID = COMPLEX_TYPE_BASE + 13;
    public static final int SINGLETON_LIST_TYPE_ID = COMPLEX_TYPE_BASE + 14;
    public static final int SINGLETON_SET_TYPE_ID = COMPLEX_TYPE_BASE + 15;
    public static final int UNMODIFIABLE_LIST_TYPE_ID = COMPLEX_TYPE_BASE + 16;
    public static final int UNMODIFIABLE_SET_TYPE_ID = COMPLEX_TYPE_BASE + 17;
    public static final int LIST_OF_N_TYPE_ID = COMPLEX_TYPE_BASE + 18;
    public static final int LIST_OF_1_TYPE_ID = COMPLEX_TYPE_BASE + 19;
    public static final int SET_OF_N_TYPE_ID = COMPLEX_TYPE_BASE + 20;
    public static final int SET_OF_1_TYPE_ID = COMPLEX_TYPE_BASE + 21;

    /**
     * Maps and immutable maps.
     */
    public static final int HASH_MAP_TYPE_ID = COMPLEX_TYPE_BASE + 22;
    public static final int LINKED_HASH_MAP_TYPE_ID = COMPLEX_TYPE_BASE + 23;
    public static final int TREE_MAP_TYPE_ID = COMPLEX_TYPE_BASE + 24;
    public static final int EMPTY_MAP_TYPE_ID = COMPLEX_TYPE_BASE + 25;
    public static final int SINGLETON_MAP_TYPE_ID = COMPLEX_TYPE_BASE + 26;
    public static final int UNMODIFIABLE_MAP_TYPE_ID = COMPLEX_TYPE_BASE + 27;
    public static final int MAP_OF_1_TYPE_ID = COMPLEX_TYPE_BASE + 28;
    public static final int MAP_OF_N_TYPE_ID = COMPLEX_TYPE_BASE + 29;
}
