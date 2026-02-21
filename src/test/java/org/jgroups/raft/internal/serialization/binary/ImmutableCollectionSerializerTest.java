package org.jgroups.raft.internal.serialization.binary;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.jgroups.Global;
import org.jgroups.raft.internal.serialization.Serializer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests for immutable collection serialization with {@link BinarySerializer}.
 *
 * <p>
 * This test class validates serialization and deserialization for JVM-provided immutable collections,
 * ensuring that immutability is preserved across serialization boundaries. Tests cover empty collections,
 * singleton collections, and collections created via {@link List#of(Object...)} and {@link Set#of(Object...)}.
 * </p>
 *
 * @author José Bolina
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class ImmutableCollectionSerializerTest {

    private Serializer serializer;

    @BeforeMethod
    public void setUp() {
        SerializationRegistry registry = SerializationRegistry.create();
        serializer = Serializer.create(registry);
    }

    // ========== Collections.emptyList() Tests ==========

    @Test
    public void testEmptyListRoundTrip() {
        List<String> original = Collections.emptyList();

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        List<String> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThat(deserialized).isEmpty();
        assertThat(deserialized).isEqualTo(Collections.emptyList());
    }

    @Test
    public void testEmptyListIsImmutable() {
        List<String> original = Collections.emptyList();

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        List<String> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThatThrownBy(() -> deserialized.add("test"))
            .isInstanceOf(UnsupportedOperationException.class);
    }

    // ========== Collections.emptySet() Tests ==========

    @Test
    public void testEmptySetRoundTrip() {
        Set<String> original = Collections.emptySet();

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        Set<String> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThat(deserialized).isEmpty();
        assertThat(deserialized).isEqualTo(Collections.emptySet());
    }

    @Test
    public void testEmptySetIsImmutable() {
        Set<String> original = Collections.emptySet();

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        Set<String> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThatThrownBy(() -> deserialized.add("test"))
            .isInstanceOf(UnsupportedOperationException.class);
    }

    // ========== Collections.singletonList() Tests ==========

    @Test
    public void testSingletonListWithString() {
        List<String> original = Collections.singletonList("only-one");

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        List<String> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThat(deserialized).containsExactly("only-one");
    }

    @Test
    public void testSingletonListWithInteger() {
        List<Integer> original = Collections.singletonList(42);

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        List<Integer> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThat(deserialized).containsExactly(42);
    }

    @Test
    public void testSingletonListWithNull() {
        List<String> original = Collections.singletonList(null);

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        List<String> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThat(deserialized).containsExactly((String) null);
    }

    @Test
    public void testSingletonListIsImmutable() {
        List<String> original = Collections.singletonList("test");

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        List<String> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThatThrownBy(() -> deserialized.add("another"))
            .isInstanceOf(UnsupportedOperationException.class);
    }

    // ========== Collections.singleton() Tests ==========

    @Test
    public void testSingletonSetWithString() {
        Set<String> original = Collections.singleton("only-one");

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        Set<String> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThat(deserialized).containsExactly("only-one");
    }

    @Test
    public void testSingletonSetWithInteger() {
        Set<Integer> original = Collections.singleton(100);

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        Set<Integer> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThat(deserialized).containsExactly(100);
    }

    @Test
    public void testSingletonSetIsImmutable() {
        Set<String> original = Collections.singleton("test");

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        Set<String> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThatThrownBy(() -> deserialized.add("another"))
            .isInstanceOf(UnsupportedOperationException.class);
    }

    // ========== Collections.unmodifiableList() Tests ==========

    @Test
    public void testUnmodifiableListRoundTrip() {
        List<String> mutableList = new ArrayList<>();
        mutableList.add("one");
        mutableList.add("two");
        mutableList.add("three");
        List<String> original = Collections.unmodifiableList(mutableList);

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        List<String> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThat(deserialized).containsExactly("one", "two", "three");
    }

    @Test
    public void testUnmodifiableListIsImmutable() {
        List<String> original = Collections.unmodifiableList(List.of("test"));

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        List<String> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThatThrownBy(() -> deserialized.add("another"))
            .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testUnmodifiableListEmpty() {
        List<String> original = Collections.unmodifiableList(new ArrayList<>());

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        List<String> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThat(deserialized).isEmpty();
    }

    // ========== Collections.unmodifiableSet() Tests ==========

    @Test
    public void testUnmodifiableSetRoundTrip() {
        Set<String> mutableSet = new HashSet<>();
        mutableSet.add("alpha");
        mutableSet.add("beta");
        mutableSet.add("gamma");
        Set<String> original = Collections.unmodifiableSet(mutableSet);

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        Set<String> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThat(deserialized).containsExactlyInAnyOrder("alpha", "beta", "gamma");
    }

    @Test
    public void testUnmodifiableSetIsImmutable() {
        Set<String> original = Collections.unmodifiableSet(Set.of("test"));

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        Set<String> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThatThrownBy(() -> deserialized.add("another"))
            .isInstanceOf(UnsupportedOperationException.class);
    }

    // ========== List.of() Tests ==========

    @Test
    public void testListOfEmpty() {
        List<String> original = List.of();

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        List<String> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThat(deserialized).isEmpty();
        assertThat(deserialized).isEqualTo(List.of());
    }

    @Test
    public void testListOfOneElement() {
        List<String> original = List.of("single");

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        List<String> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThat(deserialized).containsExactly("single");
    }

    @Test
    public void testListOfTwoElements() {
        List<String> original = List.of("first", "second");

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        List<String> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThat(deserialized).containsExactly("first", "second");
    }

    @Test
    public void testListOfMultipleElements() {
        List<String> original = List.of("one", "two", "three", "four", "five");

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        List<String> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThat(deserialized).containsExactly("one", "two", "three", "four", "five");
    }

    @Test
    public void testListOfWithIntegers() {
        List<Integer> original = List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        List<Integer> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThat(deserialized).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    @Test
    public void testListOfIsImmutable() {
        List<String> original = List.of("one", "two", "three");

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        List<String> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThatThrownBy(() -> deserialized.add("four"))
            .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testListOfNested() {
        List<List<Integer>> original = List.of(
            List.of(1, 2, 3),
            List.of(4, 5, 6),
            List.of(7, 8, 9)
        );

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        List<List<Integer>> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThat(deserialized).hasSize(3);
        assertThat(deserialized.get(0)).containsExactly(1, 2, 3);
        assertThat(deserialized.get(1)).containsExactly(4, 5, 6);
        assertThat(deserialized.get(2)).containsExactly(7, 8, 9);
    }

    // ========== Set.of() Tests ==========

    @Test
    public void testSetOfEmpty() {
        Set<String> original = Set.of();

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        Set<String> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThat(deserialized).isEmpty();
        assertThat(deserialized).isEqualTo(Set.of());
    }

    @Test
    public void testSetOfOneElement() {
        Set<String> original = Set.of("single");

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        Set<String> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThat(deserialized).containsExactly("single");
    }

    @Test
    public void testSetOfTwoElements() {
        Set<String> original = Set.of("first", "second");

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        Set<String> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThat(deserialized).containsExactlyInAnyOrder("first", "second");
    }

    @Test
    public void testSetOfMultipleElements() {
        Set<String> original = Set.of("one", "two", "three", "four", "five");

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        Set<String> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThat(deserialized).containsExactlyInAnyOrder("one", "two", "three", "four", "five");
    }

    @Test
    public void testSetOfWithIntegers() {
        Set<Integer> original = Set.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        Set<Integer> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThat(deserialized).containsExactlyInAnyOrder(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    @Test
    public void testSetOfIsImmutable() {
        Set<String> original = Set.of("one", "two", "three");

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        Set<String> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThatThrownBy(() -> deserialized.add("four"))
            .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testSetOfNested() {
        Set<Set<String>> original = Set.of(
            Set.of("a", "b"),
            Set.of("c", "d"),
            Set.of("e", "f")
        );

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        Set<Set<String>> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThat(deserialized).hasSize(3);
        assertThat(deserialized).containsExactlyInAnyOrder(
            Set.of("a", "b"),
            Set.of("c", "d"),
            Set.of("e", "f")
        );
    }

    // ========== Large Collection Tests ==========

    @Test
    public void testListOfLargeCollection() {
        Integer[] elements = new Integer[100];
        for (int i = 0; i < 100; i++) {
            elements[i] = i;
        }
        List<Integer> original = List.of(elements);

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        List<Integer> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThat(deserialized).hasSize(100);
        assertThat(deserialized).isEqualTo(original);
    }

    @Test
    public void testSetOfLargeCollection() {
        Integer[] elements = new Integer[100];
        for (int i = 0; i < 100; i++) {
            elements[i] = i;
        }
        Set<Integer> original = Set.of(elements);

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        Set<Integer> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThat(deserialized).hasSize(100);
        assertThat(deserialized).isEqualTo(original);
    }

    // ========== Mixed Types Tests ==========

    @Test
    public void testListOfWithMixedPrimitiveTypes() {
        List<Number> original = List.of(1, 2L, 3.0f, 4.0);

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        List<Number> deserialized = serializer.deserialize(bytes, original.getClass());

        assertThat(deserialized).hasSize(4);
        assertThat(deserialized.get(0)).isEqualTo(1);
        assertThat(deserialized.get(1)).isEqualTo(2L);
        assertThat(deserialized.get(2)).isEqualTo(3.0f);
        assertThat(deserialized.get(3)).isEqualTo(4.0);
    }

    // ========== Null Handling ==========

    @Test
    public void testNullImmutableList() {
        byte[] bytes = serializer.serialize(null);
        List<?> deserialized = serializer.deserialize(bytes, List.class);
        assertThat(deserialized).isNull();
    }

    @Test
    public void testNullImmutableSet() {
        byte[] bytes = serializer.serialize(null);
        Set<?> deserialized = serializer.deserialize(bytes, Set.class);
        assertThat(deserialized).isNull();
    }
}
