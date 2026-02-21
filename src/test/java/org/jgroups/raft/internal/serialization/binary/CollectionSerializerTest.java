package org.jgroups.raft.internal.serialization.binary;

import static org.assertj.core.api.Assertions.assertThat;

import org.jgroups.Global;
import org.jgroups.raft.internal.serialization.Serializer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests for mutable collection serialization with {@link BinarySerializer}.
 *
 * <p>
 * This test class validates serialization and deserialization for common mutable collection types,
 * including lists and sets. Tests verify proper handling of empty collections, collections with
 * primitives (auto-boxed), nested collections, and collections with null elements.
 * </p>
 *
 * @author José Bolina
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class CollectionSerializerTest {

    private Serializer serializer;

    @BeforeMethod
    public void setUp() {
        SerializationRegistry registry = SerializationRegistry.create();
        serializer = Serializer.create(registry);
    }

    // ========== ArrayList Tests ==========

    @Test
    public void testArrayListWithStrings() {
        ArrayList<String> original = new ArrayList<>();
        original.add("one");
        original.add("two");
        original.add("three");

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        ArrayList<String> deserialized = serializer.deserialize(bytes, ArrayList.class);

        assertThat(deserialized).containsExactly("one", "two", "three");
    }

    @Test
    public void testArrayListWithIntegers() {
        ArrayList<Integer> original = new ArrayList<>();
        original.add(1);
        original.add(2);
        original.add(3);
        original.add(4);
        original.add(5);

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        ArrayList<Integer> deserialized = serializer.deserialize(bytes, ArrayList.class);

        assertThat(deserialized).containsExactly(1, 2, 3, 4, 5);
    }

    @Test
    public void testEmptyArrayList() {
        ArrayList<String> original = new ArrayList<>();

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        ArrayList<String> deserialized = serializer.deserialize(bytes, ArrayList.class);

        assertThat(deserialized).isEmpty();
    }

    @Test
    public void testArrayListWithNullElements() {
        ArrayList<String> original = new ArrayList<>();
        original.add("one");
        original.add(null);
        original.add("three");

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        ArrayList<String> deserialized = serializer.deserialize(bytes, ArrayList.class);

        assertThat(deserialized).containsExactly("one", null, "three");
    }

    @Test
    public void testArrayListNested() {
        ArrayList<List<Integer>> original = new ArrayList<>();
        original.add(List.of(1, 2, 3));
        original.add(List.of(4, 5, 6));
        original.add(List.of(7, 8, 9));

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        ArrayList<List<Integer>> deserialized = serializer.deserialize(bytes, ArrayList.class);

        assertThat(deserialized).hasSize(3);
        assertThat(deserialized.get(0)).containsExactly(1, 2, 3);
        assertThat(deserialized.get(1)).containsExactly(4, 5, 6);
        assertThat(deserialized.get(2)).containsExactly(7, 8, 9);
    }

    @Test
    public void testLargeArrayList() {
        ArrayList<Integer> original = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            original.add(i);
        }

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        ArrayList<Integer> deserialized = serializer.deserialize(bytes, ArrayList.class);

        assertThat(deserialized).hasSize(1000);
        assertThat(deserialized).isEqualTo(original);
    }

    // ========== LinkedList Tests ==========

    @Test
    public void testLinkedListWithStrings() {
        LinkedList<String> original = new LinkedList<>();
        original.add("first");
        original.add("second");
        original.add("third");

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        LinkedList<String> deserialized = serializer.deserialize(bytes, LinkedList.class);

        assertThat(deserialized).containsExactly("first", "second", "third");
    }

    @Test
    public void testEmptyLinkedList() {
        LinkedList<Integer> original = new LinkedList<>();

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        LinkedList<Integer> deserialized = serializer.deserialize(bytes, LinkedList.class);

        assertThat(deserialized).isEmpty();
    }

    @Test
    public void testLinkedListWithPrimitives() {
        LinkedList<Long> original = new LinkedList<>();
        original.add(100L);
        original.add(200L);
        original.add(300L);

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        LinkedList<Long> deserialized = serializer.deserialize(bytes, LinkedList.class);

        assertThat(deserialized).containsExactly(100L, 200L, 300L);
    }

    // ========== HashSet Tests ==========

    @Test
    public void testHashSetWithStrings() {
        HashSet<String> original = new HashSet<>();
        original.add("apple");
        original.add("banana");
        original.add("cherry");

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        HashSet<String> deserialized = serializer.deserialize(bytes, HashSet.class);

        assertThat(deserialized).containsExactlyInAnyOrder("apple", "banana", "cherry");
    }

    @Test
    public void testHashSetWithIntegers() {
        HashSet<Integer> original = new HashSet<>();
        original.add(10);
        original.add(20);
        original.add(30);
        original.add(40);

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        HashSet<Integer> deserialized = serializer.deserialize(bytes, HashSet.class);

        assertThat(deserialized).containsExactlyInAnyOrder(10, 20, 30, 40);
    }

    @Test
    public void testEmptyHashSet() {
        HashSet<String> original = new HashSet<>();

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        HashSet<String> deserialized = serializer.deserialize(bytes, HashSet.class);

        assertThat(deserialized).isEmpty();
    }

    @Test
    public void testHashSetWithNullElement() {
        HashSet<String> original = new HashSet<>();
        original.add("one");
        original.add(null);
        original.add("three");

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        HashSet<String> deserialized = serializer.deserialize(bytes, HashSet.class);

        assertThat(deserialized).containsExactlyInAnyOrder("one", null, "three");
    }

    @Test
    public void testHashSetNested() {
        HashSet<Set<String>> original = new HashSet<>();
        original.add(Set.of("a", "b"));
        original.add(Set.of("c", "d"));

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        HashSet<Set<String>> deserialized = serializer.deserialize(bytes, HashSet.class);

        assertThat(deserialized).hasSize(2);
        System.out.println(deserialized);
        assertThat(deserialized).containsExactlyInAnyOrder(Set.of("a", "b"), Set.of("c", "d"));
    }

    // ========== LinkedHashSet Tests ==========

    @Test
    public void testLinkedHashSetMaintainsOrder() {
        LinkedHashSet<String> original = new LinkedHashSet<>();
        original.add("first");
        original.add("second");
        original.add("third");
        original.add("fourth");

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        LinkedHashSet<String> deserialized = serializer.deserialize(bytes, LinkedHashSet.class);

        // LinkedHashSet maintains insertion order
        assertThat(deserialized).containsExactly("first", "second", "third", "fourth");
    }

    @Test
    public void testEmptyLinkedHashSet() {
        LinkedHashSet<Integer> original = new LinkedHashSet<>();

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        LinkedHashSet<Integer> deserialized = serializer.deserialize(bytes, LinkedHashSet.class);

        assertThat(deserialized).isEmpty();
    }

    @Test
    public void testLinkedHashSetWithPrimitives() {
        LinkedHashSet<Double> original = new LinkedHashSet<>();
        original.add(1.1);
        original.add(2.2);
        original.add(3.3);

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        LinkedHashSet<Double> deserialized = serializer.deserialize(bytes, LinkedHashSet.class);

        assertThat(deserialized).containsExactly(1.1, 2.2, 3.3);
    }

    // ========== TreeSet Tests ==========

    @Test
    public void testTreeSetMaintainsSortedOrder() {
        TreeSet<Integer> original = new TreeSet<>();
        original.add(30);
        original.add(10);
        original.add(20);
        original.add(50);
        original.add(40);

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        TreeSet<Integer> deserialized = serializer.deserialize(bytes, TreeSet.class);

        // TreeSet maintains sorted order
        assertThat(deserialized).containsExactly(10, 20, 30, 40, 50);
    }

    @Test
    public void testTreeSetWithStrings() {
        TreeSet<String> original = new TreeSet<>();
        original.add("zebra");
        original.add("apple");
        original.add("monkey");
        original.add("banana");

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        TreeSet<String> deserialized = serializer.deserialize(bytes, TreeSet.class);

        assertThat(deserialized).containsExactly("apple", "banana", "monkey", "zebra");
    }

    @Test
    public void testEmptyTreeSet() {
        TreeSet<String> original = new TreeSet<>();

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        TreeSet<String> deserialized = serializer.deserialize(bytes, TreeSet.class);

        assertThat(deserialized).isEmpty();
    }

    // ========== Mixed Type Tests ==========

    @Test
    public void testArrayListWithMixedPrimitiveTypes() {
        ArrayList<Number> original = new ArrayList<>();
        original.add(1);          // Integer
        original.add(2L);         // Long
        original.add(3.0f);       // Float
        original.add(4.0);        // Double

        byte[] bytes = serializer.serialize(original);
        @SuppressWarnings("unchecked")
        ArrayList<Number> deserialized = serializer.deserialize(bytes, ArrayList.class);

        assertThat(deserialized).hasSize(4);
        assertThat(deserialized.get(0)).isEqualTo(1);
        assertThat(deserialized.get(1)).isEqualTo(2L);
        assertThat(deserialized.get(2)).isEqualTo(3.0f);
        assertThat(deserialized.get(3)).isEqualTo(4.0);
    }

    // ========== Null Handling ==========

    @Test
    public void testNullArrayList() {
        byte[] bytes = serializer.serialize(null);
        ArrayList<?> deserialized = serializer.deserialize(bytes, ArrayList.class);
        assertThat(deserialized).isNull();
    }

    @Test
    public void testNullHashSet() {
        byte[] bytes = serializer.serialize(null);
        HashSet<?> deserialized = serializer.deserialize(bytes, HashSet.class);
        assertThat(deserialized).isNull();
    }
}
