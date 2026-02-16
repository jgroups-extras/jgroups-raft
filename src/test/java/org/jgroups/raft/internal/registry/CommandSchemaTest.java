package org.jgroups.raft.internal.registry;

import org.jgroups.Global;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class CommandSchemaTest {

    // Dummy target to provide reliable reflection signatures for all schema type variants
    @SuppressWarnings("unused")
    interface SchemaTarget<T extends Number, U, I extends Number & Runnable> {
        String stringMethod();
        int primitiveIntMethod();
        Integer wrapperIntMethod();
        void voidMethod();
        List<String> parameterizedMethod();
        T boundedTypeVariableMethod();
        U unboundedTypeVariableMethod();
        T[] genericArrayMethod();
        List<? extends Number> wildcardMethod();
        I intersectionTypeMethod(); // Multi-bound generic
    }

    private Type getReturnType(String methodName) {
        try {
            Method method = SchemaTarget.class.getMethod(methodName);
            return method.getGenericReturnType();
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Failed to find method: " + methodName, e);
        }
    }

    public void testParameterizedTypeExactMatch() {
        CommandSchema schema = new CommandSchema(getReturnType("parameterizedMethod"));

        // The raw type is List.class, so exact match expects List.class
        assertThat(schema.isTypeExactMatch(List.class)).isTrue();

        // ArrayList is assignable, but not an EXACT match, so it must return false
        assertThat(schema.isTypeExactMatch(java.util.ArrayList.class)).isFalse();
    }

    public void testBoundedTypeVariableExactMatch() {
        CommandSchema schema = new CommandSchema(getReturnType("boundedTypeVariableMethod"));

        // 'T extends Number' requires exactly Number.class to match the upper bound constraint
        assertThat(schema.isTypeExactMatch(Number.class)).isTrue();

        // Integer extends Number, but it is not an exact schema match for 'T'
        assertThat(schema.isTypeExactMatch(Integer.class)).isFalse();
    }

    public void testUnboundedTypeVariableExactMatch() {
        CommandSchema schema = new CommandSchema(getReturnType("unboundedTypeVariableMethod"));

        // 'U' is unbounded, so its implicit upper bound is Object.class
        assertThat(schema.isTypeExactMatch(Object.class)).isTrue();
        assertThat(schema.isTypeExactMatch(String.class)).isFalse();
    }

    public void testGenericArrayTypeExactMatch() {
        CommandSchema schema = new CommandSchema(getReturnType("genericArrayMethod"));

        // 'T[]' where T extends Number. Expects exactly Number[].class
        assertThat(schema.isTypeExactMatch(Number[].class)).isTrue();

        // Sub-arrays or non-arrays must be rejected
        assertThat(schema.isTypeExactMatch(Integer[].class)).isFalse();
        assertThat(schema.isTypeExactMatch(Number.class)).isFalse();
    }

    public void testWildcardTypeExactMatch() {
        // Get the ParameterizedType "List<? extends Number>"
        ParameterizedType parameterizedType = (ParameterizedType) getReturnType("wildcardMethod");
        // Extract the actual WildcardType "? extends Number"
        Type wildcardType = parameterizedType.getActualTypeArguments()[0];

        CommandSchema schema = new CommandSchema(wildcardType);

        // The wildcard's upper bound is exactly Number.class
        assertThat(schema.isTypeExactMatch(Number.class)).isTrue();
        assertThat(schema.isTypeExactMatch(Integer.class)).isFalse();
        assertThat(schema.isTypeExactMatch(Object.class)).isFalse();
    }

    public void testClassExactMatch() {
        CommandSchema schema = new CommandSchema(getReturnType("stringMethod"));
        assertThat(schema.isTypeExactMatch(String.class)).isTrue();
    }

    public void testPrimitiveWrapperEquivalence() {
        CommandSchema primitiveSchema = new CommandSchema(getReturnType("primitiveIntMethod"));
        CommandSchema wrapperSchema = new CommandSchema(getReturnType("wrapperIntMethod"));

        assertThat(primitiveSchema.isTypeExactMatch(int.class)).isTrue();
        assertThat(primitiveSchema.isTypeExactMatch(Integer.class)).isTrue();

        assertThat(wrapperSchema.isTypeExactMatch(Integer.class)).isTrue();
        assertThat(wrapperSchema.isTypeExactMatch(int.class)).isTrue();
    }

    public void testVoidEquivalence() {
        CommandSchema schema = new CommandSchema(getReturnType("voidMethod"));
        assertThat(schema.isTypeExactMatch(void.class)).isTrue();
        assertThat(schema.isTypeExactMatch(Void.class)).isTrue();
    }

    public void testNegativeClassMatch() {
        CommandSchema schema = new CommandSchema(getReturnType("stringMethod"));

        // Subclasses, superclasses, and unrelated classes must be strictly rejected
        assertThat(schema.isTypeExactMatch(CharSequence.class)).isFalse();
        assertThat(schema.isTypeExactMatch(Object.class)).isFalse();
        assertThat(schema.isTypeExactMatch(Integer.class)).isFalse();
    }

    public void testNegativePrimitiveMatch() {
        CommandSchema primitiveSchema = new CommandSchema(getReturnType("primitiveIntMethod"));
        CommandSchema wrapperSchema = new CommandSchema(getReturnType("wrapperIntMethod"));

        // Unrelated primitives
        assertThat(primitiveSchema.isTypeExactMatch(long.class)).isFalse();
        assertThat(primitiveSchema.isTypeExactMatch(float.class)).isFalse();
        assertThat(primitiveSchema.isTypeExactMatch(boolean.class)).isFalse();

        // Unrelated wrappers
        assertThat(wrapperSchema.isTypeExactMatch(Long.class)).isFalse();
        assertThat(wrapperSchema.isTypeExactMatch(Double.class)).isFalse();

        // Primitive against completely unrelated objects
        assertThat(primitiveSchema.isTypeExactMatch(String.class)).isFalse();
    }

    public void testNegativeParameterizedTypeMatch() {
        CommandSchema schema = new CommandSchema(getReturnType("parameterizedMethod")); // List<String>

        // The raw type is List.class, so exact match expects List.class
        assertThat(schema.isTypeExactMatch(List.class)).isTrue();

        // Implementations of the interface are NOT an exact schema match
        assertThat(schema.isTypeExactMatch(java.util.ArrayList.class)).isFalse();

        // Completely unrelated parameterized types
        assertThat(schema.isTypeExactMatch(Set.class)).isFalse();
        assertThat(schema.isTypeExactMatch(Map.class)).isFalse();
    }

    public void testNegativeBoundedTypeVariableMatch() {
        CommandSchema schema = new CommandSchema(getReturnType("boundedTypeVariableMethod")); // T extends Number

        // Fails because String does not satisfy the 'Number' bound
        assertThat(schema.isTypeExactMatch(String.class)).isFalse();

        // Fails because Integer extends Number, but it is not exactly Number.class
        assertThat(schema.isTypeExactMatch(Integer.class)).isFalse();
    }

    public void testNegativeIntersectionTypeMatch() {
        CommandSchema schema = new CommandSchema(getReturnType("intersectionTypeMethod")); // I extends Number & Runnable

        // Exercises the generic.getBounds() loop.
        // Integer satisfies Number, but FAILS the Runnable bound.
        assertThat(schema.isTypeExactMatch(Integer.class)).isFalse();

        // Thread satisfies Runnable, but FAILS the Number bound.
        assertThat(schema.isTypeExactMatch(Thread.class)).isFalse();
    }

    public void testNegativeGenericArrayMatch() {
        CommandSchema schema = new CommandSchema(getReturnType("genericArrayMethod")); // T[] where T extends Number

        // Fails the `if (!clazz.isArray())` check
        assertThat(schema.isTypeExactMatch(List.class)).isFalse();
        assertThat(schema.isTypeExactMatch(Number.class)).isFalse();

        // Fails the inner component check (String[] does not satisfy Number[])
        assertThat(schema.isTypeExactMatch(String[].class)).isFalse();

        // Fails the exact class match on the bounds (Integer[] is not Number[])
        assertThat(schema.isTypeExactMatch(Integer[].class)).isFalse();
    }

    public void testNegativeWildcardMatch() {
        // Extract the actual WildcardType "? extends Number"
        ParameterizedType parameterizedType = (ParameterizedType) getReturnType("wildcardMethod");
        Type wildcardType = parameterizedType.getActualTypeArguments()[0];

        CommandSchema schema = new CommandSchema(wildcardType);

        // Subclasses and unrelated classes fail the strict exact match loop
        assertThat(schema.isTypeExactMatch(Integer.class)).isFalse();
        assertThat(schema.isTypeExactMatch(Object.class)).isFalse();
        assertThat(schema.isTypeExactMatch(String.class)).isFalse();
    }
}
