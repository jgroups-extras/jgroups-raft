package org.jgroups.raft.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.jgroups.Global;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL)
public class ClassUtilTest {

    @SuppressWarnings("unused")
    interface TypeTarget<T extends Number, U, I extends Number & Runnable> {
        String stringMethod();
        int primitiveIntMethod();
        Integer wrapperIntMethod();
        void voidMethod();
        List<String> parameterizedMethod();
        T boundedTypeVariableMethod();
        U unboundedTypeVariableMethod();
        T[] genericArrayMethod();
        List<? extends Number> wildcardMethod();
        I intersectionTypeMethod();
    }

    private Type getReturnType(String methodName) {
        try {
            Method method = TypeTarget.class.getMethod(methodName);
            return method.getGenericReturnType();
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Failed to find method: " + methodName, e);
        }
    }

    public void testParameterizedTypeExactMatch() {
        Type type = getReturnType("parameterizedMethod");
        assertThat(ClassUtil.isEquivalent(List.class, type)).isTrue();
        assertThat(ClassUtil.isEquivalent(java.util.ArrayList.class, type)).isFalse();
    }

    public void testBoundedTypeVariableExactMatch() {
        Type type = getReturnType("boundedTypeVariableMethod");
        assertThat(ClassUtil.isEquivalent(Number.class, type)).isTrue();
        assertThat(ClassUtil.isEquivalent(Integer.class, type)).isFalse();
    }

    public void testUnboundedTypeVariableExactMatch() {
        Type type = getReturnType("unboundedTypeVariableMethod");
        assertThat(ClassUtil.isEquivalent(Object.class, type)).isTrue();
        assertThat(ClassUtil.isEquivalent(String.class, type)).isFalse();
    }

    public void testGenericArrayTypeExactMatch() {
        Type type = getReturnType("genericArrayMethod");
        assertThat(ClassUtil.isEquivalent(Number[].class, type)).isTrue();
        assertThat(ClassUtil.isEquivalent(Integer[].class, type)).isFalse();
        assertThat(ClassUtil.isEquivalent(Number.class, type)).isFalse();
    }

    public void testWildcardTypeExactMatch() {
        ParameterizedType parameterizedType = (ParameterizedType) getReturnType("wildcardMethod");
        Type wildcardType = parameterizedType.getActualTypeArguments()[0];

        assertThat(ClassUtil.isEquivalent(Number.class, wildcardType)).isTrue();
        assertThat(ClassUtil.isEquivalent(Integer.class, wildcardType)).isFalse();
        assertThat(ClassUtil.isEquivalent(Object.class, wildcardType)).isFalse();
    }

    public void testClassExactMatch() {
        Type type = getReturnType("stringMethod");
        assertThat(ClassUtil.isEquivalent(String.class, type)).isTrue();
    }

    public void testPrimitiveWrapperEquivalence() {
        Type primitiveType = getReturnType("primitiveIntMethod");
        Type wrapperType = getReturnType("wrapperIntMethod");

        assertThat(ClassUtil.isEquivalent(int.class, primitiveType)).isTrue();
        assertThat(ClassUtil.isEquivalent(Integer.class, primitiveType)).isTrue();

        assertThat(ClassUtil.isEquivalent(Integer.class, wrapperType)).isTrue();
        assertThat(ClassUtil.isEquivalent(int.class, wrapperType)).isTrue();
    }

    public void testVoidEquivalence() {
        Type type = getReturnType("voidMethod");
        assertThat(ClassUtil.isEquivalent(void.class, type)).isTrue();
        assertThat(ClassUtil.isEquivalent(Void.class, type)).isTrue();
    }

    public void testNegativeClassMatch() {
        Type type = getReturnType("stringMethod");
        assertThat(ClassUtil.isEquivalent(CharSequence.class, type)).isFalse();
        assertThat(ClassUtil.isEquivalent(Object.class, type)).isFalse();
        assertThat(ClassUtil.isEquivalent(Integer.class, type)).isFalse();
    }

    public void testNegativePrimitiveMatch() {
        Type primitiveType = getReturnType("primitiveIntMethod");
        Type wrapperType = getReturnType("wrapperIntMethod");

        assertThat(ClassUtil.isEquivalent(long.class, primitiveType)).isFalse();
        assertThat(ClassUtil.isEquivalent(float.class, primitiveType)).isFalse();
        assertThat(ClassUtil.isEquivalent(boolean.class, primitiveType)).isFalse();

        assertThat(ClassUtil.isEquivalent(Long.class, wrapperType)).isFalse();
        assertThat(ClassUtil.isEquivalent(Double.class, wrapperType)).isFalse();

        assertThat(ClassUtil.isEquivalent(String.class, primitiveType)).isFalse();
    }

    public void testNegativeParameterizedTypeMatch() {
        Type type = getReturnType("parameterizedMethod");
        assertThat(ClassUtil.isEquivalent(List.class, type)).isTrue();
        assertThat(ClassUtil.isEquivalent(java.util.ArrayList.class, type)).isFalse();
        assertThat(ClassUtil.isEquivalent(Set.class, type)).isFalse();
        assertThat(ClassUtil.isEquivalent(Map.class, type)).isFalse();
    }

    public void testNegativeBoundedTypeVariableMatch() {
        Type type = getReturnType("boundedTypeVariableMethod");
        assertThat(ClassUtil.isEquivalent(String.class, type)).isFalse();
        assertThat(ClassUtil.isEquivalent(Integer.class, type)).isFalse();
    }

    public void testNegativeIntersectionTypeMatch() {
        Type type = getReturnType("intersectionTypeMethod");
        assertThat(ClassUtil.isEquivalent(Integer.class, type)).isFalse();
        assertThat(ClassUtil.isEquivalent(Thread.class, type)).isFalse();
    }

    public void testNegativeGenericArrayMatch() {
        Type type = getReturnType("genericArrayMethod");
        assertThat(ClassUtil.isEquivalent(List.class, type)).isFalse();
        assertThat(ClassUtil.isEquivalent(Number.class, type)).isFalse();
        assertThat(ClassUtil.isEquivalent(String[].class, type)).isFalse();
        assertThat(ClassUtil.isEquivalent(Integer[].class, type)).isFalse();
    }

    public void testNegativeWildcardMatch() {
        ParameterizedType parameterizedType = (ParameterizedType) getReturnType("wildcardMethod");
        Type wildcardType = parameterizedType.getActualTypeArguments()[0];

        assertThat(ClassUtil.isEquivalent(Integer.class, wildcardType)).isFalse();
        assertThat(ClassUtil.isEquivalent(Object.class, wildcardType)).isFalse();
        assertThat(ClassUtil.isEquivalent(String.class, wildcardType)).isFalse();
    }
}
