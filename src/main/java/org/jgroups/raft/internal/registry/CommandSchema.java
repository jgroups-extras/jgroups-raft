package org.jgroups.raft.internal.registry;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Objects;

record CommandSchema(Type type) {

    public boolean isAcceptable(Object obj) {
        return isObjectOfType(obj, type);
    }

    public boolean isTypeAcceptable(Class<?> other) {
        return isEquivalent(other, type);
    }

    private static boolean isEquivalent(Class<?> clazz, Type type) {
        if (type instanceof Class<?> b)
            return isEquivalent(clazz, b);

        if (type instanceof ParameterizedType param) {
            Class<?> b = (Class<?>) param.getRawType();
            return isEquivalent(clazz, b);
        }

        if (type instanceof GenericArrayType arr) {
            if (!clazz.isArray()) return false;

            Type component = arr.getGenericComponentType();
            return isEquivalent(clazz, component);
        }

        return false;
    }

    private static boolean isEquivalent(Class<?> a, Class<?> b) {
        if (Objects.equals(a, b))
            return true;

        if (a.isPrimitive())
            return isPrimitiveMatch(b, a);

        if (b.isPrimitive())
            return isPrimitiveMatch(a, b);

        return false;
    }

    private static boolean isObjectOfType(Object obj, Type type) {
        if (type instanceof Class<?> clazz) {
            if (clazz.isPrimitive())
                return isPrimitiveMatch(obj, clazz);

            return clazz.isInstance(obj);
        }

        if (type instanceof ParameterizedType param) {
            Class<?> rawType = (Class<?>) param.getRawType();
            return rawType.isInstance(obj);
        }

        if (type instanceof GenericArrayType arrType) {
            if (obj == null || !obj.getClass().isArray())
                return false;

            Type componentType = arrType.getGenericComponentType();
            return isObjectOfType(obj, componentType);
        }

        return false;
    }

    private static boolean isPrimitiveMatch(Object obj, Class<?> primitiveType) {
        if (obj == null) return primitiveType == void.class || primitiveType == Void.class;

        Class<?> objClass = obj.getClass();

        // Map primitive types to their wrapper classes
        return isPrimitiveMatch(objClass, primitiveType);
    }

    private static boolean isPrimitiveMatch(Class<?> a, Class<?> b) {
        return b == int.class && a == Integer.class
                || b == double.class && a == Double.class
                || b == boolean.class && a == Boolean.class
                || b == char.class && a == Character.class
                || b == byte.class && a == Byte.class
                || b == short.class && a == Short.class
                || b == long.class && a == Long.class
                || b == float.class && a == Float.class
                || b == void.class && a == Void.class;
    }
}
