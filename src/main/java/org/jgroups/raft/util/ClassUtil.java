package org.jgroups.raft.util;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.Objects;

public final class ClassUtil {

    private ClassUtil() { }

    /**
     * Recursively evaluates strict equivalence between a concrete class and a generic type.
     *
     * @param clazz The concrete class to evaluate.
     * @param type  The target reflection type (Class, ParameterizedType, GenericArrayType, etc.).
     * @return {@code true} if the class satisfies the exact type constraints.
     */
    public static boolean isEquivalent(Class<?> clazz, Type type) {
        if (type instanceof Class<?> b)
            return isEquivalent(clazz, b);

        if (type instanceof ParameterizedType param) {
            Class<?> b = (Class<?>) param.getRawType();
            return isEquivalent(clazz, b);
        }

        if (type instanceof GenericArrayType arr) {
            if (!clazz.isArray()) return false;

            Type component = arr.getGenericComponentType();
            return isEquivalent(clazz.getComponentType(), component);
        }

        if (type instanceof TypeVariable<?> generic) {
            // The generic type must satisfy its upper bound.
            for (Type bound : generic.getBounds()) {
                if (!isEquivalent(clazz, bound))
                    return false;
            }
            return true;
        }

        // A wildcard type is the one defined as <? extends Something>.
        if (type instanceof WildcardType wildcard) {
            for (Type bound : wildcard.getUpperBounds()) {
                if (!isEquivalent(clazz, bound))
                    return false;
            }
            return true;
        }

        return false;
    }

    /**
     * Evaluates strict equivalence between two concrete classes, accounting for primitive wrappers.
     *
     * @param a The first class.
     * @param b The second class.
     * @return {@code true} if the classes are equal or represent the same primitive/wrapper pair.
     */
    public static boolean isEquivalent(Class<?> a, Class<?> b) {
        if (Objects.equals(a, b))
            return true;

        if (a.isPrimitive())
            return isPrimitiveMatch(b, a);

        if (b.isPrimitive())
            return isPrimitiveMatch(a, b);

        return false;
    }

    /**
     * Maps primitive classes to their corresponding wrapper classes.
     *
     * @param a The wrapper class (e.g., Integer.class).
     * @param b The primitive class (e.g., int.class).
     * @return {@code true} if the classes represent the same underlying primitive type.
     */
    public static boolean isPrimitiveMatch(Class<?> a, Class<?> b) {
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
