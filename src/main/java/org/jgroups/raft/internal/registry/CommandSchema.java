package org.jgroups.raft.internal.registry;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.Objects;

/**
 * Represents the input or output schema of a method defined within a state machine.
 *
 * <p>
 * This record encapsulates the reflection {@link Type} of a method's parameter or return value of method with the
 * {@link org.jgroups.raft.internal.command.JRaftWriteCommand} or {@link org.jgroups.raft.internal.command.JRaftReadCommand}
 * annotations. It provides validation mechanisms utilized both during runtime to ensure data complies with the API
 * contract, and during system restart to verify compatibility between the persisted schema and the current Java definitions.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 * @param type The underlying {@link Type} of the state machine method's parameter or return value.
 */
record CommandSchema(Type type) {

    /**
     * Checks whether the provided object is an acceptable instance of this schema's type.
     *
     * <p>
     * This evaluation accounts for inheritance and generic bounds (e.g., allowing subclasses or interface implementations).
     * This is utilized during runtime to assert that the request payload or the response of the state machine complies
     * with the defined contract in the state machine API.
     * </p>
     *
     * @param obj The object to validate against the schema.
     * @return {@code true} if the object is of an acceptable type, {@code false} otherwise.
     */
    public boolean isTypeAllowed(Object obj) {
        return isObjectOfType(obj, type);
    }

    /**
     * Verifies if the provided class is an exact type match to this schema.
     *
     * <p>
     * Unlike {@link #isTypeAllowed(Object)}, this strictly checks for type equivalence rather than assignability.
     * This is utilized after a system restart to validate that the state machine schema stored on disk perfectly matches
     * the current Java class, verifying backwards compatibility.
     * </p>
     *
     * @param other The class to compare against this schema's type.
     * @return {@code true} if the types are an exact match, {@code false} otherwise.
     */
    public boolean isTypeExactMatch(Class<?> other) {
        return isEquivalent(other, type);
    }

    /**
     * Recursively evaluates strict equivalence between a concrete class and a generic type.
     *
     * @param clazz The concrete class to evaluate.
     * @param type  The target reflection type (Class, ParameterizedType, GenericArrayType, etc.).
     * @return {@code true} if the class satisfies the exact type constraints.
     */
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
    private static boolean isEquivalent(Class<?> a, Class<?> b) {
        if (Objects.equals(a, b))
            return true;

        if (a.isPrimitive())
            return isPrimitiveMatch(b, a);

        if (b.isPrimitive())
            return isPrimitiveMatch(a, b);

        return false;
    }

    /**
     * Recursively evaluates if an instantiated object is assignable to a specific reflection type.
     *
     * @param obj  The object to evaluate.
     * @param type The target reflection type.
     * @return {@code true} if the object is an instance of the type or satisfies its bounds.
     */
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
            Class<?> arrComponentClass = obj.getClass().getComponentType();
            return isClassAssignable(arrComponentClass, componentType);
        }

        if (type instanceof TypeVariable<?> generic) {
            for (Type bound : generic.getBounds()) {
                if (!isObjectOfType(obj, bound))
                    return false;
            }
            return true;
        }

        if (type instanceof WildcardType wildcard) {
            for (Type bound : wildcard.getUpperBounds()) {
                if (!isObjectOfType(obj, bound))
                    return false;
            }
            return true;
        }

        return false;
    }

    /**
     * Recursively evaluates if a class is an assignable subclass of a reflection type.
     *
     * <p>
     * This is specifically utilized for array evaluation where a concrete object instance
     * of the array's component is not available.
     * </p>
     *
     * @param clazz The class to evaluate (e.g., an array's component type).
     * @param type  The target reflection type bounds.
     * @return {@code true} if the class satisfies the bounds.
     */
    private static boolean isClassAssignable(Class<?> clazz, Type type) {
        if (type instanceof Class<?> c)
            return c.isAssignableFrom(clazz);

        if (type instanceof ParameterizedType p)
            return isClassAssignable(clazz, p.getRawType());

        if (type instanceof TypeVariable<?> generic) {
            for (Type bound : generic.getBounds()) {
                if (!isClassAssignable(clazz, bound))
                    return false;
            }
            return true;
        }

        if (type instanceof WildcardType wildcard) {
            for (Type bound : wildcard.getUpperBounds()) {
                if (!isClassAssignable(clazz, bound))
                    return false;
            }
            return true;
        }

        return false;
    }

    /**
     * Checks if an object matches a primitive class, accounting for auto-boxing.
     *
     * @param obj           The object to check.
     * @param primitiveType The primitive class (e.g., int.class).
     * @return {@code true} if the object matches the primitive type.
     */
    private static boolean isPrimitiveMatch(Object obj, Class<?> primitiveType) {
        if (obj == null) return primitiveType == void.class || primitiveType == Void.class;

        Class<?> objClass = obj.getClass();

        // Map primitive types to their wrapper classes
        return isPrimitiveMatch(objClass, primitiveType);
    }

    /**
     * Maps primitive classes to their corresponding wrapper classes.
     *
     * @param a The wrapper class (e.g., Integer.class).
     * @param b The primitive class (e.g., int.class).
     * @return {@code true} if the classes represent the same underlying primitive type.
     */
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
