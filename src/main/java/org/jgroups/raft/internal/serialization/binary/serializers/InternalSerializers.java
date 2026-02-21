package org.jgroups.raft.internal.serialization.binary.serializers;

import org.jgroups.raft.internal.serialization.SingleBinarySerializer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Registry of all built-in serializers provided by the framework.
 *
 * <p>
 * This class aggregates all internal serializers for Java primitive wrapper types and common Java classes. These serializers
 * are automatically registered when creating a serialization context and do not need to be registered manually.
 * </p>
 *
 * <p>
 * The built-in serializers currently include:
 * </p>
 * <ul>
 *   <li>Numeric types: {@link Byte}, {@link Short}, {@link Integer}, {@link Long}, {@link Float}, {@link Double}</li>
 *   <li>Character sequences: {@link String}</li>
 * </ul>
 *
 * <p>
 * <b>Note:</b> This is an internal class and should not be used directly by users.
 * </p>
 *
 * @author José Bolina
 * @since 2.0
 */

public final class InternalSerializers {
    private static final List<SingleBinarySerializer<?>> SERIALIZERS;

    static {
        List<SingleBinarySerializer<?>> serializers = new ArrayList<>();
        serializers.addAll(List.of(NumberSerializer.SERIALIZERS));
        serializers.addAll(List.of(CharSequenceSerializer.SERIALIZERS));
        serializers.addAll(List.of(PrimitiveArraySerializer.SERIALIZERS));
        serializers.addAll(List.of(CollectionSerializer.SERIALIZERS));
        serializers.addAll(List.of(ImmutableCollectionSerializer.SERIALIZERS));
        serializers.addAll(List.of(MapSerializer.SERIALIZERS));
        serializers.addAll(List.of(ImmutableMapSerializer.SERIALIZERS));

        SERIALIZERS = Collections.unmodifiableList(serializers);
    }

    private InternalSerializers() { }

    /**
     * Returns an unmodifiable collection of all built-in serializers.
     *
     * @return The collection of internal serializers
     */
    public static Collection<SingleBinarySerializer<?>> serializers() {
        return SERIALIZERS;
    }
}
