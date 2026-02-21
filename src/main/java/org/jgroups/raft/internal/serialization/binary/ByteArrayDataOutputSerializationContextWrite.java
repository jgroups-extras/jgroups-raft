package org.jgroups.raft.internal.serialization.binary;

import org.jgroups.raft.util.io.RandomAccessDataOutputStream;
import org.jgroups.util.ByteArrayDataOutputStream;

/**
 * Serialization context optimized for zero-copy streaming to {@link ByteArrayDataOutputStream}.
 *
 * <p>
 * This context enables direct serialization to JGroups output streams without intermediate buffering, eliminating memory
 * copies for large objects such as snapshots. It wraps the output stream in a {@link RandomAccessDataOutputStream} adapter
 * to provide the position manipulation required by the binary wire format.
 * </p>
 *
 * <h2>Use Case: Snapshot Serialization</h2>
 *
 * <p>
 * Snapshots can be very large (100+ MB) and are written infrequently. The standard serialization path using
 * {@link org.jgroups.raft.util.io.CustomByteBuffer} requires allocating a buffer, serializing into it, then copying the
 * entire content to the output stream:
 * </p>
 *
 * <pre>
 * Standard path (double allocation):
 *   CustomByteBuffer buffer = pool.acquire(100MB);  // Allocate
 *   ctx.writeObject(snapshot);                       // Serialize to buffer
 *   byte[] copy = buffer.toByteArray();              // Allocate 100 mb
 *   out.write(copy);                                 // Write to stream causes an extra 100mb copy
 *   Total: 200MB allocated
 *
 * Zero-copy path (single allocation):
 *   ByteArrayDataOutputSerializationContextWrite ctx = new(..., out);
 *   ctx.writeObject(snapshot);                       // Serialize directly to stream
 *   Total: ~100MB allocated (just the output stream)
 * </pre>
 *
 * <h2>Wire Format Compatibility</h2>
 *
 * <p>
 * This context produces <b>exactly the same wire format</b> as {@link DefaultSerializationContext}. Both inherit from
 * {@link AbstractSerializationContextWrite}, which ensures format consistency. Deserialization is identical regardless of
 * which context was used for serialization.
 * </p>
 *
 * <h2>Thread Safety</h2>
 *
 * <p>
 * Instances are not thread-safe. Each serialization operation should use a dedicated instance.
 * </p>
 *
 * <h2>Example Usage</h2>
 *
 * <pre>{@code
 * ByteArrayDataOutputStream out = new ByteArrayDataOutputStream(initialSize);
 * ByteArrayDataOutputSerializationContextWrite ctx =
 *     new ByteArrayDataOutputSerializationContextWrite(registry, out);
 *
 * // Write directly to stream without intermediate buffer
 * ctx.writeObject(largeSnapshot);
 *
 * // Get serialized bytes
 * byte[] result = out.buffer();
 * int length = out.position();
 * }</pre>
 *
 * @author José Bolina
 * @since 2.0
 * @see AbstractSerializationContextWrite
 * @see DefaultSerializationContext
 * @see RandomAccessDataOutputStream
 */
final class ByteArrayDataOutputSerializationContextWrite extends AbstractSerializationContextWrite {

    /**
     * Creates a new serialization context that writes directly to the given output stream.
     *
     * <p>
     * The stream is wrapped in a {@link RandomAccessDataOutputStream} adapter to provide the position manipulation
     * required for patching length fields in the wire format.
     * </p>
     *
     * @param registry The serializer registry for looking up serializers by type
     * @param out The output stream to write serialized data to
     * @throws NullPointerException if registry or out is null
     */
    ByteArrayDataOutputSerializationContextWrite(BinarySerializationRegistry registry, ByteArrayDataOutputStream out) {
        super(registry, new RandomAccessDataOutputStream(out));
    }
}
