package org.jgroups.raft.util.io;

import java.io.DataOutput;

/**
 * A {@link DataOutput} extension that supports random access positioning.
 *
 * <p>
 * This interface extends {@link DataOutput} with position manipulation capabilities, enabling the serialization framework
 * to patch length fields after writing variable-length data. This is essential for maintaining wire format compatibility
 * while avoiding buffering entire objects.
 * </p>
 *
 * <p>
 * <b>Use Cases:</b>
 * </p>
 * <ul>
 *   <li>Binary serialization with embedded length fields</li>
 *   <li>Zero-copy streaming to output streams</li>
 *   <li>Forward-compatible wire formats that allow field skipping</li>
 * </ul>
 *
 * <p>
 * <b>Example Usage:</b>
 * </p>
 * <pre>{@code
 * RandomAccessOutput out = ...;
 *
 * // Reserve space for length
 * int lengthPos = out.position();
 * out.writeInt(0);  // Placeholder
 *
 * // Write variable-length data
 * int dataStart = out.position();
 * writeComplexObject(out);
 *
 * // Patch the length field
 * int dataLength = out.position() - dataStart;
 * out.position(lengthPos);
 * out.writeInt(dataLength);
 * out.position(dataStart + dataLength);
 * }</pre>
 *
 * <p>
 * Implementations must ensure that position changes do not affect data integrity and that subsequent writes occur at the
 * correct position.
 * </p>
 *
 * @author José Bolina
 * @since 2.0
 * @see CustomByteBuffer
 * @see RandomAccessDataOutputStream
 */
public interface RandomAccessOutput extends DataOutput {

    /**
     * Returns the current write position in bytes.
     *
     * <p>
     * The position indicates where the next write operation will occur. Position zero represents the start of the output.
     * </p>
     *
     * @return The current position in bytes (zero-based)
     */
    int position();

    /**
     * Sets the write position to the specified byte offset.
     *
     * <p>
     * This method is used to patch previously written data, typically for updating length fields after writing
     * variable-length content. Setting the position does not modify any existing data.
     * </p>
     *
     * <p>
     * <b>Constraints:</b>
     * </p>
     * <ul>
     *   <li>Position must be non-negative</li>
     *   <li>Position may be set beyond current data (creates gap)</li>
     *   <li>Subsequent writes occur at the new position</li>
     * </ul>
     *
     * @param pos The new position in bytes (zero-based)
     * @throws IllegalArgumentException if pos is negative
     */
    void position(int pos);
}
