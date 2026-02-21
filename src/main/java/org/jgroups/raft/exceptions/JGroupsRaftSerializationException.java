package org.jgroups.raft.exceptions;

/**
 * Exception thrown when serialization or deserialization operations fail.
 *
 * <p>
 * This exception is thrown by the binary serialization framework when errors occur during object encoding or decoding.
 * It wraps underlying I/O exceptions or other failures to provide a consistent exception hierarchy for JGroups Raft errors.
 * </p>
 *
 * <h2>Common Causes</h2>
 *
 * <ul>
 *   <li><b>I/O failures:</b> Writing to or reading from streams fails</li>
 *   <li><b>Buffer overflow:</b> Data exceeds buffer capacity (rare due to auto-growth)</li>
 *   <li><b>Encoding errors:</b> String encoding fails (e.g., invalid UTF-8)</li>
 *   <li><b>Wire format errors:</b> Corrupted data or version mismatches during deserialization</li>
 * </ul>
 *
 * <h2>Handling Strategy</h2>
 *
 * <p>
 * Serialization failures are typically non-recoverable at the serialization layer. Callers should:
 * </p>
 * <ul>
 *   <li>Log the error with full stack trace for debugging</li>
 *   <li>Propagate to higher layers (e.g., as a Raft command failure)</li>
 *   <li>Consider the operation failed (do not retry at serialization level)</li>
 * </ul>
 *
 * @author José Bolina
 * @since 2.0
 * @see org.jgroups.raft.internal.serialization.binary.AbstractSerializationContextWrite
 * @see org.jgroups.raft.internal.serialization.binary.BinarySerializer
 * @see JRaftException
 */
public class JGroupsRaftSerializationException extends JRaftException {
    public JGroupsRaftSerializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
