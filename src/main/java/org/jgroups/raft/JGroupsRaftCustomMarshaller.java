package org.jgroups.raft;

/**
 * Provide custom marshaller capabilities.
 *
 * <p>
 * Each marshaller implementation handles serialization for exactly one concrete class. Operations submitted through
 * {@link JGroupsRaft} require proper serialization to ensure data consistency across the cluster.
 * </p>
 *
 * <p>
 * <b>Compatibility considerations:</b>
 * <ul>
 * <li>ProtoStream is the recommended serialization approach for backwards compatibility.</li>
 * <li>This marshaller should only be used when ProtoStream is not feasible.</li>
 * <li>By utilizing a custom marshaller, you must always ensure backwards compatibility.</li>
 * </ul>
 * </p>
 *
 * <h2>Example</h2>
 *
 * <p>
 * Here is an example of a custom marshaller implementation to marshall a {@code int[][]} matrix using Java's built-in
 * {@link java.nio.ByteBuffer}:
 * <pre>{@code
 * new JGroupsRaftCustomMarshaller<int[][]>() {
 *     @Override
 *     public Class<? extends int[][]> javaClass() {
 *         return int[][].class;
 *     }
 *
 *     @Override
 *     public byte[] write(int[][] obj) {
 *         int size = Integer.BYTES * obj.length;
 *         for (int[] ints : obj) {
 *             size += Integer.BYTES + Integer.BYTES * ints.length;
 *         }
 *         byte[] datum = new byte[size];
 *         ByteBuffer buffer = ByteBuffer.wrap(datum);
 *         buffer.putInt(obj.length);
 *         for (int[] ints : obj) {
 *             buffer.putInt(ints.length);
 *             for (int i : ints) {
 *                 buffer.putInt(i);
 *             }
 *         }
 *         return datum;
 *     }
 *
 *     @Override
 *     public int[][] read(byte[] datum) {
 *         ByteBuffer buffer = ByteBuffer.wrap(datum);
 *         int outer = buffer.getInt();
 *         int[][] output = new int[outer][];
 *         for (int i = 0; i < outer; i++) {
 *             int inner = buffer.getInt();
 *             output[i] = new int[inner];
 *             for (int j = 0; j < inner; j++) {
 *                 output[i][j] = buffer.getInt();
 *             }
 *         }
 *         return output;
 *     }
 * }
 * }</pre>
 *
 * You can register the marshaller using the {@link JGroupsRaft.Builder#registerMarshaller(JGroupsRaftCustomMarshaller)}.
 * </p>
 *
 * <p>
 * <b>Thread safety:</b> Implementations must be thread-safe.
 * </p>
 *
 * @param <T> the type of object to be marshalled
 * @since 2.0
 * @author José Bolina
 * @see <a href="https://github.com/infinispan/protostream">ProtoStream</a>
 */
public interface JGroupsRaftCustomMarshaller<T> {

    /**
     * Returns the Java class that this marshaller handles.
     *
     * @return the Java class
     */
    Class<? extends T> javaClass();

    /**
     * Serializes the given object to a byte array.
     *
     * @param obj the object to serialize
     * @return the serialized byte array
     */
    byte[] write(T obj);

    /**
     * Deserializes the given byte array to an object.
     *
     * @param datum the byte array to deserialize
     * @return the deserialized object
     */
    T read(byte[] datum);
}
