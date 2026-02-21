package org.jgroups.raft.serialization;

import org.jgroups.raft.JGroupsRaft;
import org.jgroups.raft.internal.serialization.SingleBinarySerializer;

/**
 * Custom serializer for user-defined types in JGroups Raft.
 *
 * <p>
 * Each serializer implementation handles serialization and deserialization for exactly one concrete class. All operations
 * submitted through {@link JGroupsRaft} require proper serialization to ensure data consistency across the cluster.
 * </p>
 *
 * <h2>Basic Usage</h2>
 *
 * <p>
 * To serialize a custom type, implement this interface and register it when building your {@link JGroupsRaft} instance:
 * </p>
 *
 * <pre>{@code
 * public class Person {
 *     private final String name;
 *     private final int age;
 *
 *     public Person(String name, int age) {
 *         this.name = name;
 *         this.age = age;
 *     }
 *
 *     // getters...
 * }
 *
 * public class PersonMarshaller implements JGroupsRaftCustomMarshaller<Person> {
 *
 *     @Override
 *     public void write(SerializationContextWrite ctx, Person person) {
 *         ctx.writeUTF(person.getName());
 *         ctx.writeInt(person.getAge());
 *     }
 *
 *     @Override
 *     public Person read(SerializationContextRead ctx, byte version) {
 *         String name = ctx.readUTF();
 *         int age = ctx.readInt();
 *         return new Person(name, age);
 *     }
 *
 *     @Override
 *     public Class<Person> javaClass() {
 *         return Person.class;
 *     }
 *
 *     @Override
 *     public int type() {
 *         return 1001;  // Choose a unique type ID
 *     }
 *
 *     @Override
 *     public byte version() {
 *         return 1;  // Current version of this serializer
 *     }
 * }
 *
 * // Register the serializer:
 * JGroupsRaft<MyStateMachine> raft = JGroupsRaft.builder(stateMachine, MyStateMachine.class)
 *     .registerMarshaller(new PersonMarshaller())
 *     .build();
 * }</pre>
 *
 * <h2>Type IDs</h2>
 *
 * <p>
 * Each serializer must return a unique type ID via {@link #type()}. The type ID is utilized to uniquely identify the
 * entry in the byte stream.
 * </p>
 *
 * <p>
 * <b>Important guidelines for type IDs:</b>
 * </p>
 * <ul>
 *   <li><b>Choose IDs starting from 1000</b> - IDs below 1000 are reserved for internal use, an exception will be thrown.</li>
 *   <li><b>Never reuse a type ID</b> - Once a type ID is used in production, it must remain permanently associated with that type.</li>
 *   <li><b>Never change a type ID</b> - Changing would break compatibility with existing persisted data</li>
 *   <li><b>Document your type IDs</b> - Maintain a documentation of type IDs in your codebase to avoid unexpected changes.</li>
 * </ul>
 *
 * <pre>{@code
 * // Good practice: Document your type IDs
 * public class TypeIds {
 *     public static final int PERSON = 1001;
 *     public static final int ADDRESS = 1002;
 *     public static final int ORDER = 1003;
 *     // ... add new IDs as needed
 * }
 * }</pre>
 *
 * <h2>Versioning and Evolution</h2>
 *
 * <p>
 * The {@link #version()} method returns the current version of your serializer's wire format. Increment this version when
 * you need to change the serialization format while maintaining backward compatibility. The version byte is passed to
 * {@link #read(SerializationContextRead, byte)}, allowing you to handle multiple format versions during deserialization:
 * </p>
 *
 * <pre>{@code
 * public class PersonMarshaller implements JGroupsRaftCustomMarshaller<Person> {
 *
 *     @Override
 *     public void write(SerializationContextWrite ctx, Person person) {
 *         // Version 2 format: added email field
 *         ctx.writeUTF(person.getName());
 *         ctx.writeInt(person.getAge());
 *         ctx.writeUTF(person.getEmail());  // NEW in version 2
 *     }
 *
 *     @Override
 *     public Person read(SerializationContextRead ctx, byte version) {
 *         String name = ctx.readUTF();
 *         int age = ctx.readInt();
 *
 *         // Handle both version 1 and version 2
 *         String email = version >= 2 ? ctx.readUTF() : "unknown@example.com";
 *
 *         return new Person(name, age, email);
 *     }
 *
 *     @Override
 *     public byte version() {
 *         return 2;  // Incremented from 1
 *     }
 *
 *     // ... other methods
 * }
 * }</pre>
 *
 * <h2>Forward Compatibility</h2>
 *
 * <p>
 * The framework automatically handles forward compatibility (old code reading new data). When an old serializer encounters
 * data from a newer version, it reads what it understands and the framework automatically skips unknown trailing fields.
 * </p>
 *
 * <p>
 * <b>This works automatically - you don't need to do anything special.</b> Just make sure to increment {@link #version()}
 * when adding new fields.
 * </p>
 *
 * <h2>Nested Objects</h2>
 *
 * <p>
 * You can serialize nested objects using {@link SerializationContextWrite#writeObject(Object)} and
 * {@link SerializationContextRead#readObject()}. The nested object's serializer will be automatically invoked:
 * </p>
 *
 * <pre>{@code
 * public class Order {
 *     private final Person customer;
 *     private final List<String> items;
 *     private final double total;
 * }
 *
 * public class OrderMarshaller implements JGroupsRaftCustomMarshaller<Order> {
 *
 *     @Override
 *     public void write(SerializationContextWrite ctx, Order order) {
 *         ctx.writeObject(order.getCustomer());  // Delegates to PersonMarshaller
 *         ctx.writeInt(order.getItems().size());
 *         for (String item : order.getItems()) {
 *             ctx.writeUTF(item);
 *         }
 *         ctx.writeDouble(order.getTotal());
 *     }
 *
 *     @Override
 *     public Order read(SerializationContextRead ctx, byte version) {
 *         Person customer = ctx.readObject();  // Delegates to PersonMarshaller
 *         int itemCount = ctx.readInt();
 *         List<String> items = new ArrayList<>(itemCount);
 *         for (int i = 0; i < itemCount; i++) {
 *             items.add(ctx.readUTF());
 *         }
 *         double total = ctx.readDouble();
 *         return new Order(customer, items, total);
 *     }
 *
 *     @Override
 *     public int type() {
 *         return 1003;
 *     }
 *
 *     @Override
 *     public byte version() {
 *         return 1;
 *     }
 *
 *     @Override
 *     public Class<Order> javaClass() {
 *         return Order.class;
 *     }
 * }
 * }</pre>
 *
 * <h2>Null Handling</h2>
 *
 * <p>
 * The {@link #write(SerializationContextWrite, Object)} method accept null values - the framework handles null values
 * automatically. Similarly, you should never return null from {@link #read(SerializationContextRead, byte)}.
 * </p>
 *
 * <p>
 * For nullable fields within your object, use {@link SerializationContextWrite#writeObject(Object)} which handles null
 * correctly:
 * </p>
 *
 * <pre>{@code
 * @Override
 * public void write(SerializationContextWrite ctx, Person person) {
 *     ctx.writeUTF(person.getName());
 *     ctx.writeObject(person.getAddress());  // Can be null - handled automatically
 * }
 *
 * @Override
 * public Person read(SerializationContextRead ctx, byte version) {
 *     String name = ctx.readUTF();
 *     Address address = ctx.readObject();  // May return null
 *     return new Person(name, address);
 * }
 * }</pre>
 *
 * <h2>Backward Compatibility Considerations</h2>
 *
 * <ul>
 *   <li><b>Never remove fields</b> - Old data may still reference them</li>
 *   <li><b>Never change field types</b> - The wire format is tied to Java types</li>
 *   <li><b>Never reorder fields</b> - Read order must match write order</li>
 *   <li><b>Always increment version</b> when adding fields - Allows old deserializers to detect new formats</li>
 *   <li><b>Provide defaults</b> for new fields when reading old versions</li>
 * </ul>
 *
 * <p>
 * <b>Warning: </b> The framework does <b>NOT</b> validate these changes in the object schema. It is your responsibility
 * to maintain the contract with the API.
 * </p>
 *
 * <h2>Performance Considerations</h2>
 *
 * <p>
 * Serializers should be stateless and lightweight. The same serializer instance may be used concurrently by multiple threads, so:
 * </p>
 *
 * <ul>
 *   <li>Don't store mutable state in instance fields</li>
 *   <li>Make serializers thread-safe (stateless implementations are inherently thread-safe)</li>
 *   <li>Avoid creating unnecessary objects during serialization</li>
 *   <li>Use primitive write methods (writeInt, writeLong, etc.) instead of boxing</li>
 * </ul>
 *
 * <p>
 * And always remember that serialization is in the hot-path of the algorithm. A slow serialization will harm the throughput
 * in the application level.
 * </p>
 *
 * <h2>Registration</h2>
 *
 * <p>
 * Register your serializers when building the {@link JGroupsRaft} instance:
 * </p>
 *
 * <pre>{@code
 * JGroupsRaft<MyStateMachine> raft = JGroupsRaft.builder(stateMachine, MyStateMachine.class)
 *     .registerMarshaller(new PersonMarshaller())
 *     .registerMarshaller(new AddressMarshaller())
 *     .registerMarshaller(new OrderMarshaller())
 *     .build();
 * }</pre>
 *
 * <p>
 * <b>Important:</b> All cluster members must register the same serializers with the same type IDs. Failure to do so will
 * result in deserialization errors.
 * </p>
 *
 * <h2>Thread Safety</h2>
 *
 * <p>
 * Serializer implementations must be thread-safe. Multiple threads may invoke {@link #write} and {@link #read} concurrently
 * on the same serializer instance.
 * </p>
 *
 * @param <T> The type of object this serializer handles
 * @since 2.0
 * @author José Bolina
 * @see SerializationContextWrite
 * @see SerializationContextRead
 * @see JGroupsRaft.Builder#registerMarshaller(JGroupsRaftCustomMarshaller)
 */
public interface JGroupsRaftCustomMarshaller<T> extends SingleBinarySerializer<T> {
    int MINIMUM_TYPE_ID = 1000;
}
