package org.jgroups.raft;

import org.jgroups.protocols.raft.InMemoryLog;
import org.jgroups.raft.command.JGroupsRaftReadCommandOptions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.Proto;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.protostream.annotations.ProtoSyntax;

public class ReplicatedHashMapExample {

    public static void main(String[] args) throws Exception {
        // First, creates a new instance of the state machine.
        // The new API revolves around the state machine type and commands.
        ReplicatedHashMap stateMachine = new ReplicatedHashMap();

        // Now, creates an instance of JGroupsRaft to use the new API.
        // To start configuration, we use the builder pattern, which requires the state machine instance.
        JGroupsRaft<StateMachineApi> raft = JGroupsRaft.builder(stateMachine, StateMachineApi.class)
                // The JGroups configuration file to use when creating the JChannel instance.
                .withJGroupsConfig("test-raft.xml")

                // The name of the cluster the nodes we'll connect to.
                .withClusterName("replicated-hash-map")

                // Register the serialization context so ProtoStream can serialize the objects.
                .registerSerializationContextInitializer(new ReplicatedHashMapSerializationContextImpl())

                // We use a custom configuration to set the raft ID and members.
                .configureRaft()
                    .withRaftId("example")
                    .withMembers(Collections.singletonList("example"))
                    .withLogClass(InMemoryLog.class)
                    .and()

                // And finally, we build the JGroupsRaft instance.
                .build();

        // The first step is to start the JGroupsRaft instance.
        // This will trigger the bootstrap of all classes, starting and connecting the JChannel, and extra validations.
        raft.start();

        // We'll wait until the single node becomes the leader.
        while (raft.role() != JGroupsRaftRole.LEADER) {
            System.out.println("Waiting until become leader...");
            Thread.sleep(500);
        }

        // Now, we can start using the state machine.
        // We submit an operation to retrieve the user information for a given ID.
        String userId = "1234";
        UserInformation ui = raft.read((Function<StateMachineApi, UserInformation>) rhm -> rhm.get(userId));
        System.out.printf("User %s is currently: %s%n", userId, ui);

        // Uses the read-only client.
        StateMachineApi readOnlyApi = raft.readOnly(JGroupsRaftReadCommandOptions.options().linearizable(false).build());
        readOnlyApi.get(userId);

        // Now, we can update the user information for the given ID.
        final UserInformation value = new UserInformation(UUID.randomUUID().toString(), ThreadLocalRandom.current().nextInt());
        System.out.printf("Updating user %s to key %s%n", ui, userId);
        raft.read(api -> api.get(userId), JGroupsRaftReadCommandOptions.options().linearizable(false).build());
        raft.write(rhm -> {
           rhm.put(userId, value);
        });

        // Now, we can retrieve the user information again to verify the update.
        //ui = raft.submit(GET_USER_INFO_REQUEST, userId, 10, TimeUnit.SECONDS);
        ui = raft.read((Function<StateMachineApi, UserInformation>) rhm -> rhm.get(userId));
        System.out.println("After update: " + ui);

        // After submitting all operations, stop the raft instance.
        raft.stop();
    }

    /**
     * ReplicatedHashMap is the state machine that stores the user information.
     *
     * <p>
     * The example shows how to use the new API to create a state machine and register the commands. We use the
     * annotations to define the commands and their versions. We need to distinguish whether it is a read or write method.
     * </p>
     *
     * <p>
     * This approach avoids all manual serialization and deserialization of the objects. The state machine will receive
     * only the input and output types defined in the commands. We only need to implement the logic for the state machine
     * to handle each command.
     * </p>
     */
    @JGroupsRaftStateMachine
    public interface StateMachineApi {
        @StateMachineRead(id = 1)
        UserInformation get(String key);

        @StateMachineWrite(id = 2)
        void put(String key, UserInformation value);
    }

    /**
     * Concrete implementation of the state machine.
     * <p>
     * This implementation does not need to be thread-safe, the library will take care of that. The implementation
     * must guarantee determinism when handling commands.
     * </p>
     */
    public static final class ReplicatedHashMap implements StateMachineApi {
        /**
         * We can utilize a simple hash map, no need for concurrent.
         */
        @StateMachineField(order = 0)
        Map<String, UserInformation> data = new HashMap<>();

        @Override
        public UserInformation get(String key) {
            return data.get(key);
        }

        @Override
        public void put(String key, UserInformation value) {
            data.put(key, value);
        }
    }

    /**
     * Represents some generic user information to be stored in the state machine.
     *
     * <p>
     * This example shows how to use ProtoStream to serialize the objects to store in the state machine.
     * </p>
     *
     * @param name The user's name
     * @param age The user's age.
     */
    @Proto
    public record UserInformation(String name, int age) {}

    /**
     * Creates the serialization context for ProtoStream.
     */
    @ProtoSchema(
            allowNullFields = true,
            includeClasses = {
                    ReplicatedHashMapExample.UserInformation.class,
            },
            schemaFileName = "replicated_hash_map.proto",
            schemaFilePath = "proto/generated",
            schemaPackageName = "org.jgroups.tests.examples.hash",
            service = false,
            syntax = ProtoSyntax.PROTO3
    )
    public interface ReplicatedHashMapSerializationContext extends SerializationContextInitializer { }
}
