package org.jgroups.raft.internal.registry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.jgroups.raft.testfwk.RaftTestUtils.eventually;

import org.jgroups.Global;
import org.jgroups.protocols.raft.FileBasedLog;
import org.jgroups.raft.JGroupsRaft;
import org.jgroups.raft.JGroupsRaftRole;
import org.jgroups.raft.JGroupsRaftStateMachine;
import org.jgroups.raft.StateMachineRead;
import org.jgroups.raft.StateMachineWrite;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Integration tests for schema evolution with a real RAFT cluster.
 *
 * <p>
 * These tests start a single-node RAFT cluster with {@link FileBasedLog}, stop it (preserving the log),
 * and restart with a different state machine to verify schema validation in the full startup flow.
 * </p>
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class SchemaEvolutionIntegrationTest {

    private Path logDir;

    @BeforeMethod
    public void setUp() throws IOException {
        logDir = Files.createTempDirectory("schema-integration-test");
    }

    @AfterMethod
    public void tearDown() throws IOException {
        if (logDir != null && logDir.toFile().exists()) {
            try (var walk = Files.walk(logDir)) {
                walk.sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            }
        }
    }

    public void testAddNewCommandOnRestart() throws Exception {
        JGroupsRaft<SingleCommandSM> raft1 = buildRaft(new SingleCommandSMImpl(), SingleCommandSM.class);
        raft1.start();
        assertThat(eventually(() -> raft1.role() == JGroupsRaftRole.LEADER, 10, TimeUnit.SECONDS)).isTrue();
        raft1.stop();

        JGroupsRaft<TwoCommandSM> raft2 = buildRaft(new TwoCommandSMImpl(), TwoCommandSM.class);
        raft2.start();
        assertThat(eventually(() -> raft2.role() == JGroupsRaftRole.LEADER, 10, TimeUnit.SECONDS)).isTrue();
        raft2.stop();
    }

    public void testIncompatibleChangeFailsOnRestart() throws Exception {
        JGroupsRaft<SingleCommandSM> raft1 = buildRaft(new SingleCommandSMImpl(), SingleCommandSM.class);
        raft1.start();
        assertThat(eventually(() -> raft1.role() == JGroupsRaftRole.LEADER, 10, TimeUnit.SECONDS)).isTrue();
        raft1.stop();

        JGroupsRaft<IncompatibleSM> raft2 = buildRaft(new IncompatibleSMImpl(), IncompatibleSM.class);
        assertThatThrownBy(raft2::start)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("parameter types changed");
    }

    public void testRemovedWithoutRetirementFailsOnRestart() throws Exception {
        JGroupsRaft<TwoCommandSM> raft1 = buildRaft(new TwoCommandSMImpl(), TwoCommandSM.class);
        raft1.start();
        assertThat(eventually(() -> raft1.role() == JGroupsRaftRole.LEADER, 10, TimeUnit.SECONDS)).isTrue();
        raft1.stop();

        JGroupsRaft<SingleCommandSM> raft2 = buildRaft(new SingleCommandSMImpl(), SingleCommandSM.class);
        assertThatThrownBy(raft2::start)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("retired");
    }

    public void testRetiredCommandPassesOnRestart() throws Exception {
        JGroupsRaft<TwoCommandSM> raft1 = buildRaft(new TwoCommandSMImpl(), TwoCommandSM.class);
        raft1.start();
        assertThat(eventually(() -> raft1.role() == JGroupsRaftRole.LEADER, 10, TimeUnit.SECONDS)).isTrue();
        raft1.stop();

        JGroupsRaft<RetiredReadSM> raft2 = buildRaft(new RetiredReadSMImpl(), RetiredReadSM.class);
        raft2.start();
        assertThat(eventually(() -> raft2.role() == JGroupsRaftRole.LEADER, 10, TimeUnit.SECONDS)).isTrue();
        raft2.stop();
    }

    private <T> JGroupsRaft<T> buildRaft(T sm, Class<T> api) {
        return JGroupsRaft.builder(sm, api)
                .withJGroupsConfig("test-raft.xml")
                .withClusterName("schema-evolution-" + System.nanoTime())
                .configureRaft()
                    .withRaftId("node")
                    .withMembers(Collections.singletonList("node"))
                    .withLogClass(FileBasedLog.class)
                    .withLogDirectory(logDir.toString())
                    .withUseFsync(false)
                    .and()
                .build();
    }

    @JGroupsRaftStateMachine
    interface SingleCommandSM {
        @StateMachineWrite(id = 1, version = 1)
        void put(String value);
    }

    static class SingleCommandSMImpl implements SingleCommandSM {
        @Override
        public void put(String value) { }
    }

    @JGroupsRaftStateMachine
    interface TwoCommandSM {
        @StateMachineWrite(id = 1, version = 1)
        void put(String value);

        @StateMachineRead(id = 2, version = 1)
        String get(String key);
    }

    static class TwoCommandSMImpl implements TwoCommandSM {
        @Override
        public void put(String value) { }

        @Override
        public String get(String key) { return key; }
    }

    @JGroupsRaftStateMachine
    interface IncompatibleSM {
        @StateMachineWrite(id = 1, version = 1)
        void put(Integer value);
    }

    static class IncompatibleSMImpl implements IncompatibleSM {
        @Override
        public void put(Integer value) { }
    }

    @JGroupsRaftStateMachine(
            retiredReads = @StateMachineRead(id = 2, version = 1)
    )
    interface RetiredReadSM {
        @StateMachineWrite(id = 1, version = 1)
        void put(String value);
    }

    static class RetiredReadSMImpl implements RetiredReadSM {
        @Override
        public void put(String value) { }
    }
}
