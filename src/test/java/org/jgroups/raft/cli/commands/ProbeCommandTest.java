package org.jgroups.raft.cli.commands;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.jgroups.raft.cli.commands.ProbeCommandTest.TestProbeCommand.TEST_PROBE_REQUEST;
import static org.jgroups.raft.cli.exceptions.JGroupsRaftExceptionHandler.INVALID_ARGUMENT_CODE;

import org.jgroups.Global;
import org.jgroups.raft.cli.exceptions.JGroupsProbeException;
import org.jgroups.raft.cli.probe.ProbeArguments;
import org.jgroups.raft.cli.probe.ProbeRunner;
import org.jgroups.raft.internal.probe.RaftProtocolProbe;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import picocli.CommandLine;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class ProbeCommandTest {

    private static final Logger LOG = LogManager.getLogger(ProbeCommandTest.class);

    @Test
    public void testInvalidAddressThrowsProbeException() {
        TestProbeCommand cmd = new TestProbeCommand();
        class ThrowableAssertionWrapper {
            private Throwable t;

            public void assertThrownBy() throws Throwable {
                if (t == null)
                    throw new AssertionError("No throwable set");

                throw t;
            }
        }

        CommandLine cli = new CommandLine(cmd);

        // Fist test with invalid -addr
        ThrowableAssertionWrapper invalidAddress = new ThrowableAssertionWrapper();
        cli.setExecutionExceptionHandler((ex, ignore, res) -> {
            invalidAddress.t = ex;
            return 0;
        });

        // "invalid-host-name-xyz" will cause UnknownHostException, which BaseProbeCommand wraps in JGroupsProbeException.
        cli.execute("-addr", "invalid-host-name-xyz-999");
        assertThatThrownBy(invalidAddress::assertThrownBy)
                .isInstanceOf(JGroupsProbeException.class)
                .isInstanceOfSatisfying(JGroupsProbeException.class, jpe -> assertThat(jpe.exitCode()).isEqualTo(INVALID_ARGUMENT_CODE))
                .hasMessageContaining("Failed parsing target addresses");

        // Second test with invalid -bind-addr
        ThrowableAssertionWrapper invalidBind = new ThrowableAssertionWrapper();
        cli.setExecutionExceptionHandler((ex, ignore, res) -> {
            invalidBind.t = ex;
            return 0;
        });
        cli.execute("-bind-addr", "invalid-host-name-xyz-999");
        assertThatThrownBy(invalidBind::assertThrownBy)
                .isInstanceOf(JGroupsProbeException.class)
                .isInstanceOfSatisfying(JGroupsProbeException.class, jpe -> assertThat(jpe.exitCode()).isEqualTo(INVALID_ARGUMENT_CODE))
                .hasMessageContaining("Failed getting bind address");
    }

    @Test(dataProvider = "validArgumentsProvider")
    public void testArgumentsAreMappedCorrectly(String[] cliArgs, @SuppressWarnings("ClassEscapesDefinedScope") ExpectedState expected) {
        LOG.info("Running with seed {}", expected.seed());

        // Setup
        TestProbeCommand cmd = new TestProbeCommand();
        AtomicReference<ProbeArguments> capturedArgs = new AtomicReference<>();

        // Inject mock runner to capture the built arguments and avoid calling actual Probe.
        cmd.overrideProbeRunner((args, handler) -> capturedArgs.set(args));

        // Execute
        int exitCode = new CommandLine(cmd).execute(cliArgs);

        // Assertions
        assertThat(exitCode).as("Command should exit successfully").isEqualTo(0);

        ProbeArguments actual = capturedArgs.get();
        assertThat(actual).isNotNull();

        assertThat(actual.timeout())
                .as("Timeout should match input")
                .isEqualTo(expected.timeout);

        assertThat(actual.port())
                .as("Port should match input")
                .isEqualTo(expected.port);

        if (expected.tcp) {
            assertThat(actual.tcp()).as("TCP flag should be set").isTrue();
            assertThat(actual.udp()).as("UDP flag should be false").isFalse();
        } else {
            assertThat(actual.udp()).as("UDP should be default/set").isTrue();
        }

        if (expected.cluster != null) {
            assertThat(actual.request())
                    .as("Request string should contain cluster filter")
                    .isEqualTo(String.format("%s cluster=%s ", TEST_PROBE_REQUEST, expected.cluster));
        } else {
            assertThat(actual.request()).isEqualTo(TEST_PROBE_REQUEST);
        }


    }

    @Test
    public void testStatusProbeRequest() {
        Status cmd = new Status();
        AtomicReference<ProbeArguments> captured = new AtomicReference<>();
        cmd.overrideProbeRunner((args, h) -> captured.set(args));

        new CommandLine(cmd).execute();

        assertThat(captured.get()).isNotNull();
        assertThat(captured.get().request()).isEqualTo(RaftProtocolProbe.PROBE_RAFT_STATUS);
    }

    @Test
    public void testMetricsProbeRequest() {
        Metrics cmd = new Metrics();
        AtomicReference<ProbeArguments> captured = new AtomicReference<>();
        cmd.overrideProbeRunner((args, h) -> captured.set(args));

        new CommandLine(cmd).execute();

        assertThat(captured.get()).isNotNull();
        assertThat(captured.get().request()).isEqualTo(RaftProtocolProbe.PROBE_RAFT_METRICS);
    }

    @Test
    public void testMemberAddProbeRequest() {
        Member.Add cmd = new Member.Add();
        AtomicReference<ProbeArguments> captured = new AtomicReference<>();
        cmd.overrideProbeRunner((args, h) -> captured.set(args));

        new CommandLine(cmd).execute("nodeA", "--force");

        assertThat(captured.get()).isNotNull();
        assertThat(captured.get().request()).isEqualTo("RAFT.addServer[nodeA]");
    }

    @Test
    public void testMemberRemoveProbeRequest() {
        Member.Remove cmd = new Member.Remove();
        AtomicReference<ProbeArguments> captured = new AtomicReference<>();
        cmd.overrideProbeRunner((args, h) -> captured.set(args));

        new CommandLine(cmd).execute("nodeB", "--force");

        assertThat(captured.get()).isNotNull();
        assertThat(captured.get().request()).isEqualTo("RAFT.removeServer[nodeB]");
    }

    @Test
    public void testMemberListProbeRequest() {
        Member.List cmd = new Member.List();
        AtomicReference<ProbeArguments> captured = new AtomicReference<>();
        cmd.overrideProbeRunner((args, h) -> captured.set(args));

        new CommandLine(cmd).execute();

        assertThat(captured.get()).isNotNull();
        assertThat(captured.get().request()).isEqualTo("RAFT.members");
    }

    @Test
    public void testDefaultPortAndTimeout() {
        TestProbeCommand cmd = new TestProbeCommand();
        AtomicReference<ProbeArguments> captured = new AtomicReference<>();
        cmd.overrideProbeRunner((args, h) -> captured.set(args));

        new CommandLine(cmd).execute();

        assertThat(captured.get()).isNotNull();
        assertThat(captured.get().port())
                .as("Default port should be 7500")
                .isEqualTo(7500);
        assertThat(captured.get().timeout())
                .as("Default timeout should be 2000")
                .isEqualTo(2000);
    }

    @Test
    public void testUDPIsDefault() {
        TestProbeCommand cmd = new TestProbeCommand();
        AtomicReference<ProbeArguments> captured = new AtomicReference<>();
        cmd.overrideProbeRunner((args, h) -> captured.set(args));

        new CommandLine(cmd).execute();

        assertThat(captured.get()).isNotNull();
        assertThat(captured.get().udp())
                .as("UDP should be the default transport")
                .isTrue();
        assertThat(captured.get().tcp()).isFalse();
    }

    @Test
    public void testTCPFlagSetsTransport() {
        TestProbeCommand cmd = new TestProbeCommand();
        AtomicReference<ProbeArguments> captured = new AtomicReference<>();
        cmd.overrideProbeRunner((args, h) -> captured.set(args));

        new CommandLine(cmd).execute("-tcp");

        assertThat(captured.get()).isNotNull();
        assertThat(captured.get().tcp())
                .as("TCP flag should set TCP transport")
                .isTrue();
        assertThat(captured.get().udp()).isFalse();
    }

    @Test
    public void testVerboseFlagPropagated() {
        TestProbeCommand cmd = new TestProbeCommand();
        AtomicReference<ProbeArguments> captured = new AtomicReference<>();
        cmd.overrideProbeRunner((args, h) -> captured.set(args));

        new CommandLine(cmd).execute("-v");

        assertThat(captured.get()).isNotNull();
        assertThat(captured.get().verbose())
                .as("Verbose flag should be propagated to probe arguments")
                .isTrue();
    }

    @Test
    public void testClusterFilterAppendsToRequest() {
        TestProbeCommand cmd = new TestProbeCommand();
        AtomicReference<ProbeArguments> captured = new AtomicReference<>();
        cmd.overrideProbeRunner((args, h) -> captured.set(args));

        new CommandLine(cmd).execute("-cluster", "myCluster");

        assertThat(captured.get()).isNotNull();
        assertThat(captured.get().request())
                .as("Cluster filter should be appended to probe request")
                .isEqualTo(TEST_PROBE_REQUEST + " cluster=myCluster ");
    }

    @Test
    public void testBlankClusterFilterIgnored() {
        TestProbeCommand cmd = new TestProbeCommand();
        AtomicReference<ProbeArguments> captured = new AtomicReference<>();
        cmd.overrideProbeRunner((args, h) -> captured.set(args));

        new CommandLine(cmd).execute("-cluster", "  ");

        assertThat(captured.get()).isNotNull();
        assertThat(captured.get().request())
                .as("Blank cluster filter should be ignored")
                .isEqualTo(TEST_PROBE_REQUEST);
    }

    public static final class TestProbeCommand extends BaseProbeCommand {
        public static final String TEST_PROBE_REQUEST = "test-request";

        public TestProbeCommand() { }

        public TestProbeCommand(ProbeRunner runner) {
            overrideProbeRunner(runner);
        }

        @Override
        protected String probeRequest() {
            return TEST_PROBE_REQUEST;
        }
    }

    record ExpectedState(long seed, int port, long timeout, boolean tcp, String cluster) { }

    @DataProvider(name = "validArgumentsProvider")
    protected Object[][] generateRandomArguments() {
        List<Object[]> cases = new ArrayList<>();
        long seed = System.currentTimeMillis();
        Random random = new Random(seed);

        for (int i = 0; i < 20; i++) {
            List<String> args = new ArrayList<>();

            int port = 1000 + random.nextInt(50000);
            long timeout = 100 + random.nextInt(5000);
            boolean useTcp = random.nextBoolean();
            String cluster = random.nextBoolean() ? "cluster-" + i : null;

            // Build CLI String
            args.add("-port"); args.add(String.valueOf(port));
            args.add("-timeout"); args.add(String.valueOf(timeout));

            if (useTcp) args.add("-tcp");

            if (cluster != null) {
                args.add("-cluster");
                args.add(cluster);
            }

            cases.add(new Object[]{
                    args.toArray(new String[0]),
                    new ExpectedState(seed, port, timeout, useTcp, cluster)
            });
        }
        return cases.toArray(new Object[0][]);
    }
}
