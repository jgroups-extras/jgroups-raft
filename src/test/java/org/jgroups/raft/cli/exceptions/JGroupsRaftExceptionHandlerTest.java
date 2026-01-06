package org.jgroups.raft.cli.exceptions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jgroups.raft.cli.exceptions.JGroupsRaftExceptionHandler.DEFAULT_EXCEPTION_CODE;
import static org.jgroups.raft.cli.exceptions.JGroupsRaftExceptionHandler.INVALID_ARGUMENT_CODE;
import static org.jgroups.raft.cli.exceptions.JGroupsRaftExceptionHandler.PROBE_EXCEPTION_CODE;

import org.jgroups.Global;
import org.jgroups.raft.cli.commands.ProbeCommandTest;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.testng.annotations.Test;
import picocli.CommandLine;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class JGroupsRaftExceptionHandlerTest {

    public void testInvalidArgument() {
        ProbeCommandTest.TestProbeCommand cmd = new ProbeCommandTest.TestProbeCommand();
        CommandLine cli = new CommandLine(cmd);

        cli.setExecutionExceptionHandler(new JGroupsRaftExceptionHandler());

        int exitCode = cli.execute("-addr", "some-invalid-address");
        assertThat(exitCode).isEqualTo(INVALID_ARGUMENT_CODE);
    }

    public void testFailureInProbe() {
        ProbeCommandTest.TestProbeCommand cmd = new ProbeCommandTest.TestProbeCommand((args, h) -> {
            throw new RuntimeException("oops");
        });
        CommandLine cli = new CommandLine(cmd);

        cli.setExecutionExceptionHandler(new JGroupsRaftExceptionHandler());

        int exitCode = cli.execute();
        assertThat(exitCode).isEqualTo(PROBE_EXCEPTION_CODE);
    }

    public void testGeneralFailure() {
        @CommandLine.Command
        class Dummy implements Runnable {

            @Override
            public void run() {
                throw new RuntimeException("oops");
            }
        }

        CommandLine cli = new CommandLine(new Dummy());

        cli.setExecutionExceptionHandler(new JGroupsRaftExceptionHandler());
        int exitCode = cli.execute();
        assertThat(exitCode).isEqualTo(DEFAULT_EXCEPTION_CODE);
    }

    public void testJGroupsProbeExceptionExitCode() {
        ProbeCommandTest.TestProbeCommand cmd = new ProbeCommandTest.TestProbeCommand((args, h) -> {
            throw new JGroupsProbeException("connection refused", PROBE_EXCEPTION_CODE, null);
        });
        CommandLine cli = new CommandLine(cmd);
        cli.setExecutionExceptionHandler(new JGroupsRaftExceptionHandler());

        // The runner throws JGroupsProbeException directly, but BaseProbeCommand.executeInternal()
        // wraps it in another JGroupsProbeException with PROBE_EXCEPTION_CODE.
        int exitCode = cli.execute();
        assertThat(exitCode).isEqualTo(PROBE_EXCEPTION_CODE);
    }

    public void testErrorMessageContainsExceptionMessage() {
        StringWriter errOutput = new StringWriter();

        @CommandLine.Command
        class Dummy implements Runnable {
            @Override
            public void run() {
                throw new RuntimeException("connection refused");
            }
        }

        CommandLine cli = new CommandLine(new Dummy());
        cli.setErr(new PrintWriter(errOutput));
        cli.setExecutionExceptionHandler(new JGroupsRaftExceptionHandler());
        cli.execute();

        assertThat(errOutput.toString()).contains("connection refused");
    }

    public void testVerboseShowsStackTrace() {
        StringWriter errOutput = new StringWriter();

        ProbeCommandTest.TestProbeCommand cmd = new ProbeCommandTest.TestProbeCommand((args, h) -> {
            throw new RuntimeException("verbose-test-error");
        });

        CommandLine cli = new CommandLine(cmd);
        cli.setErr(new PrintWriter(errOutput));
        cli.setExecutionExceptionHandler(new JGroupsRaftExceptionHandler());
        cli.execute("-v");

        String output = errOutput.toString();
        assertThat(output)
                .as("Verbose mode should print the stack trace")
                .contains("RuntimeException");
    }

    public void testNonVerboseShowsHint() {
        StringWriter errOutput = new StringWriter();

        ProbeCommandTest.TestProbeCommand cmd = new ProbeCommandTest.TestProbeCommand((args, h) -> {
            throw new RuntimeException("hint-test-error");
        });

        CommandLine cli = new CommandLine(cmd);
        cli.setErr(new PrintWriter(errOutput));
        cli.setExecutionExceptionHandler(new JGroupsRaftExceptionHandler());
        cli.execute();

        String output = errOutput.toString();
        assertThat(output)
                .as("Non-verbose mode should show the verbose hint")
                .contains("--verbose");
    }
}
