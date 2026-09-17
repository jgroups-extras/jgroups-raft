package org.jgroups.raft.cli.commands;

import static org.assertj.core.api.Assertions.assertThat;

import org.jgroups.Global;
import org.jgroups.raft.cli.probe.ProbeArguments;
import org.jgroups.raft.cli.probe.writer.CategoryFilterResponseWriter;
import org.jgroups.raft.cli.probe.writer.ProbeResponseWriterFormat;
import org.jgroups.raft.internal.probe.RaftProtocolProbe;

import java.util.concurrent.atomic.AtomicReference;

import org.testng.annotations.Test;
import picocli.CommandLine;

/**
 * Tests for the {@link Metrics} command, verifying category filtering behavior.
 *
 * @author José Bolina
 * @since 2.0
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class MetricsCommandTest {

    /**
     * Verifies that a valid category option wraps the handler with CategoryFilterResponseWriter.
     */
    @Test
    public void testValidCategoryOptionWrapsHandler() {
        Metrics cmd = new Metrics();
        AtomicReference<ProbeArguments> captured = new AtomicReference<>();
        cmd.overrideProbeRunner((args, h) -> captured.set(args));

        int exitCode = new CommandLine(cmd).execute("-category", "processing-latency");

        assertThat(exitCode)
                .as("Command should succeed with valid category")
                .isEqualTo(0);

        assertThat(captured.get())
                .as("Probe arguments should be captured")
                .isNotNull();

        assertThat(captured.get().request())
                .as("Probe request should still be raft-metrics (filtering is client-side)")
                .isEqualTo(RaftProtocolProbe.PROBE_RAFT_METRICS);

        // The key assertion: handler should be wrapped with CategoryFilterResponseWriter
        assertThat(cmd.handler())
                .as("Handler should be wrapped with CategoryFilterResponseWriter")
                .isInstanceOf(CategoryFilterResponseWriter.class);
    }

    /**
     * Verifies that an invalid category token is rejected by picocli with exit code 2.
     */
    @Test
    public void testInvalidCategoryTokenRejectedByPicocli() {
        Metrics cmd = new Metrics();

        CommandLine cli = new CommandLine(cmd);
        int result = cli.execute("-category", "invalid-category-name");

        assertThat(result)
                .as("Invalid category should result in picocli usage error (exit code 2)")
                .isEqualTo(CommandLine.ExitCode.USAGE); // 2
    }

    /**
     * Verifies that no category option results in unchanged behavior (no filtering).
     */
    @Test
    public void testNoCategoryOptionNoWrapping() {
        Metrics cmd = new Metrics();
        AtomicReference<ProbeArguments> captured = new AtomicReference<>();
        cmd.overrideProbeRunner((args, h) -> captured.set(args));

        int exitCode = new CommandLine(cmd).execute();

        assertThat(exitCode)
                .as("Command should succeed without category option")
                .isEqualTo(0);

        assertThat(captured.get())
                .as("Probe arguments should be captured")
                .isNotNull();

        // Handler should NOT be wrapped when no category specified
        assertThat(cmd.handler())
                .as("Handler should NOT be CategoryFilterResponseWriter when no category")
                .isNotInstanceOf(CategoryFilterResponseWriter.class);

        assertThat(cmd.handler().format())
                .as("Default format should be TABLE")
                .isEqualTo(ProbeResponseWriterFormat.TABLE);
    }

    /**
     * Verifies all six valid category values are accepted.
     */
    @Test
    public void testAllValidCategoriesAccepted() {
        String[] validCategories = {
                "election-metrics",
                "total-latency",
                "processing-latency",
                "election-latency",
                "redirect-latency",
                "log-metrics"
        };

        for (String category : validCategories) {
            Metrics cmd = new Metrics();
            AtomicReference<ProbeArguments> captured = new AtomicReference<>();
            cmd.overrideProbeRunner((args, h) -> captured.set(args));

            int exitCode = new CommandLine(cmd).execute("-category", category);

            assertThat(exitCode)
                    .as("Category '%s' should be accepted", category)
                    .isEqualTo(0);

            assertThat(cmd.handler())
                    .as("Handler should be wrapped for category '%s'", category)
                    .isInstanceOf(CategoryFilterResponseWriter.class);
        }
    }

    /**
     * Verifies that category filtering composes with watch mode.
     */
    @Test
    public void testCategoryComposesWithWatchMode() {
        Metrics cmd = new Metrics();

        // Just verify it parses - actual watch execution would loop infinitely
        CommandLine cli = new CommandLine(cmd);
        cli.parseArgs("-category", "processing-latency", "-w");

        assertThat(cli.getParseResult().hasMatchedOption("-category"))
                .as("Category option should be parsed")
                .isTrue();

        assertThat(cli.getParseResult().hasMatchedOption("-w"))
                .as("Watch option should be parsed")
                .isTrue();
    }
}
