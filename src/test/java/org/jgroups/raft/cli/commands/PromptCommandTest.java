package org.jgroups.raft.cli.commands;

import static org.assertj.core.api.Assertions.assertThat;

import org.jgroups.Global;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.testng.annotations.Test;
import picocli.CommandLine;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class PromptCommandTest {

    public void testUserConfirm() {
        performTest("y", true);
        performTest("Y", true);
        performTest("yes", true);
    }

    public void testUserForces() {
        String raftId = UUID.randomUUID().toString();
        Member.Add cmd = new Member.Add();
        AtomicBoolean executed = new AtomicBoolean(false);
        cmd.overrideProbeRunner((args, h) -> {
            assertThat(args.request()).contains(raftId);
            executed.set(true);
        });

        CommandLine cli = new CommandLine(cmd);
        cli.execute(raftId, "--force");

        assertThat(executed).isTrue();
    }

    public void testUserCancels() {
       performTest("n", false);
       performTest("N", false);
    }

    public void testUserWritesGarbage() {
        performTest("garbage", false);
    }

    public void testUserPromptDefault() {
        // An empty response, the user confirm without typing.
        performTest("", false);
    }

    private void performTest(String userInput, boolean shouldExecute) {
        InputStream originalIn = System.in;
        System.setIn(new ByteArrayInputStream(String.format("%s%s", userInput, System.lineSeparator()).getBytes(StandardCharsets.UTF_8)));

        try {
            String raftId = UUID.randomUUID().toString();
            Member.Add cmd = new Member.Add();
            AtomicBoolean executed = new AtomicBoolean(false);
            cmd.overrideProbeRunner((args, h) -> {
                assertThat(args.request()).contains(raftId);
                executed.set(true);
            });

            CommandLine cli = new CommandLine(cmd);
            cli.execute(raftId);

            assertThat(executed.get()).isEqualTo(shouldExecute);
        } finally {
            System.setIn(originalIn);
        }
    }
}
