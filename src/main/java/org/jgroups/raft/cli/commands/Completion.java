package org.jgroups.raft.cli.commands;

import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

/**
 * Hidden command for autocompletion.
 * <p>
 * This is a hidden command that generates the autocompletion script utilizing Picocli. This command is suggested to the
 * user once to allow auto-completion of commands in the JGroups Raft CLI. It should be invoked like:
 * <pre>source (./bin/raft completion)</pre> utilizing the wrapper script.
 * </p>
 *
 * @author José Bolina
 * @since 2.0
 */
@Command(name = "completion", hidden = true, description = "Generates autocompletion script")
final class Completion implements Runnable {
    @Spec
    CommandSpec spec;

    @Override
    public void run() {
        String script = picocli.AutoComplete.bash("raft", spec.root().commandLine());
        System.out.print(script);
        System.out.println();
    }
}
