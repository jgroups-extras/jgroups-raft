package org.jgroups.raft.cli.commands.log.repair;

import org.jgroups.raft.filelog.MetadataStorage;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import picocli.CommandLine;

/**
 * Clears the voted-for field in the metadata file, preserving the commit index and current term.
 *
 * <p>
 * Truncates the metadata file to the fixed-size prefix (commit index + current term), removing the variable-length
 * voted-for field. On restart, the node participates in elections with no prior vote recorded for the current term.
 * This should be safe under a stable topology with an already elected leader.
 * </p>
 *
 * @param currentTerm the current term from metadata, used in the double-vote warning
 */
record ClearVotedFor(long currentTerm) implements RepairAction {

    @Override
    public void describe(PrintWriter out, CommandLine.Help.Ansi ansi) {
        out.println("Repair action:");
        out.println("  Clear the vote record only. Commit index and term are preserved.");
        out.println();
        out.println(ansi.string("@|bold Warnings:|@"));
        out.println(ansi.string(
                "  @|yellow Clearing the vote record means this node may vote again in|@"));
        out.println(ansi.string(String.format(
                "  @|yellow term %d if an election is in progress. In rare cases, this|@", currentTerm)));
        out.println(ansi.string(
                "  @|yellow could allow two leaders in the same term. This risk is|@"));
        out.println(ansi.string(
                "  @|yellow minimal if other nodes are healthy.|@"));
    }

    @Override
    public String confirmationPrompt() {
        return "Proceed with clearing the vote record?";
    }

    @Override
    public void execute(Path directory) throws IOException {
        Path metadataPath = directory.resolve(MetadataStorage.FILE_NAME);
        try (FileChannel ch = FileChannel.open(metadataPath, StandardOpenOption.WRITE)) {
            ch.truncate(Long.BYTES + Long.BYTES);
        }
    }

    @Override
    public void describeCompletion(PrintWriter out) {
        out.println("Vote record cleared.");
    }
}
