package org.jgroups.raft.cli.commands.log.repair;

import org.jgroups.raft.cli.commands.log.ValidationResult;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import picocli.CommandLine;

/**
 * A self-describing, executable repair action identified from a {@link ValidationResult}.
 *
 * <p>
 * Each implementation carries the data it needs to present a repair plan to the operator and to execute the corresponding
 * file mutation. The consuming orchestrator iterates over the identified actions, calling {@link #describe},
 * {@link #confirmationPrompt}, {@link #execute}, and {@link #describeCompletion} in sequence for each action.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 */
public sealed interface RepairAction permits AdjustCommitIndex, AdjustCurrentTerm, ClearVotedFor, ReconstructMetadata, TruncateEntries {

    /**
     * Writes the repair plan and warnings to the operator.
     *
     * <p>
     * The output includes the proposed action, its consequences, and any risks the operator should consider before
     * confirming. Called before the confirmation prompt. All messages should be clear and concise.
     * </p>
     *
     * @param out  the writer for operator-facing output
     * @param ansi the ANSI color scheme for highlighted warnings
     */
    void describe(PrintWriter out, CommandLine.Help.Ansi ansi);

    /**
     * Returns the action-specific confirmation prompt shown to the operator.
     *
     * @return the prompt text, e.g. {@code "Proceed with repair?"}
     */
    String confirmationPrompt();

    /**
     * Performs the file mutation for this repair action.
     *
     * @param directory the directory containing the data files
     * @throws IOException if the file mutation fails
     */
    void execute(Path directory) throws IOException;

    /**
     * Writes a summary of the completed repair to the operator.
     *
     * <p>
     * Called after successful execution to confirm what was changed.
     * </p>
     *
     * @param out the writer for operator-facing output
     */
    void describeCompletion(PrintWriter out);

    /**
     * Maps a validation result to an ordered list of repair actions.
     *
     * @param result the validation result to inspect
     * @return an ordered list of repair actions, possibly empty if no manageable issues are found
     */
    static List<RepairAction> identify(ValidationResult result) {
        List<RepairAction> actions = new ArrayList<>();

        // First check if there was any corrupted entries identified in the entries log.
        Optional<ValidationResult.CorruptionPoint> corruptionOptional = result.firstCorruption();
        if (corruptionOptional.isPresent()) {
            // The idea is to collect at which index in the log that happened.
            // A Raft log doesn't have gaps, which means we need to drop everything start and after the corrupted entry.
            // Since the metadata file tracks at which point the node confirmed the commit, we need to update the metadata file.
            ValidationResult.CorruptionPoint cp = corruptionOptional.get();
            ValidationResult.LogInfo li = result.logInfo().orElse(null);

            long lastIntactIndex = EntryFileRepairUtil.resolveLastIntactIndex(cp, li);
            long metadataCommitIndex = result.metadataInfo().orElse(null) instanceof ValidationResult.MetadataInfo.Readable readable
                    ? readable.commitIndex()
                    : -1;
            boolean adjustCommit = lastIntactIndex >= 0 && metadataCommitIndex > lastIntactIndex;

            actions.add(new TruncateEntries(cp, li, lastIntactIndex, metadataCommitIndex, adjustCommit));
        }

        // Verify for issues in the metadata file.
        // The metadata file only holds extra information utilized by Raft.
        // It has a few well-defined entries for term, commit index, and a variable-length voted-for field.
        result.metadataInfo().ifPresent(mi -> {
            // The file was truncated before fully flushed to disk.
            // We are not able to parse it completely, so we'll utilize the entries information to rebuild the file.
            if (mi instanceof ValidationResult.MetadataInfo.Truncated) {
                long highestTerm = result.logInfo().map(ValidationResult.LogInfo::highestTerm).orElse(0L);
                actions.add(new ReconstructMetadata(highestTerm));
            } else if (mi instanceof ValidationResult.MetadataInfo.Readable readable) {
                if (readable.voteStatus() == ValidationResult.MetadataInfo.VoteStatus.CORRUPT) {
                    actions.add(new ClearVotedFor(readable.currentTerm()));
                }

                // Cross-checking the information in the entries file and the metadata file.
                // They need to be synchronized about the term and commit index.
                result.logInfo().ifPresent(li -> {
                    if (corruptionOptional.isEmpty() && readable.commitIndex() > li.lastIndex()) {
                        actions.add(new AdjustCommitIndex(readable.commitIndex(), li.lastIndex()));
                    }

                    if (readable.currentTerm() < li.highestTerm()) {
                        actions.add(new AdjustCurrentTerm(readable.currentTerm(), li.highestTerm()));
                    }
                });
            }
        });
        return Collections.unmodifiableList(actions);
    }
}
