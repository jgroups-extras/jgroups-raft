package org.jgroups.raft.cli.commands;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

/**
 * A general-purpose command that submits raw probe queries directly to the cluster.
 *
 * <p>
 * This command acts as a pass-through to the JGroups diagnostic handler, allowing users to invoke probe keys that do
 * not have dedicated CLI commands (e.g., standard JGroups keys like {@code jmx}, {@code op}, or {@code props}).
 * </p>
 *
 * <p>
 * <b>Usage:</b>
 * <br>
 * It is recommended to use the {@code --} separator to ensure the request string is parsed correctly, especially if it
 * contains hyphens or spaces.
 *
 * <pre>
 * # Check JGroups properties
 * raft probe -- props
 *
 * # invoke a JMX dump (if supported)
 * raft probe -- jmx=UDP.oob,thread_pool
 * </pre>
 * </p>
 *
 * @author José Bolina
 * @since 2.0
 */
@Command(name = "probe", description = "Submits a raw probe command.")
final class RawProbeCommand extends BaseProbeCommand {

    @Parameters
    private String request;

    @Override
    protected String probeRequest() {
        return request;
    }

    /**
     * Executes the raw probe command.
     * <p>
     * This implementation overrides the default behavior to bypass standard formatting handlers,
     * allowing the raw response to be processed or output directly.
     */
    @Override
    protected void execute() {
        // Set null to bypass the formatter.
        // This will let
        handler(null);
        super.execute();
    }
}
