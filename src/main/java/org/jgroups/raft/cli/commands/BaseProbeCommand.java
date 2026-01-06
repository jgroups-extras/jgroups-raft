package org.jgroups.raft.cli.commands;

import static org.jgroups.raft.cli.exceptions.JGroupsRaftExceptionHandler.INVALID_ARGUMENT_CODE;
import static org.jgroups.raft.cli.exceptions.JGroupsRaftExceptionHandler.PROBE_EXCEPTION_CODE;

import org.jgroups.Global;
import org.jgroups.raft.cli.exceptions.JGroupsProbeException;
import org.jgroups.raft.cli.probe.ProbeArguments;
import org.jgroups.raft.cli.probe.ProbeResponseWriter;
import org.jgroups.raft.cli.probe.ProbeRunner;
import org.jgroups.raft.cli.probe.writer.ProbeResponseWriterFormat;
import org.jgroups.util.StackType;
import org.jgroups.util.Util;

import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Objects;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

/**
 * Abstract base class for all CLI commands that interact with the JGroups Probe protocol.
 *
 * <p>
 * This class encapsulates the boilerplate required to configure and execute a Probe request. It manages:
 *
 * <ul>
 *   <li>Network configuration (IPv4/IPv6, UDP/TCP transport).</li>
 *   <li>Target address parsing and binding.</li>
 *   <li>Timeouts and authentication (passcodes).</li>
 * <  li>Output formatting selection (via {@link ProbeResponseWriterFormat}).</li>
 * </ul>
 *
 * Subclasses need only implement {@link #probeRequest()} to define the specific probe key to send to the cluster.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 */
abstract class BaseProbeCommand extends BaseRaftCLICommand {

    private ProbeRunner runner = ProbeRunner.defaultProbeRunner();
    private ProbeResponseWriter handler;

    @Option(names = "-addr", defaultValue = Option.NULL_VALUE, description = "The node addresses to connect to. The option is repeatable.")
    private String[] addresses;

    @Option(names = "-bind-addr", defaultValue = Option.NULL_VALUE, description = "Describes the client address to bind to.")
    private String bindAddr;

    @Option(names = "-port", defaultValue = "7500", description = "The default diagnostic port the server exposes. (${DEFAULT-VALUE})")
    private int port;

    @Option(names = "-timeout", defaultValue = "2000", description = "The default SO_TIMEOUT utilized in the underlying socket in milliseconds. (${DEFAULT-VALUE})")
    private long timeout;

    @Option(names = "-cluster", defaultValue = Option.NULL_VALUE, description = "Match only requests of a specific JGroups cluster. (${DEFAULT-VALUE})")
    private String cluster;

    @Option(names = "-passcode", interactive = true, description = "Optionally defines a passcode when submitting probe requests. This command is interactive.")
    private String passcode;

    @Option(names = "--format", description = "Format the CLI response: ${COMPLETION-CANDIDATES}. (${DEFAULT-VALUE})", defaultValue = "TABLE")
    private ProbeResponseWriterFormat format;

    @ArgGroup(heading = "Transport Protocol%n")
    private final TransportOptions transport = new TransportOptions();

    @ArgGroup(heading = "Stack type%n")
    private final StackTypeOptions stack = new StackTypeOptions();

    private ProbeArguments arguments;

    private static final class StackTypeOptions {
        @Option(names = "-4", description = "Flag to utilize IPv4 stack. (${DEFAULT-VALUE})")
        private boolean ipv4Stack;

        @Option(names = "-6", description = "Flag to utilize IPv6 stack. (${DEFAULT-VALUE})")
        private boolean ipv6Stack;

        private StackType stack() {
            if (ipv6Stack)
                return StackType.IPv6;

            return Util.getIpStackType();
        }
    }

    private static final class TransportOptions {
        @Option(names = "-udp", defaultValue = "true", description = "Flag to utilize UDP to connect to the server. (${DEFAULT-VALUE})")
        private boolean udp;

        @Option(names = "-tcp", defaultValue = "false", description = "Flag to utilize TCP to connect to the server. (${DEFAULT-VALUE})")
        private boolean tcp;

        public boolean isUDP() {
            return !tcp;
        }

        public boolean isTCP() {
            return !isUDP();
        }
    }

    /**
     * Orchestrates the execution of the command.
     *
     * <p>
     * This method performs the following steps:
     * <ol>
     *   <li>Determines the network stack (IPv4 vs IPv6).</li>
     *   <li>Parses target and bind addresses.</li>
     *   <li>Builds the {@link ProbeArguments} configuration object.</li>
     *   <li>Initializes or updates the {@link ProbeResponseWriter} based on the selected format.</li>
     *   <li>Delegates execution to {@link #execute()}.</li>
     * </ol>
     *
     * <p>
     * This configuration is completely base on {@link org.jgroups.tests.Probe#main(String[])} parsing.
     * </p>
     *
     * @throws JGroupsProbeException if address parsing fails.
     * @see org.jgroups.tests.Probe
     */
    @Override
    public final void run() {
        ProbeArguments.ProbeArgumentsBuilder builder = ProbeArguments.builder();
        StackType st = stack.stack();

        try {
            if (addresses != null) {
                for (String address : addresses) {
                    if (address != null && !address.isBlank()) {
                        builder.withAddress(parseAddress(address, st));
                    }
                }
            } else {
                InetAddress address;
                if (transport.isUDP()) {
                    address = InetAddress.getByName(st == StackType.IPv6 ? Global.DEFAULT_DIAG_ADDR_IPv6 : Global.DEFAULT_DIAG_ADDR);
                } else {
                    address = Util.getNonLoopbackAddress();
                }
                builder.withAddress(address);
            }
        } catch (UnknownHostException | SocketException e) {
            throw new JGroupsProbeException(String.format("Failed parsing target addresses: %s", e.getMessage()), INVALID_ARGUMENT_CODE, e);
        }

        try {
            InetAddress address = parseAddress(bindAddr, st);
            if (st == StackType.IPv6 && address == null)
                address = Util.getLoopback(st);
            builder.withBindAddress(address);
        } catch (UnknownHostException e) {
            throw new JGroupsProbeException(String.format("Failed getting bind address: %s", e.getMessage()), INVALID_ARGUMENT_CODE, e);
        }

        String request = probeRequest();
        if (cluster != null && !cluster.isBlank())
            request += String.format(" -cluster %s ", cluster);

        builder
                .withPort(port)
                .withTimeout(timeout)
                .withPasscode(passcode)
                .withRequest(request);

        if (isVerbose())
            builder.withVerboseOutput();

        if (handler == null) {
            handler = format.writer(out());
        } else if (handler.format() != format) {
            handler = format.writer(out());
        }

        if (transport.isUDP()) {
            builder.withUDP();
        } else {
            builder.withTCP();
        }

        arguments = builder.build();
        execute();
    }

    /**
     * Allows replacing the default {@link ProbeRunner} with a custom implementation.
     *
     * <p>
     * <b>Used for testing purposes.</b>
     * </p>
     *
     * @param runner The new probe runner instance.
     */
    void overrideProbeRunner(ProbeRunner runner) {
        this.runner = runner;
    }

    /**
     * Executes the configured probe request.
     * <p>
     * Subclasses can override this method if they need to perform actions before or after the probe execution, but
     * they must call {@code super.execute()} or {@link #executeInternal()} to actually run the probe.
     * </p>
     */
    protected void execute() {
        executeInternal();
    }

    /**
     * Internal execution logic that invokes the {@link ProbeRunner}.
     *
     * <p>
     * Wraps any runtime exceptions in {@link JGroupsProbeException} to be handled by
     * {@link org.jgroups.raft.cli.exceptions.JGroupsRaftExceptionHandler}.
     * </p>
     */
    private void executeInternal() {
        try {
            runner.run(Objects.requireNonNull(arguments, "Probe arguments are null"), handler);
        } catch (Throwable e) {
            throw new JGroupsProbeException(String.format("Failed running probe command: %s", e.getMessage()), PROBE_EXCEPTION_CODE, e);
        }
    }

    public final ProbeResponseWriter handler() {
        return handler;
    }

    public final void handler(ProbeResponseWriter handler) {
        this.handler = handler;
    }

    public final long timeout() {
        return timeout;
    }

    /**
     * Defines the specific Probe request key(s) to send to the server.
     *
     * <p>
     * Example return values: {@code "jgroups-raft"}, {@code "RAFT.members"}, {@code "jmx=java.lang:type=Memory"}.
     * </p>
     *
     * @return The probe query string.
     */
    protected abstract String probeRequest();

    private static InetAddress parseAddress(String addr, StackType st) throws UnknownHostException {
        if (addr == null) return null;
        return Util.getByName(addr, st);
    }
}
