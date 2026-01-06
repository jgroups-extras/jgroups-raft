package org.jgroups.raft.cli.probe;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public record ProbeArguments(
        List<InetAddress> addresses,
        InetAddress bindAddress,
        int port,
        long timeout,
        String request,
        String passcode,
        boolean udp,
        boolean tcp,
        boolean verbose
) {

    public static ProbeArgumentsBuilder builder() {
        return new ProbeArgumentsBuilder();
    }

    public static final class ProbeArgumentsBuilder {
        private final List<InetAddress> addresses = new ArrayList<>();
        private InetAddress bindAddress;
        private int port;
        private long timeout;
        private String request;
        private String passcode;
        private boolean udp;
        private boolean tcp;
        private boolean verbose;

        private ProbeArgumentsBuilder() { }

        public ProbeArgumentsBuilder withAddress(InetAddress address) throws UnknownHostException {
            if (address != null)
                this.addresses.add(address);
            return this;
        }

        public ProbeArgumentsBuilder withBindAddress(InetAddress address) throws UnknownHostException {
            this.bindAddress = address;
            return this;
        }

        public ProbeArgumentsBuilder withPort(int port) {
            this.port = port;
            return this;
        }

        public ProbeArgumentsBuilder withTimeout(long timeout) {
            this.timeout = timeout;
            return this;
        }

        public ProbeArgumentsBuilder withRequest(String request) {
            this.request = request;
            return this;
        }

        public ProbeArgumentsBuilder withPasscode(String passcode) {
            this.passcode = passcode;
            return this;
        }

        public ProbeArgumentsBuilder withUDP() {
            this.udp = true;
            this.tcp = false;
            return this;
        }

        public ProbeArgumentsBuilder withTCP() {
            this.udp = false;
            this.tcp = true;
            return this;
        }

        public ProbeArgumentsBuilder withVerboseOutput() {
            this.verbose = true;
            return this;
        }

        public ProbeArguments build() {
            return new ProbeArguments(
                    addresses,
                    bindAddress,
                    port,
                    timeout,
                    Objects.requireNonNull(request, "Probe request is null"),
                    passcode,
                    udp,
                    tcp,
                    verbose
            );
        }
    }
}
