package org.jgroups.raft.cli.probe;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.jgroups.Global;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class ProbeArgumentsTest {

    public void testNullRequestThrowsNPE() {
        ProbeArguments.ProbeArgumentsBuilder builder = ProbeArguments.builder()
                .withPort(7500)
                .withTimeout(2000);

        assertThatThrownBy(builder::build)
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("Probe request is null");
    }

    public void testDefaultValues() {
        ProbeArguments args = ProbeArguments.builder()
                .withRequest("test")
                .build();

        assertThat(args.port()).isZero();
        assertThat(args.timeout()).isZero();
        assertThat(args.udp()).isFalse();
        assertThat(args.tcp()).isFalse();
        assertThat(args.verbose()).isFalse();
        assertThat(args.passcode()).isNull();
        assertThat(args.addresses()).isEmpty();
        assertThat(args.bindAddress()).isNull();
        assertThat(args.request()).isEqualTo("test");
    }

    public void testWithUDPClearsTCP() {
        ProbeArguments args = ProbeArguments.builder()
                .withRequest("test")
                .withTCP()
                .withUDP()
                .build();

        assertThat(args.udp()).isTrue();
        assertThat(args.tcp()).isFalse();
    }

    public void testWithTCPClearsUDP() {
        ProbeArguments args = ProbeArguments.builder()
                .withRequest("test")
                .withUDP()
                .withTCP()
                .build();

        assertThat(args.tcp()).isTrue();
        assertThat(args.udp()).isFalse();
    }

    public void testMultipleAddresses() throws UnknownHostException {
        InetAddress addr1 = InetAddress.getByName("127.0.0.1");
        InetAddress addr2 = InetAddress.getByName("127.0.0.2");
        InetAddress addr3 = InetAddress.getByName("127.0.0.3");

        ProbeArguments args = ProbeArguments.builder()
                .withRequest("test")
                .withAddress(addr1)
                .withAddress(addr2)
                .withAddress(addr3)
                .build();

        assertThat(args.addresses()).containsExactly(addr1, addr2, addr3);
    }

    public void testNullAddressIgnored() throws UnknownHostException {
        ProbeArguments args = ProbeArguments.builder()
                .withRequest("test")
                .withAddress(null)
                .build();

        assertThat(args.addresses()).isEmpty();
    }

    public void testCompleteBuilder() throws UnknownHostException {
        InetAddress target = InetAddress.getByName("127.0.0.1");
        InetAddress bind = InetAddress.getByName("127.0.0.2");

        ProbeArguments args = ProbeArguments.builder()
                .withAddress(target)
                .withBindAddress(bind)
                .withPort(9500)
                .withTimeout(5000)
                .withRequest("raft-status")
                .withPasscode("secret")
                .withUDP()
                .withVerboseOutput()
                .build();

        assertThat(args.addresses()).containsExactly(target);
        assertThat(args.bindAddress()).isEqualTo(bind);
        assertThat(args.port()).isEqualTo(9500);
        assertThat(args.timeout()).isEqualTo(5000);
        assertThat(args.request()).isEqualTo("raft-status");
        assertThat(args.passcode()).isEqualTo("secret");
        assertThat(args.udp()).isTrue();
        assertThat(args.tcp()).isFalse();
        assertThat(args.verbose()).isTrue();
    }
}
