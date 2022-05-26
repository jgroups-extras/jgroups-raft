package org.jgroups.raft.demos;

import org.jgroups.JChannel;
import org.jgroups.Receiver;
import org.jgroups.View;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.*;
import org.jgroups.protocols.dns.DNS_PING;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.protocols.raft.*;
import org.jgroups.raft.blocks.ReplicatedStateMachine;
import org.jgroups.stack.*;
import org.jgroups.util.Util;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.stream.Stream;

/**
 * Demos {@link ReplicatedStateMachine}. Option -members defines the set of members (supposed to be static across
 * all nodes). Option -name needs to name one of the members.
 * @author Bela Ban
 * @since  1.0.0
 */
public class ProgrammaticRSM {
    protected static final JChannel                              ch;
    protected static final ReplicatedStateMachine<String,Object> rsm;
    protected static final NonReflectiveProbeHandler             h;


    static {
        LogFactory.useJdkLogger(true);
        // prevents setting default values: GraalVM doesn't accept creation of InetAddresses at build time (in the
        // image), so we have to set the default valiues at run time
        Configurator.skipSettingDefaultValues(true);

        boolean use_udp=Boolean.getBoolean("use.udp");

        try {
            ch=create(use_udp);
            Configurator.skipSettingDefaultValues(false);
            h=new NonReflectiveProbeHandler(ch).initialize(ch.getProtocolStack().getProtocols());
            ch.setReceiver(new Receiver() {
                @Override public void viewAccepted(View view) {
                    System.out.println("-- view change: " + view);
                }
            });
            rsm=new ReplicatedStateMachine<>(ch);
        }
        catch(Exception ex) {
            throw new RuntimeException(ex);
        }
    }


    public static void main(String[] args) throws Exception {
        String       name=null;
        long         timeout=3000;
        String       bind_addr=null;
        int          bind_port=0;
        List<String> members=null;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-name")) {
                name=args[++i];
                continue;
            }
            if(args[i].equals("-timeout")) {
                timeout=Long.parseLong(args[++i]);
                continue;
            }
            if(args[i].equals("-bind_addr")) {
                bind_addr=args[++i];
                continue;
            }
            if(args[i].equals("-bind_port")) {
                bind_port=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-members")) {
                members=Util.parseCommaDelimitedStrings(args[++i]);
                continue;
            }
            System.out.println("ReplicatedStateMachine -members members -name name" +
                               "                       [-timeout timeout] -tcp true|false]\n" +
                               "                       [-bind_addr addr] [-bind_port port]\n" +
                               " Example: -members A,B,C,D -name C");
            return;
        }

        if(members == null || members.isEmpty())
            throw new IllegalArgumentException("-members must be set");
        if(name == null)
            throw new IllegalArgumentException("-name must be set");

        ch.setName(name);
        ProtocolStack stack=ch.getProtocolStack();
        TP transport=stack.getTransport();
        transport.registerProbeHandler(h);

        InetAddress ba=bind_addr == null? Util.getAddress("site_local", Util.getIpStackType())
          : InetAddress.getByName(bind_addr);
        InetAddress diag_addr=Util.getAddress("224.0.75.75", Util.getIpStackType()),
          mcast_addr=Util.getAddress("228.8.8.8", Util.getIpStackType()),
          mping_mcast=Util.getAddress("230.5.6.7", Util.getIpStackType());
        transport.setBindAddress(ba).setBindPort(bind_port).getDiagnosticsHandler().setMcastAddress(diag_addr);
        if(transport instanceof UDP)
            ((UDP)transport).setMulticastAddress(mcast_addr);

        Discovery discovery=stack.findProtocol(TCPPING.class);
        if(discovery != null)
            ((TCPPING)discovery).initialHosts(Collections.singletonList(new InetSocketAddress(ba, 7800)));
        discovery=stack.findProtocol(MPING.class);
        if(discovery != null)
            ((MPING)discovery).setMcastAddr(mping_mcast);

        RAFT raft=stack.findProtocol(RAFT.class);
        raft.members(members).raftId(name);

        rsm.raftId(name).timeout(timeout);

        try {
            ch.connect("rsm");
            DiagnosticsHandler diag_handler=transport.getDiagnosticsHandler();
            if(diag_handler != null) {
                Set<DiagnosticsHandler.ProbeHandler> probe_handlers=diag_handler.getProbeHandlers();
                probe_handlers.removeIf(probe_handler -> {
                    String[] keys=probe_handler.supportedKeys();
                    return keys != null && Stream.of(keys).anyMatch(s -> s.startsWith("jmx"));
                });
            }
            transport.registerProbeHandler(h);

            rsm.addNotificationListener(new ReplicatedStateMachine.Notification<>() {
                @Override
                public void put(String key, Object val, Object old_val) {
                    System.out.printf("-- put(%s, %s) -> %s\n", key, val, old_val);
                }

                @Override
                public void remove(String key, Object old_val) {
                    System.out.printf("-- remove(%s) -> %s\n", key, old_val);
                }
            });

            rsm.addRoleChangeListener(role -> System.out.println("-- changed role to " + role));

            loop();
        }
        finally {
            Util.close(ch);
        }
    }

    protected static JChannel create(boolean udp) throws Exception {
        List<Protocol> prots=new ArrayList<>();
        TP transport=udp? new UDP() : new TCP().setBindPort(7800);
        if(transport instanceof UDP)
            transport.getDiagnosticsHandler().enableUdp(true);
        else
            transport.getDiagnosticsHandler().enableUdp(false).enableTcp(true);
        transport.getThreadPool().setMaxThreads(200);
        transport.getDiagnosticsHandler().setEnabled(true);
        prots.add(transport);

        // discovery protocols
        if(udp)
            prots.add(new PING());
        else {
            prots.add(new MPING());
            if(System.getProperty("jgroups.dns.dns_query") != null)
                prots.add(new DNS_PING());
            prots.add(new TCPPING());
        }

        Protocol[] rest={
          new MERGE3().setMinInterval(10000).setMaxInterval(30000),
          new FD_SOCK(),
          new FD_ALL3().setTimeout(60000).setInterval(10000),
          new VERIFY_SUSPECT(),
          new NAKACK2(),
          new UNICAST3(),
          new STABLE(),
          new NO_DUPES(),
          new GMS().setJoinTimeout(2000),
          new UFC(),
          new MFC(),
          new FRAG4(),
          new ELECTION(),
          new RAFT(),
          new REDIRECT(),
          new CLIENT()
        };
        prots.addAll(Arrays.asList(rest));
        return new JChannel(prots);
    }


    protected static void loop() {
        boolean looping=true;
        while(looping) {
            int input=Util.keyPress("[1] add [2] get [3] remove [4] show all [5] dump log [6] snapshot [7] put N [x] exit\n" +
                                      "first-applied=" + firstApplied() +
                                      ", last-applied=" + rsm.lastApplied() +
                                      ", commit-index=" + rsm.commitIndex() +
                                      ", log size=" + Util.printBytes(logSize()) + "\n");
            switch(input) {
                case '1':
                    put(read("key"), read("value"));
                    break;
                case '2':
                    get(read("key"));
                    break;
                case '3':
                    remove(read("key"));
                    break;
                case '4':
                    System.out.println(rsm + "\n");
                    break;
                case '5':
                    dumpLog();
                    break;
                case '6':
                    try {
                        rsm.snapshot();
                    }
                    catch(Exception e) {
                        e.printStackTrace();
                    }
                    break;
                case '7':
                    try {
                        int num=Util.readIntFromStdin("num: ");
                        System.out.println();
                        String value="hello world #";
                        int print=num / 10;
                        long start=System.currentTimeMillis();
                        for(int i=1; i <= num; i++) {
                            put("key-" + i, value + i);
                            if(i > 0 && i % print == 0)
                                System.out.println("-- count=" + i);
                        }
                        long diff=System.currentTimeMillis() - start;
                        System.out.println("\n" + num + " puts took " + diff + " ms; " + (num / (diff / 1000.0)) + " ops /sec\n");
                    }
                    catch(Throwable ignored) {

                    }
                    break;
                case 'x':
                    looping=false;
                    break;
            }
        }
    }

    protected static void put(String key, String value) {
        if(key == null || value == null) {
            System.err.printf("Key (%s) or value (%s) is null\n",key,value);
            return;
        }
        try {
            rsm.put(key, value);
        }
        catch(Throwable t) {
            System.err.println("failed setting " + key + "=" + value + ": " + t);
        }
    }

    protected static void get(String key) {
        Object val=rsm.get(key);
        System.out.printf("-- get(%s) -> %s\n", key, val);
    }

    protected static void remove(String key) {
        try {
            rsm.remove(key);
        }
        catch(Exception ex) {
            System.err.println("failed removing " + key + ": " + ex);
        }
    }

    protected static String read(String name) {
        try {
            return Util.readStringFromStdin(name + ": ");
        }
        catch(Exception e) {
            return null;
        }
    }

    protected static long firstApplied() {
        RAFT raft=rsm.channel().getProtocolStack().findProtocol(RAFT.class);
        return raft.log().firstAppended();
    }

    protected static long logSize() {
        return rsm.logSize();
    }

    protected static void dumpLog() {
        System.out.printf("\nindex (term): command\n---------------------\n%s\n",
                          rsm.dumpLog());
    }

}
