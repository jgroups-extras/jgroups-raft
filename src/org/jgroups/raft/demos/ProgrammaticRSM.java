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
import org.jgroups.stack.NonReflectiveProbeHandler;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Demos {@link ReplicatedStateMachine}. Option -members defines the set of members (supposed to be static across
 * all nodes). Option -name needs to name one of the members.
 * @author Bela Ban
 * @since  1.0.0
 */
public class ProgrammaticRSM implements Receiver, RAFT.RoleChange {
    protected JChannel                              ch;
    protected ReplicatedStateMachine<String,Object> rsm;

    static {
        LogFactory.useJdkLogger(true);
    }

    protected void start(List<String> members, String name, boolean tcp,
                         InetAddress bind_addr, int bind_port, boolean follower, long timeout) throws Exception {
        ch=create(tcp, members, name, bind_addr, bind_port);
        NonReflectiveProbeHandler h=new NonReflectiveProbeHandler(ch);
        ch.getProtocolStack().getTransport().registerProbeHandler(h);
        h.initialize(ch.getProtocolStack().getProtocols());

        rsm=new ReplicatedStateMachine<String,Object>(ch).raftId(name).timeout(timeout);
        if(follower)
            disableElections(ch);
        ch.setReceiver(this);

        try {
            ch.connect("rsm");
            rsm.addRoleChangeListener(this);
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
            loop();
        }
        finally {
            Util.close(ch);
        }
    }

    protected static JChannel create(boolean tcp, List<String> members, String name,
                                     InetAddress bind_addr, int bind_port) throws Exception {
        List<Protocol> prots=new ArrayList<>();
        TP transport=tcp?
          new TCP().diagEnableUdp(false).diagEnableTcp(true)
          : new UDP().setMulticastAddress(InetAddress.getByName("228.8.8.8")).diagEnableUdp(true);
        transport.setThreadPoolMaxThreads(200)
          .setDiagnosticsAddr(InetAddress.getByName("224.0.75.75")).setDiagnosticsEnabled(true);
        if(bind_addr != null)
            transport.setBindAddr(bind_addr);
        if(bind_port> 0)
            transport.setBindPort(bind_port);
        prots.add(transport);

        // discovery protocols
        if(tcp) {
            prots.add(new MPING());
            if(System.getProperty("jgroups.dns.dns_query") != null)
                prots.add(new DNS_PING());
            prots.add(new TCPPING());
        }
        else
            prots.add(new PING());

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
          new RAFT().members(members).raftId(name),
          new REDIRECT(),
          new CLIENT()
        };
        prots.addAll(Arrays.asList(rest));
        return new JChannel(prots).name(name);
    }

    protected static void disableElections(JChannel ch) {
        ELECTION election=ch.getProtocolStack().findProtocol(ELECTION.class);
        if(election != null)
            election.noElections(true);
    }

    protected void loop() {
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

    protected void put(String key, String value) {
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

    protected void get(String key) {
        Object val=rsm.get(key);
        System.out.printf("-- get(%s) -> %s\n", key, val);
    }

    protected void remove(String key) {
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

    protected int firstApplied() {
        RAFT raft=rsm.channel().getProtocolStack().findProtocol(RAFT.class);
        return raft.log().firstAppended();
    }

    protected int logSize() {
        return rsm.logSize();
    }

    protected void dumpLog() {
        System.out.printf("\nindex (term): command\n---------------------\n%s\n",
                          rsm.dumpLog());
    }

    @Override
    public void viewAccepted(View view) {
        System.out.println("-- view change: " + view);
    }

    @Override
    public void roleChanged(Role role) {
        System.out.println("-- changed role to " + role);
    }

    public static void main(String[] args) throws Exception {
        String name=null;
        boolean follower=false;
        long timeout=3000;
        boolean tcp=true;
        InetAddress bind_addr=null;
        int bind_port=0;
        List<String> members=null;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-name")) {
                name=args[++i];
                continue;
            }
            if(args[i].equals("-follower")) {
                follower=true;
                continue;
            }
            if(args[i].equals("-timeout")) {
                timeout=Long.parseLong(args[++i]);
                continue;
            }
            if(args[i].equals("-tcp")) {
                tcp=Boolean.parseBoolean(args[++i]);
                continue;
            }
            if(args[i].equals("-bind_addr")) {
                bind_addr=Util.getAddress(args[++i], Util.getIpStackType());
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
                               "                       [-follower] [-timeout timeout] -tcp true|false]\n" +
                               "                       [-bind_addr addr] [-bind_port port]\n" +
                               " Example: -members A,B,C,D -name C");
            return;
        }

        if(members == null || members.isEmpty())
            throw new IllegalArgumentException("-members must be set");
        if(name == null)
            throw new IllegalArgumentException("-name must be set");

        new ProgrammaticRSM().start(members, name, tcp, bind_addr, bind_port, follower, timeout);
    }


}
