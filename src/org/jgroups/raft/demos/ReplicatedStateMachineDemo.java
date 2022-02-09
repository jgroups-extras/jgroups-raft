package org.jgroups.raft.demos;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.blocks.cs.BaseServer;
import org.jgroups.blocks.cs.TcpServer;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.protocols.raft.ELECTION;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.Role;
import org.jgroups.raft.blocks.ReplicatedStateMachine;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.ByteArrayDataInputStream;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Demos {@link ReplicatedStateMachine}
 * @author Bela Ban
 * @since  0.1
 */
public class ReplicatedStateMachineDemo implements org.jgroups.blocks.cs.Receiver, RAFT.RoleChange {
    protected JChannel                              ch;
    protected ReplicatedStateMachine<String,Object> rsm;
    protected BaseServer                            server; // listens for commands from a remote client

    public enum Command {PUT, GET, REMOVE, SHOW_ALL, DUMP_LOG, SNAPSHOT, GET_VIEW}


    public void start(String props, String name, boolean follower, long timeout,
                      InetAddress bind_addr, int port, boolean listen, boolean nohup) throws Exception {
        ch=new JChannel(props).name(name);
        rsm=new ReplicatedStateMachine<String,Object>(ch).raftId(name).timeout(timeout);
        if(follower)
            disableElections(ch);
        ch.setReceiver(new org.jgroups.Receiver() {
            @Override public void viewAccepted(View view) {
                System.out.println("-- view change: " + view);
            }
        });

        ch.connect("rsm");
        Util.registerChannel(rsm.channel(), "rsm");
        rsm.addRoleChangeListener(this);
        rsm.addNotificationListener(new ReplicatedStateMachine.Notification<>() {
            @Override public void put(String key, Object val, Object old_val) {
                // System.out.printf("-- put(%s, %s) -> %s\n", key, val, old_val);
            }

            @Override public void remove(String key, Object old_val) {
                System.out.printf("-- remove(%s) -> %s\n", key, old_val);
            }
        });
        if(listen)
            start(bind_addr, port);
        if(!nohup)
            loop();
    }

    @Override
    public void receive(Address sender, byte[] buf, int offset, int length) {
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(buf, offset, length);
        try {
            receive(sender, in);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void receive(Address sender, ByteBuffer buf) {
        Util.bufferToArray(sender, buf, this);
    }

    @Override
    public void receive(Address sender, DataInput in) throws Exception {
        int ordinal=in.readByte();
        ReplicatedStateMachineDemo.Command cmd=Command.values()[ordinal];

        switch(cmd) {
            case PUT: // key (String) : value(String) -> Object
                String key=Util.objectFromStream(in), value=Util.objectFromStream(in);
                Object retval=put(key, value);
                sendResponse(sender, retval);
                break;
            case GET: // key (String) -> Object
                key=Util.objectFromStream(in);
                retval=get(key);
                sendResponse(sender, retval);
                break;

            case REMOVE:
                key=Util.objectFromStream(in);
                retval=remove(key);
                sendResponse(sender, retval);
                break;

            case SHOW_ALL:
                String result=rsm.toString();
                sendResponse(sender, result);
                break;

            case DUMP_LOG:
                result=dumpLog();
                sendResponse(sender, result);
                break;

            case SNAPSHOT:
                result=(String)snapshot();
                sendResponse(sender, result);
                break;

            case GET_VIEW:
                result=getView();
                sendResponse(sender, result);
                break;
        }

    }

    protected void start(InetAddress bind_addr, int port) throws Exception {
        server=new TcpServer(bind_addr, port).receiver(this);
        server.start();
        JmxConfigurator.register(server, Util.getMBeanServer(), "rsm:name=rsm");
        int local_port=server.localAddress() instanceof IpAddress? ((IpAddress)server.localAddress()).getPort(): 0;
        System.out.printf("\n-- %s listening at %s:%s\n\n", ReplicatedStateMachineDemo.class.getSimpleName(),
                          bind_addr != null? bind_addr : "0.0.0.0",  local_port);
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
                                      ", log size=" + logSize() + "\n");
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
                    System.out.println(dumpLog());
                    break;
                case '6':
                    snapshot();
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

    protected Object put(String key, String value) {
        try {
            return rsm.put(Objects.requireNonNull(key, "key must be non-null)"),
                           Objects.requireNonNull(value, "value must be non-null"));
        }
        catch(Throwable t) {
            String ret=String.format("failed setting %s=%s: %s", key, value, t);
            System.err.println(ret);
            return ret;
        }
    }

    protected Object get(String key) {
        Object val=rsm.get(key);
        System.out.printf("-- get(%s) -> %s\n", key, val);
        return val;
    }

    protected Object remove(String key) {
        try {
            return rsm.remove(key);
        }
        catch(Exception ex) {
            String err=String.format("failed removing %s: %s", key, ex);
            System.out.println(err);
            return err;
        }
    }

    protected Object snapshot() {
        try {
            rsm.snapshot();
            return "snapshot suceeded";
        }
        catch(Exception e) {
            return String.format("snapshot failed: %s", e);
        }
    }

    protected String getView() {
        return String.format("local address: %s\nview: %s", ch.getAddress(), ch.getView());
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

    protected String dumpLog() {
        return String.format("\nindex (term): command\n---------------------\n%s\n", rsm.dumpLog());
    }

    protected void sendResponse(Address target, Object rsp) throws Exception {
        byte[] buf=Util.objectToByteBuffer(rsp);
        server.send(target, buf, 0, buf.length);
    }

    @Override
    public void roleChanged(Role role) {
        System.out.println("-- changed role to " + role);
    }

    public static void main(String[] args) throws Exception {
        String      props="raft.xml";
        String      name=null;
        boolean     follower=false, listen=false, nohup=false;
        long        timeout=3000;
        InetAddress bind_addr=null;
        int         port=2065;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-props")) {
                props=args[++i];
                continue;
            }
            if(args[i].equals("-name")) {
                name=args[++i];
                continue;
            }
            if(args[i].equals("-follower")) {
                follower=true;
                continue;
            }
            if(args[i].equals("-listen")) {
                listen=true;
                continue;
            }
            if(args[i].equals("-nohup")) {
                nohup=true;
                continue;
            }
            if(args[i].equals("-timeout")) {
                timeout=Long.parseLong(args[++i]);
                continue;
            }
            if(args[i].equals("-bind_addr")) {
                bind_addr=InetAddress.getByName(args[++i]);
                continue;
            }
            if(args[i].equals("-port")) {
                port=Integer.parseInt(args[++i]);
                continue;
            }
            System.out.printf("\n%s [-props <config>] [-name <name>] [-follower] [-timeout timeout]\n" +
                                "                   [-bind_addr <bind address>] [-port <bind port>] [-nohup]\n\n",
                              ReplicatedStateMachineDemo.class.getSimpleName());
            return;
        }
        new ReplicatedStateMachineDemo().start(props, name, follower, timeout, bind_addr, port, listen, nohup);
    }


}
