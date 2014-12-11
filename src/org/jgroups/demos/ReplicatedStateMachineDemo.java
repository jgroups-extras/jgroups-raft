package org.jgroups.demos;

import org.jgroups.JChannel;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.blocks.raft.ReplicatedStateMachine;
import org.jgroups.protocols.raft.ELECTION;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.Role;
import org.jgroups.util.Util;

/**
 * Demos {@link org.jgroups.blocks.raft.ReplicatedStateMachine}
 * @author Bela Ban
 * @since  0.1
 */
public class ReplicatedStateMachineDemo extends ReceiverAdapter implements RAFT.RoleChange {
    protected JChannel ch;
    protected ReplicatedStateMachine<String,Object> rsm;

    protected void start(String props, String name, boolean follower) throws Exception {
        ch=new JChannel(props);
        if(name != null)
            ch.name(name);
        if(follower)
            disableElections(ch);
        ch.setReceiver(this);
        ch.connect("rsm");
        rsm=new ReplicatedStateMachine<>(ch);
        rsm.addRoleChangeListener(this);
        loop();
        Util.close(ch);
    }

    protected static void disableElections(JChannel ch) {
        ELECTION election=(ELECTION)ch.getProtocolStack().findProtocol(ELECTION.class);
        if(election != null)
            election.setValue("no_elections", true);
    }

    protected void loop() {
        boolean looping=true;
        while(looping) {
            int input=Util.keyPress("[1] add [2] get [3] remove [4] show all [5] dump log [x] exit");
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
                    System.out.println(rsm);
                    break;
                case '5':
                    dumpLog();
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
            Object old_val=rsm.put(key, value);
            System.out.printf("-- put(%s,%s) -> %s\n", key, value, old_val);
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
            Object val=rsm.remove(key);
            System.out.printf("-- remove(%s) -> %s\n", key, val);
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

    protected void dumpLog() {
        rsm.dumpLog();
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
        String props="raft.xml";
        String name=null;
        boolean follower=false;

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
            System.out.println("ReplicatedStateMachine [-props <config>] [-name <name>] [-follower]");
            return;
        }
        new ReplicatedStateMachineDemo().start(props, name, follower);
    }


}
