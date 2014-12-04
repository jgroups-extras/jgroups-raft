package org.jgroups.demos;

import org.jgroups.JChannel;
import org.jgroups.blocks.raft.ReplicatedStateMachine;
import org.jgroups.util.Util;

/**
 * Demos {@link org.jgroups.blocks.raft.ReplicatedStateMachine}
 * @author Bela Ban
 * @since  0.1
 */
public class ReplicatedStateMachineDemo {
    protected JChannel ch;
    protected ReplicatedStateMachine<String,Object> rsm;

    protected void start(String props, String name) throws Exception {
        ch=new JChannel(props);
        if(name != null)
            ch.name(name);
        ch.connect("rsm");
        rsm=new ReplicatedStateMachine<>(ch);
        loop();
        Util.close(ch);
    }

    protected void loop() {
        boolean looping=true;
        while(looping) {
            int input=Util.keyPress("[1] add [2] get [3] remove [4] show all [x] exit");
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
            rsm.put(key,value);
        }
        catch(Throwable t) {
            System.err.println("failed setting " + key + "=" + value + ": " + t);
        }
    }

    protected void get(String key) {

    }

    protected void remove(String key) {

    }

    protected static String read(String name) {
        try {
            return Util.readStringFromStdin(name + ": ");
        }
        catch(Exception e) {
            return null;
        }
    }

    public static void main(String[] args) throws Exception {
        String props="raft.xml";
        String name=null;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-props")) {
                props=args[++i];
                continue;
            }
            if(args[i].equals("-name")) {
                name=args[++i];
                continue;
            }
            System.out.println("ReplicatedStateMachine [-props <config>] [-name <name>]");
            return;
        }
        new ReplicatedStateMachineDemo().start(props, name);
    }
}
