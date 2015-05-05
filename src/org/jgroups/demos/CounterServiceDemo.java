package org.jgroups.demos;

import org.jgroups.JChannel;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.blocks.atomic.Counter;
import org.jgroups.blocks.raft.CounterService;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.util.Util;

/**
 * Demo of CounterService. When integrating with JGroups, this class should be removed and the original demo in JGroups
 * should be modified to accept different CounterService implementations.
 */
public class CounterServiceDemo {
    protected JChannel ch;
    protected CounterService counter_service;

    void start(String props, String name, long repl_timeout, boolean allow_dirty_reads) throws Exception {
        ch=new JChannel(props).name(name);
        ch.setReceiver(new ReceiverAdapter() {
            public void viewAccepted(View view) {
                System.out.println("-- view: " + view);
            }
        });
        RAFT raft=(RAFT)ch.getProtocolStack().findProtocol(RAFT.class);
        raft.raftId(name);
        loop(repl_timeout, allow_dirty_reads);
    }

    public void start(JChannel ch, long repl_timeout, boolean allow_dirty_reads) throws Exception {
       this.ch=ch;
        ch.setReceiver(new ReceiverAdapter() {
            public void viewAccepted(View view) {
                System.out.println("-- view: " + view);
            }
        });
        loop(repl_timeout, allow_dirty_reads);
    }


    void loop(long repl_timeout, boolean allow_dirty_reads) throws Exception {
        counter_service=new CounterService(ch);
        counter_service.replTimeout(repl_timeout).allowDirtyReads(allow_dirty_reads);
        ch.connect("cntrs");

        Counter counter=null;
        boolean looping=true;
        while(looping) {
            try {
                int key=Util.keyPress("[1] Increment [2] Decrement [3] Compare and set\n" +
                                        "[4] Create counter [5] Delete counter\n" +
                                        "[6] Print counters [7] Dump log\n" +
                                        "[8] Snapshot [9] Increment 1M times [x] Exit\n" +
                                        "first-applied=" + firstApplied() +
                                        ", last-applied=" + counter_service.lastApplied() +
                                        ", commit-index=" + counter_service.commitIndex() +
                                        ", log size=" + Util.printBytes(logSize()) + "\n");

                if(counter == null && key != 'x')
                    counter=counter_service.getOrCreateCounter("mycounter", 1);

                switch(key) {
                    case '1':
                        long val=counter.incrementAndGet();
                        System.out.printf("%s: %s\n", counter.getName(), val);
                        break;
                    case '2':
                        val=counter.decrementAndGet();
                        System.out.printf("%s: %s\n", counter.getName(), val);
                        break;
                    case '3':
                        long expect=Util.readLongFromStdin("expected value: ");
                        long update=Util.readLongFromStdin("update: ");
                        if(counter.compareAndSet(expect, update)) {
                            System.out.println("-- set counter \"" + counter.getName() + "\" to " + update + "\n");
                        }
                        else {
                            System.err.println("failed setting counter \"" + counter.getName() + "\" from " + expect +
                                                 " to " + update + ", current value is " + counter.get() + "\n");
                        }
                        break;
                    case '4':
                        String counter_name=Util.readStringFromStdin("counter name: ");
                        counter=counter_service.getOrCreateCounter(counter_name, 1);
                        break;
                    case '5':
                        counter_name=Util.readStringFromStdin("counter name: ");
                        counter_service.deleteCounter(counter_name);
                        counter=null;
                        break;
                    case '6':
                        System.out.println("Counters (current=" + counter.getName() + "):\n\n" + counter_service.printCounters());
                        break;
                    case '7':
                        dumpLog();
                        break;
                    case '8':
                        counter_service.snapshot();
                        break;
                    case '9':
                        int NUM=Util.readIntFromStdin("num: ");
                        System.out.println("");
                        int print=NUM / 10;
                        long retval=0;
                        long start=System.currentTimeMillis();
                        for(int i=0; i < NUM; i++) {
                            retval=counter.incrementAndGet();
                            if(i > 0 && i % print == 0)
                                System.out.println("-- count=" + retval);
                        }
                        long diff=System.currentTimeMillis() - start;
                        System.out.println("\n" + NUM + " incrs took " + diff + " ms; " + (NUM / (diff / 1000.0)) + " ops /sec\n");
                        break;
                    case 'x':
                        looping=false;
                        break;
                }
            }
            catch(Throwable t) {
                System.err.println(t.toString());
            }
        }
        Util.close(ch);
    }

    protected void dumpLog() {
        System.out.println("\nindex (term): command\n---------------------");
        counter_service.dumpLog();
        System.out.println("");
    }

    protected int firstApplied() {
        RAFT raft=(RAFT)ch.getProtocolStack().findProtocol(RAFT.class);
        return raft.log().firstApplied();
    }

    protected int logSize() {
        return counter_service.logSize();
    }



    public static void main(final String[] args) throws Exception {
        String properties="raft.xml";
        String name=null;
        long repl_timeout=5000;
        boolean allow_dirty_reads=true;
        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-props")) {
                properties=args[++i];
                continue;
            }
            if(args[i].equals("-name")) {
                name=args[++i];
                continue;
            }
            if(args[i].equals("repl_timeout")) {
                repl_timeout=Long.parseLong(args[++i]);
                continue;
            }
            if(args[i].equals("-allow_dirty_reads")) {
                allow_dirty_reads=Boolean.parseBoolean(args[++i]);
                continue;
            }
            help();
            return;
        }


        new CounterServiceDemo().start(properties, name, repl_timeout, allow_dirty_reads);

    }

    private static void help() {
        System.out.println("CounterServiceDemo [-props props] [-name name] " +
                             "[-repl_timeout timeout] [-allow_dirty_reads true|false]");
    }


}
