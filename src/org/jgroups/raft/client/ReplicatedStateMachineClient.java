package org.jgroups.raft.client;

import org.jgroups.Address;
import org.jgroups.blocks.cs.Connection;
import org.jgroups.blocks.cs.ConnectionListener;
import org.jgroups.blocks.cs.Receiver;
import org.jgroups.blocks.cs.TcpClient;
import org.jgroups.util.ByteArrayDataInputStream;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.Util;

import java.io.DataInput;
import java.net.InetAddress;
import java.nio.ByteBuffer;

import static org.jgroups.raft.demos.ReplicatedStateMachineDemo.Command;

/**
 * Client connecting to a remote {@link org.jgroups.raft.demos.ReplicatedStateMachineDemo}.
 * @author Bela Ban
 * @since  1.0.0
 */
public class ReplicatedStateMachineClient implements Receiver, ConnectionListener {
    protected TcpClient        client;
    protected volatile boolean running=true, verbose=false;

    protected static final byte[] SHOW_ALL_CMD={(byte)Command.SHOW_ALL.ordinal()};
    protected static final byte[] DUMP_CMD={(byte)Command.DUMP_LOG.ordinal()};
    protected static final byte[] SNAPSHOT_CMD={(byte)Command.SNAPSHOT.ordinal()};
    protected static final byte[] GET_VIEW_CMD={(byte)Command.GET_VIEW.ordinal()};


    public ReplicatedStateMachineClient(boolean verbose) {
        this.verbose=verbose;
    }

    public void start(InetAddress host, int port) throws Exception {
        client=new TcpClient(null, 0, host, port);
        client.receiver(this);
        client.addConnectionListener(this);
        try {
            client.start();
            eventLoop();
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
        finally {
            Util.close(client);
        }
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
        Object obj=Util.objectFromStream(in);
        if(obj instanceof Exception)
            throw (Exception)obj;
        if(obj != null) {
            if(verbose)
                System.out.printf("-- result from %s: %s\n", sender, obj);
            else
                System.out.println(obj);
        }
    }


    public static void main(String[] args) throws Exception {
        InetAddress host=InetAddress.getLocalHost(); // host to connect to (running ReplicatedStateMachineDemo)
        int         port=2065;
        boolean     verbose=false;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-host")) {
                host=InetAddress.getByName(args[++i]);
                continue;
            }
            if(args[i].equals("-port")) {
                port=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-verbose")) {
                verbose=true;
                continue;
            }
            System.out.printf("\n%s [-host host] [-port port] [-verbose]\n\n",
                              ReplicatedStateMachineClient.class.getSimpleName());
            return;
        }
        ReplicatedStateMachineClient cl=new ReplicatedStateMachineClient(verbose);
        cl.start(host, port);
    }


    @Override
    public void connectionClosed(Connection conn) {
        client.stop();
        running=false;
        System.out.printf("connection to %s closed\n", conn.peerAddress());
    }

    @Override
    public void connectionEstablished(Connection conn) {

    }

    protected void eventLoop() throws Exception {
        while(running) {
            int input=Util.keyPress("[1] add [2] get [3] remove [4] show all [5] dump log [6] snapshot " +
                                      "[v] view [x] exit\n");
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
                    showAll();
                    break;
                case '5':
                    dumpLog();
                    break;
                case '6':
                    snapshot();
                    break;
                case 'v':
                    getView();
                    break;
                case 'x':
                    client.stop();
                    running=false;
                    break;
            }
        }
    }

    protected void put(String key, String value) throws Exception {
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream();
        out.writeByte(Command.PUT.ordinal());
        Util.objectToStream(key, out);
        Util.objectToStream(value, out);
        client.send(out.buffer(), 0, out.position());
    }

    protected void get(String key) throws Exception {
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream();
        out.writeByte(Command.GET.ordinal());
        Util.objectToStream(key, out);
        client.send(out.buffer(), 0, out.position());
    }

    protected void remove(String key) throws Exception {
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream();
        out.writeByte(Command.REMOVE.ordinal());
        Util.objectToStream(key, out);
        client.send(out.buffer(), 0, out.position());
    }

    protected void showAll() throws Exception {
        client.send(SHOW_ALL_CMD, 0, SHOW_ALL_CMD.length);
    }

    protected void dumpLog() throws Exception {
        client.send(DUMP_CMD, 0, DUMP_CMD.length);
    }

    protected void snapshot() throws Exception {
        client.send(SNAPSHOT_CMD, 0, SNAPSHOT_CMD.length);
    }

    protected void getView() throws Exception {
        client.send(GET_VIEW_CMD, 0, GET_VIEW_CMD.length);
    }

    protected static String read(String name) {
        try {
            return Util.readStringFromStdin(name + ": ");
        }
        catch(Exception e) {
            return null;
        }
    }

}
