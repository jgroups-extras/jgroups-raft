package org.jgroups.raft.client;

import org.jgroups.protocols.raft.CLIENT;
import org.jgroups.util.Util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetAddress;
import java.net.Socket;

/**
 * Client which accesses the {@link org.jgroups.protocols.raft.CLIENT} protocol through a socket. Currently used to
 * submit addServer and remove Server commands
 * @author Bela Ban
 * @since  0.2
 */
public class Client {

    protected static void start(InetAddress dest, int port, String add_server, String remove_server) throws Throwable {
        try(Socket sock=new Socket(dest, port);
            DataInputStream in=new DataInputStream(sock.getInputStream());
            DataOutputStream out=new DataOutputStream(sock.getOutputStream())) {

            CLIENT.RequestType type=add_server != null? CLIENT.RequestType.add_server : CLIENT.RequestType.remove_server;
            out.writeByte((byte)type.ordinal());
            byte[] buf=Util.stringToBytes(add_server != null? add_server : remove_server);
            out.writeInt(buf.length);
            out.write(buf, 0, buf.length);

            type=CLIENT.RequestType.values()[in.readByte()];
            if(type != CLIENT.RequestType.rsp)
                throw new IllegalStateException(String.format("expected type %s but got %s", CLIENT.RequestType.rsp, type));
            int len=in.readInt();
            if(len == 0)
                return;
            buf=new byte[len];
            in.readFully(buf);

            Object response=Util.objectFromByteBuffer(buf);
            if(response instanceof Throwable)
                throw (Throwable)response;
            System.out.println("response = " + response);
        }
        catch(Exception ex) {
            ex.printStackTrace();
        }
    }

    public static void main(String[] args) throws Throwable {
        InetAddress dest=InetAddress.getLocalHost();
        int         port=1965;
        String      add_server=null, remove_server=null;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-dest")) {
                dest=InetAddress.getByName(args[++i]);
                continue;
            }
            if(args[i].equals("-port")) {
                port=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-add")) {
                add_server=args[++i];
                continue;
            }
            if(args[i].equals("-remove")) {
                remove_server=args[++i];
                continue;
            }
            help();
        }

        if(add_server == null && remove_server == null) {
            System.err.println("no server to be added or removed was given");
            return;
        }
        if(add_server != null && remove_server != null) {
            System.err.println("only one server can be added or removed at a time");
            return;
        }
        Client.start(dest, port, add_server, remove_server);
    }

    protected static void help() {
        System.out.println("Client [-dest <destination address>] [-port <port>] (-add <server> | -remove <server>)");
    }
}
