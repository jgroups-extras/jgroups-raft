package org.jgroups.raft.client;

import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.raft.CLIENT;
import org.jgroups.protocols.raft.Settable;
import org.jgroups.util.DefaultThreadFactory;
import org.jgroups.util.Runner;
import org.jgroups.util.Util;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Client stub which accesses a remote server via the {@link CLIENT} protocol through a socket. Forwards all requests
 * to the remote server and receives the responses.
 * @author Bela Ban
 * @since  1.0.0
 */
public class ClientStub implements Settable, Closeable {
    protected InetAddress                                  host;
    protected int                                          port=1965;
    protected Socket                                       sock;
    protected DataInputStream                              in;
    protected DataOutputStream                             out;
    protected int                                          current_request_id=1; // to match responses with requests
    protected final Map<Integer,CompletableFuture<byte[]>> requests=new ConcurrentHashMap<>();
    protected Runner                                       runner;
    protected final Log                                    log=LogFactory.getLog(ClientStub.class);

    public ClientStub(InetAddress host, int port) {
        this.host=host;
        this.port=port;
    }

    public InetAddress getHost()              {return host;}
    public ClientStub  setHost(InetAddress h) {this.host=h; return this;}
    public int         getPort()              {return port;}
    public ClientStub  setPort(int p)         {this.port=p; return this;}


    public ClientStub start() throws Exception {
        if(sock != null && sock.isConnected()) return this;
        if(host == null)
            host=InetAddress.getLocalHost();
        sock=new Socket(host, port);
        in=new DataInputStream(sock.getInputStream());
        out=new DataOutputStream(sock.getOutputStream());
        runner=new Runner(new DefaultThreadFactory("clientstub", true, true),
                          "client-stub-reader", this::readResponse, null).start();
        return this;
    }

    public ClientStub stop() {
        Util.close(runner, sock,in,out);
        requests.values().forEach(cf -> cf.completeExceptionally(new IllegalStateException("server socket closed")));
        return this;
    }

    @Override public void close() {
        stop();
    }

    @Override
    public CompletableFuture<byte[]> setAsync(byte[] buf, int offset, int length) throws Exception {
        return setAsync(CLIENT.RequestType.set_req, buf, offset, length);
    }

    public CompletableFuture<byte[]> setAsync(CLIENT.RequestType type,
                                              byte[] buf, int offset, int length) throws Exception {
           CompletableFuture<byte[]> req=new CompletableFuture<>();
           int                       req_id;

           synchronized(this) {
               req_id=this.current_request_id++;
           }

           requests.put(req_id, req);
           out.writeByte((byte)type.ordinal());
           out.writeInt(req_id);
           out.writeInt(buf.length);
           out.write(buf, offset, length);
           return req;
       }

    protected void readResponse() {
        CompletableFuture<byte[]> cf=null;
        try {
            CLIENT.RequestType type=CLIENT.RequestType.values()[in.readByte()];
            if(type != CLIENT.RequestType.rsp)
                throw new IllegalStateException(String.format("expected type %s but got %s", CLIENT.RequestType.rsp, type));
            int req_id=in.readInt();
            cf=requests.get(req_id);
            if(cf == null)
                log.warn("request with id=%d not found", req_id);
            int len=in.readInt();
            if(len == 0) {
                if(cf != null)
                    cf.complete(null);
                return;
            }
            byte[] buf=new byte[len];
            in.readFully(buf);
            if(cf != null)
                cf.complete(buf);
        }
        catch(Throwable t) {
            log.error("failed reading response", t);
            if(cf != null)
                cf.completeExceptionally(t);
        }
    }


    @Override
    public String toString() {
        return String.format("remote: %s:%d%s", host, port, sock != null && sock.isConnected()? " (connected)" : "");
    }



}
