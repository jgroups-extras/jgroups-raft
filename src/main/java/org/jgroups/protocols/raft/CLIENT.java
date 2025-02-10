package org.jgroups.protocols.raft;

import org.jgroups.Global;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.Property;
import org.jgroups.conf.AttributeType;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.raft.Settable;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Util;

import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * Protocol listening on a socket for client requests. Dispatches them to the leader (via {@link REDIRECT}) and sends
 * back the response. Requests and responses are always sent as
 * <pre>
 *     | RequestType (byte) | request-id (int) | length (int) | byte[] buffer |
 * </pre>
 * @author Bela Ban
 * @since  0.2
 */
@MBean(description="Listens on a socket for client requests, forwards them to the leader and send responses")
public class CLIENT extends Protocol implements Runnable {
    protected static final short  CLIENT_ID = 523;
    protected static final byte[] BUF={};

    public enum RequestType {set_req, add_server, remove_server, type, rsp, get_req}

    static {
        ClassConfigurator.addProtocol(CLIENT_ID, CLIENT.class);
    }

    @Property(name="bind_addr",
      description="The bind address which should be used by the server socket. The following special values " +
        "are also recognized: GLOBAL, SITE_LOCAL, LINK_LOCAL, NON_LOOPBACK, match-interface, match-host, match-address",
      systemProperty={Global.BIND_ADDR},writable=false)
    protected InetAddress       bind_addr;

    @Property(description="Port to listen for client requests",writable=false)
    protected int               port=1965;

    @Property(description="The min threads in the thread pool")
    protected int               min_threads;

    @Property(description="Max number of threads in the thread pool")
    protected int               max_threads=100;

    @Property(description="Number of ms a thread can be idle before being removed from the thread pool",
      type=AttributeType.TIME)
    protected long              idle_time=5000;

    @Property(description="Number of bytes of the server socket's receive buffer",type=AttributeType.BYTES)
    protected int               recv_buf_size;

    protected Settable settable;
    protected DynamicMembership dyn_membership;
    protected ServerSocket      sock;
    protected ExecutorService   thread_pool; // to handle requests
    protected Thread            acceptor;


    public InetAddress getBindAddress()              {return bind_addr;}
    public CLIENT      setBindAddress(InetAddress b) {this.bind_addr=b; return this;}
    public int         getPort()                     {return port;}
    public CLIENT      setPort(int p)                {this.port=p; return this;}
    public int         getMinThreads()               {return min_threads;}
    public CLIENT      setMinThreads(int t)          {this.min_threads=t; return this;}
    public int         getMaxThreads()               {return max_threads;}
    public CLIENT      setMaxThreads(int t)          {this.max_threads=t; return this;}
    public long        getIdleTime()                 {return idle_time;}
    public CLIENT      setIdleTime(long t)           {this.idle_time=t; return this;}
    public int         getReceiveBufferSize()        {return recv_buf_size;}
    public CLIENT      setReceiveBufferSize(int s)   {this.recv_buf_size=s; return this;}


    public void init() throws Exception {
        super.init();
        if((settable=RAFT.findProtocol(Settable.class, this, true)) == null)
            throw new IllegalStateException("did not find a protocol implementing Settable (e.g. REDIRECT or RAFT)");
        if((dyn_membership=RAFT.findProtocol(DynamicMembership.class, this, true)) == null)
            throw new IllegalStateException("did not find a protocol implementing DynamicMembership (e.g. REDIRECT or RAFT)");
    }

    public void start() throws Exception {
        super.start();
        sock=Util.createServerSocket(getSocketFactory(), "CLIENR.srv_sock", bind_addr, port, port+50, recv_buf_size);
        if(sock == null)
            throw new IllegalStateException(String.format("failed creating server socket at %s:%d", bind_addr, port));
        thread_pool=new ThreadPoolExecutor(min_threads, max_threads, idle_time, TimeUnit.MILLISECONDS,
                                           new SynchronousQueue<>(), getThreadFactory(), new ThreadPoolExecutor.DiscardPolicy());
        acceptor=new Thread(this, "CLIENT.Acceptor");
        acceptor.start();
    }

    public void stop() {
        super.stop();
        Util.close(sock);
        if(thread_pool != null)
            thread_pool.shutdown();
    }

    @Override
    public void destroy() {
        super.destroy();
        Util.close(sock);
        if(thread_pool != null)
            thread_pool.shutdown();
    }

    public void run() {
        while(true) {
            try {
                Socket client_sock=sock.accept();
                thread_pool.execute(new RequestHandler(client_sock));
            }
            catch(IOException ignored) {
                if(sock.isClosed())
                    return;
            }
            catch(Throwable ex) {
                log.error("error accepting new connection", ex);
            }
        }
    }

    public void up(MessageBatch batch) {
        up_prot.up(batch);
    }

    protected class RequestHandler implements Runnable {
        protected final Socket client_sock;

        public RequestHandler(Socket client_sock) {
            this.client_sock=client_sock;
        }

        public void run() {
            DataInputStream in=null; DataOutputStream out=null;
            CompletionHandler completion_handler=null;
            try {
                in=new DataInputStream(client_sock.getInputStream());
                out=new DataOutputStream(client_sock.getOutputStream());
                RequestType type=RequestType.values()[in.readByte()];
                int request_id=in.readInt();
                completion_handler=new CompletionHandler(client_sock, in, out, request_id);
                int length=in.readInt();
                byte[] buffer=new byte[length];
                in.readFully(buffer);
                switch(type) {
                    case set_req:
                        settable.setAsync(buffer, 0, buffer.length).whenComplete(completion_handler);
                        break;
                    case get_req:
                        settable.getAsync(buffer, 0, buffer.length, null).whenComplete(completion_handler);
                    case add_server:
                        dyn_membership.addServer(Util.bytesToString(buffer)).whenComplete(completion_handler);
                        break;
                    case remove_server:
                        dyn_membership.removeServer(Util.bytesToString(buffer)).whenComplete(completion_handler);
                        break;
                    case rsp:
                        // not handled on the server side
                        break;
                }
            }
            catch(Throwable ex) {
                log.error("failed handling request", ex);
                if(completion_handler != null)
                    completion_handler.accept(null, ex); // sends back an error response
                Util.close(in,out,client_sock);
            }
        }

        protected void send(DataOutput out, RequestType type, int request_id,
                            byte[] buffer, int offset, int length) throws Exception {
            out.writeByte((byte)type.ordinal());
            out.writeInt(request_id);
            int len=buffer == null? 0 : length;
            out.writeInt(len);
            if(len > 0)
                out.write(buffer, offset, length);
        }


        protected class CompletionHandler implements BiConsumer<byte[],Throwable> {
            protected final Socket           s;
            protected final DataInputStream  input;
            protected final DataOutputStream output;
            protected final int              req_id;

            public CompletionHandler(Socket client_sock, DataInputStream input, DataOutputStream output, int req) {
                this.s=client_sock;
                this.input=input;
                this.output=output;
                this.req_id=req;
            }

            public void accept(byte[] buf, Throwable ex) {
                try {
                    if(ex != null) {
                        byte[] rsp_buffer=Util.objectToByteBuffer(ex);
                        send(output, RequestType.rsp, req_id, rsp_buffer, 0, rsp_buffer.length);
                        return;
                    }
                    if(buf == null)
                        buf=BUF;
                    send(output, RequestType.rsp, req_id, buf, 0, buf.length);
                }
                catch(Throwable t) {
                    log.error("failed in sending response to client", t);
                }
                finally {
                    Util.close(output, input, s);
                }
            }
        }
    }
}
