package org.jgroups.protocols.raft;

import org.jgroups.Address;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;
import org.mapdb.Atomic;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import java.io.*;
import java.util.Map;

/**
 * Implementation of {@link org.jgroups.protocols.raft.Log} with MapDB (http://www.mapdb.org)
 * @author Bela Ban
 * @since  0.1
 */
public class MapDBLog implements Log {
    protected DB                  db;
    protected String              filename;
    protected Atomic.Integer      current_term, last_applied, commit_index;
    protected Atomic.Var<Address> voted_for;

    public MapDBLog() {
    }

    public void init(String log_name, Map<String,String> args) throws Exception {
        String dir=Util.checkForMac()? File.separator + "tmp" : System.getProperty("java.io.tmpdir", File.separator + "tmp");
        filename=dir + File.separator + log_name;
        db=DBMaker.newFileDB(new File(filename)).closeOnJvmShutdown().make();
        current_term=db.getAtomicInteger("current_term");
        last_applied=db.getAtomicInteger("last_applied");
        commit_index=db.getAtomicInteger("commit_index");
        if(db.exists("voted_for"))
            voted_for=db.getAtomicVar("voted_for");
        else
            voted_for=db.createAtomicVar("voted_for", null, new StreamableSerializer<Address>(Address.class));
    }

    public void close() {
        db.close();
    }

    @Override
    public void delete() {
        db.getCatalog().clear();
        db.commit();
    }

    public int currentTerm() {
        return current_term.get();
    }

    public Log currentTerm(int new_term) {
        current_term.set(new_term);
        db.commit();
        return this;
    }

    public Address votedFor() {
        return voted_for.get();
    }

    public Log votedFor(Address member) {
        voted_for.set(member);
        db.commit();
        return this;
    }

    public int first() {
        return 0;
    }

    public int commitIndex() {
        return commit_index.get();
    }

    public Log commitIndex(int new_index) {
        commit_index.set(new_index);
        return this;
    }

    public int lastApplied() {
        return last_applied.get();
    }

    @Override
    public void append(int index, LogEntry... entries) {

    }

    public AppendResult append(int prev_index, int prev_term, LogEntry ... entries) {
        return null;
    }

    public void forEach(Function function,int start_index,int end_index) {

    }

    public void forEach(Function function) {

    }



    protected static class StreamableSerializer<T> implements Serializer<T>, Serializable {
        private static final long serialVersionUID=7230936820893354049L;
        protected final Class<T>  clazz;

        public StreamableSerializer(Class<T> clazz) {
            this.clazz=clazz;
            if(clazz.isAssignableFrom(Streamable.class))
                throw new IllegalArgumentException("argument (" + clazz.getSimpleName() + ") has to be Streamable");
        }

        @Override
        public void serialize(DataOutput out, T value) throws IOException {
            try {
                Util.writeStreamable((Streamable)value, out);
            }
            catch(Exception ex) {
                throw new IOException(ex);
            }
        }

        @Override
        public T deserialize(DataInput in, int available) throws IOException {
            try {
                return (T)Util.readStreamable(clazz, in);
            }
            catch(Exception ex) {
                throw new IOException(ex);
            }
        }

        @Override public int fixedSize() {return -1;}
    }
}
