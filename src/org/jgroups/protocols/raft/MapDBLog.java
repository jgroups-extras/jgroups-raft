package org.jgroups.protocols.raft;

import org.jgroups.Address;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;
import org.mapdb.*;

import java.io.*;
import java.util.Map;
import java.util.function.ObjIntConsumer;

/**
 * Implementation of {@link org.jgroups.protocols.raft.Log} with MapDB (http://www.mapdb.org)
 * @author Bela Ban
 * @since  0.1
 */
public class MapDBLog implements Log {
    protected DB                         db;
    protected String                     filename;
    protected Atomic.Integer             current_term, last_appended, commit_index;
    protected Atomic.Var<Address>        voted_for;
    protected BTreeMap<Integer,LogEntry> log_entries;
    protected static final String        CURRENT_TERM  = "current_term";
    protected static final String        LAST_APPENDED = "last_appended";
    protected static final String        COMMIT_INDEX  = "commit_index";
    protected static final String        VOTED_FOR     = "voted_for";
    protected static final String        LOG_ENTRIES   = "log_entries";

    public MapDBLog() {
    }

    public void init(String log_name, Map<String,String> args) throws Exception {
        String dir=Util.checkForMac()? File.separator + "tmp" : System.getProperty("java.io.tmpdir", File.separator + "tmp");
        filename=dir + File.separator + log_name;
        db=DBMaker.newFileDB(new File(filename)).closeOnJvmShutdown().make();
        current_term=db.getAtomicInteger(CURRENT_TERM);
        last_appended=db.getAtomicInteger(LAST_APPENDED);
        commit_index=db.getAtomicInteger(COMMIT_INDEX);
        voted_for=db.exists(VOTED_FOR)? db.<Address>getAtomicVar(VOTED_FOR)
          : db.createAtomicVar(VOTED_FOR, null, new StreamableSerializer<>(Address.class));
        log_entries=db.exists(LOG_ENTRIES)? db.<Integer,LogEntry>getTreeMap(LOG_ENTRIES)
          : db.createTreeMap(LOG_ENTRIES).valueSerializer(new StreamableSerializer<>(LogEntry.class)).<Integer,LogEntry>make();
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

    public int firstAppended() {
        return 0;
    }

    public int commitIndex() {
        return commit_index.get();
    }

    public Log commitIndex(int new_index) {
        commit_index.set(new_index);
        return this;
    }

    public int lastAppended() {
        return last_appended.get();
    }

    @Override
    public void append(int index, boolean overwrite, LogEntry... entries) {
        if(!overwrite && log_entries.containsKey(index))
            throw new IllegalStateException("entry at index " + index + " already exists");
        for(LogEntry entry: entries)
            log_entries.put(index, entry);
        last_appended.set(index);
        db.commit();
    }

    /**
     * Delete all entries starting from start_index.
     * Updates current_term and last_appended accordingly
     *
     * @param index The index
     * @return The LogEntry, or null if none's present at index.
     */
    @Override
    public LogEntry get(int index) {
        return null;
    }

    @Override
    public void truncate(int index) {

    }

    @Override
    public void deleteAllEntriesStartingFrom(int start_index) {
    }

    public void forEach(ObjIntConsumer<LogEntry> function,int start_index,int end_index) {

    }

    public void forEach(ObjIntConsumer<LogEntry> function) {

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
