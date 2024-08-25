package org.jgroups.protocols.raft;

import static org.fusesource.leveldbjni.JniDBFactory.factory;
import static org.jgroups.raft.util.LongHelper.fromByteArrayToLong;
import static org.jgroups.raft.util.LongHelper.fromLongToByteArray;

import org.jgroups.Address;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.Util;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.function.ObjLongConsumer;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;
import org.iq80.leveldb.WriteOptions;

/**
 * Implementation of {@link Log}
 * @author Ugo Landini
 */
public class LevelDBLog implements Log {
    protected final org.jgroups.logging.Log log=LogFactory.getLog(this.getClass());

    private static final byte[] FIRSTAPPENDED = "FA".getBytes();
    private static final byte[] LASTAPPENDED  = "LA".getBytes();
    private static final byte[] CURRENTTERM   = "CT".getBytes();
    private static final byte[] COMMITINDEX   = "CX".getBytes();
    private static final byte[] VOTEDFOR      = "VF".getBytes();
    private static final byte[] SNAPSHOT      = "SN".getBytes();

    private DB                 db;
    private File               dbFileName;
    private long               currentTerm;
    private Address            votedFor;
    private long               firstAppended; // always: firstAppened <= commitIndex <= lastAppened
    private long               commitIndex;
    private long               lastAppended;
    private final WriteOptions write_options=new WriteOptions();


    @Override
    public void init(String log_name, Map<String,String> args) throws Exception {
        Options options = new Options().createIfMissing(true);
        this.dbFileName = new File(log_name);
        db = factory.open(dbFileName, options);
        log.trace("opened %s", db);

        if (isANewRAFTLog()) {
            log.trace("log %s is new, must be initialized", dbFileName);
            initLogWithMetadata();
        } else {
            log.trace("log %s exists, does not have to be initialized", dbFileName);
            readMetadataFromLog();
        }
        checkForConsistency();
    }

    public Log useFsync(boolean f) {
        write_options.sync(f);
        return this;
    }

    public boolean useFsync() {
        return write_options.sync();
    }

    @Override
    public void close() throws IOException {
        log.trace("closing DB: %s", db);
        Util.close(db);
        votedFor = null;
        currentTerm=commitIndex=lastAppended=firstAppended=0L;
    }

    @Override
    public void delete() throws IOException {
        Util.close(this);
        log.trace("deleting DB directory: %s", dbFileName);
        factory.destroy(dbFileName, new Options());
    }





    @Override public long firstAppended() {return firstAppended;}
    @Override public long commitIndex()   {return commitIndex;}
    @Override public long lastAppended()  {return lastAppended;}
    @Override public long currentTerm()   {return currentTerm;}
    @Override public Address votedFor()   {return votedFor;}


    @Override
    public Log commitIndex(long new_index) {
        if(new_index == commitIndex)
            return this;
        log.trace("Updating commit index: %d", new_index);
        db.put(COMMITINDEX, fromLongToByteArray(new_index));
        commitIndex=new_index;
        return this;
    }


    @Override
    public Log currentTerm(long new_term) {
        if(new_term == currentTerm)
            return this;
        log.trace("Updating current term: %d", new_term);
        db.put(CURRENTTERM, fromLongToByteArray(new_term));
        currentTerm = new_term;
        return this;
    }



    @Override
    public Log votedFor(Address member) {
        if(Objects.equals(member,votedFor))
            return this;
        try {
            log.debug("Updating voted for: %s", member);
            db.put(VOTEDFOR, Util.objectToByteBuffer(member));
            votedFor=member;
        }
        catch (Exception ignored) {
        }
        return this;
    }

    public void setSnapshot(ByteBuffer sn) {
        byte[] snapshot;
        if(sn.isDirect())
            snapshot=Util.bufferToArray(sn);
        else {
            if(sn.arrayOffset() > 0 || sn.capacity() != sn.remaining()) {
                int len=sn.remaining();
                snapshot=new byte[len];
                System.arraycopy(sn.array(), sn.arrayOffset(), snapshot, 0, len);
            }
            else
                snapshot=sn.array();
        }
        db.put(SNAPSHOT, snapshot);
    }

    public ByteBuffer getSnapshot() {
        byte[] snapshot=db.get(SNAPSHOT);
        return snapshot != null? ByteBuffer.wrap(snapshot) : null;
    }

    @Override
    public long append(long index, LogEntries entries) {
        log.trace("Appending %d entries", entries.size());
        long new_last_appended=-1;
        try (WriteBatch batch = db.createWriteBatch()) {
            for(LogEntry entry : entries) {
                appendEntry(index, entry, batch);
                new_last_appended=index;
                updateCurrentTerm(entry.term, batch);
                index++;
            }
            if(new_last_appended >= 0)
                updateLastAppended(new_last_appended, batch);
            log.trace("Flushing batch to DB: %s", batch);
            db.write(batch, write_options);
        }
        catch(Exception ex) {
        }
        return lastAppended;
    }

    @Override
    public LogEntry get(long index) {
        byte[] entryBytes=db.get(fromLongToByteArray(index));
        try {
            return entryBytes != null? Util.streamableFromByteBuffer(LogEntry.class, entryBytes) : null;
        }
        catch (Exception ex) {
            throw new RuntimeException(String.format("getting log entry at index %d failed", index), ex);
        }
    }

    @Override
    public void forEach(ObjLongConsumer<LogEntry> function, long start_index, long end_index) {
        start_index=Math.max(start_index, Math.max(firstAppended,1));
        end_index=Math.min(end_index, lastAppended);
        DBIterator it=db.iterator();  // ((DBIterator)it).seekToFirst();
        it.seek(fromLongToByteArray(start_index));
        for(long i=start_index; i <= end_index && it.hasNext(); i++) {
            Map.Entry<byte[],byte[]> e=it.next();
            try {
                LogEntry l=Util.streamableFromByteBuffer(LogEntry.class, e.getValue());
                function.accept(l, i);
            }
            catch(Exception ex) {
                throw new RuntimeException("failed deserializing LogRecord " + i, ex);
            }
        }
    }

    @Override
    public void forEach(ObjLongConsumer<LogEntry> function) {
        this.forEach(function, Math.max(1, firstAppended), lastAppended);
    }

    public long sizeInBytes() {
        // hmm, the code below doesn't work and always returns 0 (even when log_use_fsync is true)!
        /*byte[] from_bytes=fromLongToByteArray(firstAppended), to_bytes=fromLongToByteArray(lastAppended);
        long[] sizes=db.getApproximateSizes(new Range(from_bytes, to_bytes)); // hope this is not O(n)!
        return sizes[0];*/

        // this code below may not be so efficient...
        long size=0;
        long start_index=Math.max(firstAppended, 1);
        DBIterator it=db.iterator();  // ((DBIterator)it).seekToFirst();
        it.seek(fromLongToByteArray(start_index));
        for(long i=start_index; i <= lastAppended && it.hasNext(); i++) {
            Map.Entry<byte[],byte[]> e=it.next();
            byte[] v=e.getValue();
            size+=v != null? v.length : 0;
        }
        Util.close(it);
        return size;
    }

    @Override
    public void truncate(long index_exclusive) {
        if(index_exclusive < firstAppended)
            return;

        if(index_exclusive > commitIndex) {
            log.warn("upto_index (%d) is higher than commit-index (%d); only truncating up to %d",
                     index_exclusive, commitIndex, commitIndex);
            index_exclusive=commitIndex;
        }

        WriteBatch batch=null;
        try {
            batch = db.createWriteBatch();
            for(long index=firstAppended; index < index_exclusive; index++) {
                batch.delete(fromLongToByteArray(index));
            }
            batch.put(FIRSTAPPENDED, fromLongToByteArray(index_exclusive));

            if (lastAppended < index_exclusive) {
                lastAppended=index_exclusive;
                batch.put(LASTAPPENDED, fromLongToByteArray(index_exclusive));
            }

            db.write(batch, write_options);
            firstAppended=index_exclusive;
        }
        finally {
            Util.close(batch);
        }
    }

    @Override
    public void reinitializeTo(long index, LogEntry le) throws Exception {
        WriteBatch batch=null;
        try {
            batch=db.createWriteBatch();
            for(long i=firstAppended; i <= lastAppended; i++)
                batch.delete(fromLongToByteArray(i));
            appendEntry(index, le, batch);
            byte[] idx=fromLongToByteArray(index);
            batch.put(FIRSTAPPENDED, idx);
            batch.put(COMMITINDEX, idx);
            batch.put(LASTAPPENDED, idx);
            batch.put(CURRENTTERM, fromLongToByteArray(le.term()));
            firstAppended=commitIndex=lastAppended=index;
            currentTerm=le.term();
            db.write(batch, write_options);
        }
        finally {
            Util.close(batch);
        }
    }

    @Override
    public void deleteAllEntriesStartingFrom(final long start_index) {
        if (start_index< firstAppended || start_index> lastAppended)
            return;

        WriteBatch batch=null;
        try {
            batch = db.createWriteBatch();
            for (long index = start_index; index <= lastAppended; index++) {
                batch.delete(fromLongToByteArray(index));
            }
            LogEntry last = get(start_index-1);

            if (last == null) {
                updateCurrentTerm(0, batch);
            } else {
                updateCurrentTerm(last.term, batch);
            }
            updateLastAppended(start_index - 1, batch);
            if(commitIndex > lastAppended)
                commitIndex(lastAppended);
            db.write(batch, write_options);
        }
        finally {
            Util.close(batch);
        }

    }

    // Useful in debugging
    public byte[] print(byte[] bytes) {
        return db.get(bytes);
    }

    // Useful in debugging
    public void printMetadata() throws Exception {

        log.info("-----------------");
        log.info("RAFT Log Metadata");
        log.info("-----------------");

        byte[] firstAppendedBytes = db.get(FIRSTAPPENDED);
        log.info("First Appended: %d", fromByteArrayToLong(firstAppendedBytes));
        byte[] lastAppendedBytes = db.get(LASTAPPENDED);
        log.info("Last Appended: %d", fromByteArrayToLong(lastAppendedBytes));
        byte[] currentTermBytes = db.get(CURRENTTERM);
        log.info("Current Term: %d", fromByteArrayToLong(currentTermBytes));
        byte[] commitIndexBytes = db.get(COMMITINDEX);
        log.info("Commit Index: %d", fromByteArrayToLong(commitIndexBytes));
        Address votedForTmp =Util.objectFromByteBuffer(db.get(VOTEDFOR));
        log.info("Voted for: %s", votedForTmp);
    }

    @Override
    public String toString() {
        return String.format("first=%d, commit=%d, last-appended=%d, term=%d (size=%d)",
                             firstAppended, commitIndex, lastAppended, currentTerm, size());
    }


    private void appendEntry(long index, LogEntry entry, WriteBatch batch) throws Exception {
        log.trace("Appending entry %d: %s", index, entry);
        batch.put(fromLongToByteArray(index), Util.streamableToByteBuffer(entry));
    }


    private void updateCurrentTerm(long new_term, WriteBatch batch) {
        if(new_term == currentTerm)
            return;
        log.trace("Updating currentTerm: %d", new_term);
        batch.put(CURRENTTERM, fromLongToByteArray(new_term));
        currentTerm = new_term;
    }

    private void updateLastAppended(long new_last_appended, WriteBatch batch) {
        if(new_last_appended == lastAppended)
            return;
        log.trace("Updating lastAppended: %d", new_last_appended);
        batch.put(LASTAPPENDED, fromLongToByteArray(new_last_appended));
        lastAppended = new_last_appended;
    }



    private boolean isANewRAFTLog() {
        return (db.get(FIRSTAPPENDED) == null);
    }

    private void initLogWithMetadata() {

        log.debug("Initializing log with empty Metadata");
        WriteBatch batch = db.createWriteBatch();
        try {
            batch.put(FIRSTAPPENDED, fromLongToByteArray(0));
            batch.put(LASTAPPENDED, fromLongToByteArray(0));
            batch.put(CURRENTTERM, fromLongToByteArray(0));
            batch.put(COMMITINDEX, fromLongToByteArray(0));
            db.write(batch, write_options);
        } catch (Exception ex) {
            ex.printStackTrace(); // todo: better error handling
        } finally {
            try {
                batch.close();
            } catch (IOException e) {
                e.printStackTrace(); // todo: better error handling
            }
        }
    }

    private void readMetadataFromLog() throws Exception {
        firstAppended= fromByteArrayToLong(db.get(FIRSTAPPENDED));
        lastAppended= fromByteArrayToLong(db.get(LASTAPPENDED));
        currentTerm = fromByteArrayToLong(db.get(CURRENTTERM));
        commitIndex = fromByteArrayToLong(db.get(COMMITINDEX));
        votedFor =Util.objectFromByteBuffer(db.get(VOTEDFOR));
        log.debug("read metadata from log: firstAppended=%d, lastAppended=%d, currentTerm=%d, commitIndex=%d, votedFor=%s",
                  firstAppended, lastAppended, currentTerm, commitIndex, votedFor);
    }

    private void checkForConsistency() throws Exception {

        long loggedFirstAppended=fromByteArrayToLong(db.get(FIRSTAPPENDED));
        log.trace("FirstAppended in DB is: %d", loggedFirstAppended);

        long loggedLastAppended = fromByteArrayToLong(db.get(LASTAPPENDED));
        log.trace("LastAppended in DB is: %d", loggedLastAppended);

        long loggedCurrentTerm = fromByteArrayToLong(db.get(CURRENTTERM));
        log.trace("CurrentTerm in DB is: %d", loggedCurrentTerm);

        long loggedCommitIndex = fromByteArrayToLong(db.get(COMMITINDEX));
        log.trace("CommitIndex in DB is: %d", loggedCommitIndex);

        Address loggedVotedForAddress =Util.objectFromByteBuffer(db.get(VOTEDFOR));
        log.trace("VotedFor in DB is: %s", loggedVotedForAddress);

        assert firstAppended == loggedFirstAppended;
        assert lastAppended == loggedLastAppended;
        assert currentTerm == loggedCurrentTerm;
        assert commitIndex == loggedCommitIndex;
        assert votedFor == null || (votedFor.equals(loggedVotedForAddress));

        LogEntry lastAppendedEntry = get(lastAppended);
        assert (lastAppendedEntry==null || lastAppendedEntry.term <= currentTerm);
        assert firstAppended <= commitIndex : String.format("first=%d, commit=%d", firstAppended, commitIndex);
        assert commitIndex <= lastAppended : String.format("commit=%d, last=%d", commitIndex, lastAppended);
    }

}
