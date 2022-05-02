package org.jgroups.protocols.raft;

import org.iq80.leveldb.*;
import org.jgroups.Address;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.Util;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.function.ObjIntConsumer;

import static org.fusesource.leveldbjni.JniDBFactory.factory;
import static org.jgroups.raft.util.IntegerHelper.fromByteArrayToInt;
import static org.jgroups.raft.util.IntegerHelper.fromIntToByteArray;

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

    private DB                 db;
    private File               dbFileName;
    private int                currentTerm;
    private Address            votedFor;
    private int                firstAppended; // always: firstAppened <= commitIndex <= lastAppened
    private int                commitIndex;
    private int                lastAppended;
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
        if (db != null) db.close();
        currentTerm = 0;
        votedFor = null;
        commitIndex = 0;
        lastAppended= 0;
        firstAppended= 0;
    }

    @Override
    public void delete() throws IOException {
        Util.close(this);
        log.trace("deleting DB directory: %s", dbFileName);
        factory.destroy(dbFileName, new Options());
    }





    @Override public int     firstAppended() {return firstAppended;}
    @Override public int     commitIndex()   {return commitIndex;}
    @Override public int     lastAppended()  {return lastAppended;}
    @Override public int     currentTerm()   {return currentTerm;}
    @Override public Address votedFor()      {return votedFor;}


    @Override
    public Log commitIndex(int new_index) {
        if(new_index == commitIndex)
            return this;
        log.trace("Updating commit index: %d", new_index);
        db.put(COMMITINDEX, fromIntToByteArray(new_index));
        commitIndex=new_index;
        return this;
    }


    @Override
    public Log currentTerm(int new_term) {
        if(new_term == currentTerm)
            return this;
        log.trace("Updating current term: %d", new_term);
        db.put(CURRENTTERM, fromIntToByteArray(new_term));
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


    @Override
    public int append(int index, LogEntries entries) {
        log.trace("Appending %d entries", entries.size());
        int new_last_appended=-1;
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
    public LogEntry get(int index) {
        byte[] entryBytes=db.get(fromIntToByteArray(index));
        try {
            return entryBytes != null? Util.streamableFromByteBuffer(LogEntry.class, entryBytes) : null;
        }
        catch (Exception ex) {
            throw new RuntimeException(String.format("getting log entry at index %d failed", index), ex);
        }
    }

    @Override
    public void forEach(ObjIntConsumer<LogEntry> function, int start_index, int end_index) {
        start_index=Math.max(start_index, Math.max(firstAppended,1));
        end_index=Math.min(end_index, lastAppended);
        DBIterator it=db.iterator();  // ((DBIterator)it).seekToFirst();
        it.seek(fromIntToByteArray(start_index));
        for(int i=start_index; i <= end_index && it.hasNext(); i++) {
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
    public void forEach(ObjIntConsumer<LogEntry> function) {
        this.forEach(function, Math.max(1, firstAppended), lastAppended);
    }

    @Override
    public void truncate(int index_exclusive) {
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
            for(int index=firstAppended; index < index_exclusive; index++) {
                batch.delete(fromIntToByteArray(index));
            }
            batch.put(FIRSTAPPENDED, fromIntToByteArray(index_exclusive));

            if (lastAppended < index_exclusive) {
                lastAppended=index_exclusive;
                batch.put(LASTAPPENDED, fromIntToByteArray(index_exclusive));
            }

            db.write(batch, write_options);
            firstAppended=index_exclusive;
        }
        finally {
            Util.close(batch);
        }
    }

    @Override
    public void reinitializeTo(int index, LogEntry le) throws Exception {
        WriteBatch batch=null;
        try {
            batch=db.createWriteBatch();
            for(int i=firstAppended; i <= lastAppended; i++)
                batch.delete(fromIntToByteArray(i));
            appendEntry(index, le, batch);
            byte[] idx=fromIntToByteArray(index);
            batch.put(FIRSTAPPENDED, idx);
            batch.put(COMMITINDEX, idx);
            batch.put(LASTAPPENDED, idx);
            batch.put(CURRENTTERM, fromIntToByteArray(le.term()));
            firstAppended=commitIndex=lastAppended=index;
            currentTerm=le.term();
            db.write(batch, write_options);
        }
        finally {
            Util.close(batch);
        }
    }

    @Override
    public void deleteAllEntriesStartingFrom(final int start_index) {
        if (start_index< firstAppended || start_index> lastAppended)
            return;

        WriteBatch batch=null;
        try {
            batch = db.createWriteBatch();
            for (int index = start_index; index <= lastAppended; index++) {
                batch.delete(fromIntToByteArray(index));
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
        log.info("First Appended: %d", fromByteArrayToInt(firstAppendedBytes));
        byte[] lastAppendedBytes = db.get(LASTAPPENDED);
        log.info("Last Appended: %d", fromByteArrayToInt(lastAppendedBytes));
        byte[] currentTermBytes = db.get(CURRENTTERM);
        log.info("Current Term: %d", fromByteArrayToInt(currentTermBytes));
        byte[] commitIndexBytes = db.get(COMMITINDEX);
        log.info("Commit Index: %d", fromByteArrayToInt(commitIndexBytes));
        Address votedForTmp =Util.objectFromByteBuffer(db.get(VOTEDFOR));
        log.info("Voted for: %s", votedForTmp);
    }

    @Override
    public String toString() {
        return String.format("first=%d, commit=%d, last-appended=%d, term=%d (size=%d)",
                             firstAppended, commitIndex, lastAppended, currentTerm, size());
    }


    private void appendEntry(int index, LogEntry entry, WriteBatch batch) throws Exception {
        log.trace("Appending entry %d: %s", index, entry);
        batch.put(fromIntToByteArray(index), Util.streamableToByteBuffer(entry));
    }


    private void updateCurrentTerm(int new_term, WriteBatch batch) {
        if(new_term == currentTerm)
            return;
        log.trace("Updating currentTerm: %d", new_term);
        batch.put(CURRENTTERM, fromIntToByteArray(new_term));
        currentTerm = new_term;
    }

    private void updateLastAppended(int new_last_appended, WriteBatch batch) {
        if(new_last_appended == lastAppended)
            return;
        log.trace("Updating lastAppended: %d", new_last_appended);
        batch.put(LASTAPPENDED, fromIntToByteArray(new_last_appended));
        lastAppended = new_last_appended;
    }



    private boolean isANewRAFTLog() {
        return (db.get(FIRSTAPPENDED) == null);
    }

    private void initLogWithMetadata() {

        log.debug("Initializing log with empty Metadata");
        WriteBatch batch = db.createWriteBatch();
        try {
            batch.put(FIRSTAPPENDED, fromIntToByteArray(0));
            batch.put(LASTAPPENDED, fromIntToByteArray(0));
            batch.put(CURRENTTERM, fromIntToByteArray(0));
            batch.put(COMMITINDEX, fromIntToByteArray(0));
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
        firstAppended= fromByteArrayToInt(db.get(FIRSTAPPENDED));
        lastAppended= fromByteArrayToInt(db.get(LASTAPPENDED));
        currentTerm = fromByteArrayToInt(db.get(CURRENTTERM));
        commitIndex = fromByteArrayToInt(db.get(COMMITINDEX));
        votedFor =Util.objectFromByteBuffer(db.get(VOTEDFOR));
        log.debug("read metadata from log: firstAppended=%d, lastAppended=%d, currentTerm=%d, commitIndex=%d, votedFor=%s",
                  firstAppended, lastAppended, currentTerm, commitIndex, votedFor);
    }

    private void checkForConsistency() throws Exception {

        int loggedFirstAppended = fromByteArrayToInt(db.get(FIRSTAPPENDED));
        log.trace("FirstAppended in DB is: %d", loggedFirstAppended);

        int loggedLastAppended = fromByteArrayToInt(db.get(LASTAPPENDED));
        log.trace("LastAppended in DB is: %d", loggedLastAppended);

        int loggedCurrentTerm = fromByteArrayToInt(db.get(CURRENTTERM));
        log.trace("CurrentTerm in DB is: %d", loggedCurrentTerm);

        int loggedCommitIndex = fromByteArrayToInt(db.get(COMMITINDEX));
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
