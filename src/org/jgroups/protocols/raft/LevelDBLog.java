package org.jgroups.protocols.raft;

import org.apache.commons.io.FileUtils;
import org.iq80.leveldb.*;
import org.jgroups.Address;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.Util;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.function.ObjIntConsumer;

import static org.fusesource.leveldbjni.JniDBFactory.*;
import static org.jgroups.raft.util.IntegerHelper.fromByteArrayToInt;
import static org.jgroups.raft.util.IntegerHelper.fromIntToByteArray;

/**
 * Implementation of ${link #Log}
 * @author Ugo Landini
 */
public class LevelDBLog implements Log {

    protected final org.jgroups.logging.Log log= LogFactory.getLog(this.getClass());

    private static final byte[] FIRSTAPPENDED = "FA".getBytes();
    private static final byte[] LASTAPPENDED  = "LA".getBytes();
    private static final byte[] CURRENTTERM   = "CT".getBytes();
    private static final byte[] COMMITINDEX   = "CX".getBytes();
    private static final byte[] VOTEDFOR      = "VF".getBytes();

    private DB db;
    private File dbFileName;

    private Integer currentTerm = 0;
    private Address votedFor = null;

    private Integer commitIndex   = 0;
    private Integer lastAppended  = 0;
    private Integer firstAppended = 0;

    @Override
    public void init(String log_name, Map<String,String> args) throws Exception {

        Options options = new Options();
        options.createIfMissing(true);

        //String dir=Util.checkForMac()? File.separator + "tmp" : System.getProperty("java.io.tmpdir", File.separator + "tmp");
        //filename=dir + File.separator + log_name;

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

    @Override
    public void close() {
        try {
            log.trace("closing DB: %s", db);

            if (db!= null) db.close();
            currentTerm = 0;
            votedFor = null;
            commitIndex = 0;
            lastAppended= 0;
            firstAppended= 0;
        } catch (IOException e) {
            //@todo proper logging, etc
            e.printStackTrace();
        }
    }

    @Override
    public void delete() {
        this.close();
        try {
            log.trace("deleting DB directory: %s", dbFileName);
            FileUtils.deleteDirectory(dbFileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Override
    public int currentTerm() {
        return currentTerm;
    }

    @Override
    public Log currentTerm(int new_term) {
        currentTerm = new_term;
        log.trace("Updating current term: %d", currentTerm);
        db.put(CURRENTTERM, fromIntToByteArray(currentTerm));
        return this;
    }

    @Override
    public Address votedFor() {
        return votedFor;
    }

    @Override
    public Log votedFor(Address member) {
        votedFor = member;
        try {
            log.debug("Updating Voted for: %s", votedFor);
            db.put(VOTEDFOR, Util.objectToByteBuffer(member));
        } catch (Exception e) {
            e.printStackTrace(); // todo: better error handling
        }
        return this;
    }

    @Override
    public int firstAppended() {
        return firstAppended;
    }

    @Override
    public int commitIndex() {
        return commitIndex;
    }

    @Override
    public Log commitIndex(int new_index) {
        commitIndex = new_index;
        log.trace("Updating commit index: %d", commitIndex);
        db.put(COMMITINDEX, fromIntToByteArray(commitIndex));
        return this;
    }

    @Override
    public int lastAppended() {
        return lastAppended;
    }

    @Override
    public void append(int index, boolean overwrite, LogEntry... entries) {

        log.trace("Appending %d entries", entries.length);
        try (WriteBatch batch = db.createWriteBatch()){
            for(LogEntry entry : entries) {
                if(overwrite) {
                    appendEntry(index, entry, batch);
                }
                else {
                    appendEntryIfAbsent(index, entry, batch);
                }

                updateLastAppended(index, batch);
                updateCurrentTerm(entry.term, batch);

                log.trace("Flushing batch to DB: %s", batch);
                db.write(batch);
                index++;
            }
        }
        catch(Exception ex) {
            ex.printStackTrace(); // todo: better error handling
        }

    }

    @Override
    public LogEntry get(int index) {
        return getLogEntry(index);
    }

    @Override
    public void forEach(ObjIntConsumer<LogEntry> function, int start_index, int end_index) {
        start_index = Math.max(start_index, Math.max(firstAppended,1));
        end_index = Math.min(end_index, lastAppended);

        for (int i=start_index; i<=end_index; i++) {
            LogEntry entry = getLogEntry(i);
            function.accept(entry, i);
        }
    }

    @Override
    public void forEach(ObjIntConsumer<LogEntry> function) {
        this.forEach(function, Math.max(1, firstAppended), lastAppended);
    }

    @Override
    public void truncate(int upto_index) {
        if ((upto_index< firstAppended) || (upto_index> lastAppended)) {
            //@todo wrong index, must throw runtime exception
            return;
        }

        WriteBatch batch=null;
        try {
            batch = db.createWriteBatch();
            for (int index =firstAppended; index < upto_index; index++) {
                batch.delete(fromIntToByteArray(index));
            }

            firstAppended= upto_index;
            batch.put(FIRSTAPPENDED, fromIntToByteArray(upto_index));
            db.write(batch);
        }
        finally {
            Util.close(batch);
        }
    }

    @Override
    public void deleteAllEntriesStartingFrom(int start_index) {

        if ((start_index< firstAppended) || (start_index> lastAppended)) {
            //@todo wrong index, must throw runtime exception
            return;
        }
        WriteBatch batch=null;
        try {
            batch = db.createWriteBatch();
            for (int index = start_index; index <= lastAppended; index++) {
                batch.delete(fromIntToByteArray(index));
            }
            LogEntry last = getLogEntry(start_index-1);

            if (last == null) {
                updateCurrentTerm(0, batch);
            } else {
                updateCurrentTerm(last.term, batch);
            }
            updateLastAppended(start_index - 1, batch);
            if(commitIndex > lastAppended)
                commitIndex(lastAppended);
            db.write(batch);
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
        return "firstAppended=" + firstAppended + ", lastAppended=" + lastAppended + ", commitIndex=" + commitIndex + ", currentTerm=" + currentTerm;
    }

    private boolean checkIfPreviousEntryHasDifferentTerm(int prev_index, int prev_term) {

        log.trace("Checking term (%d) of previous entry (%d)", prev_term, prev_index);
        if(prev_index == 0) // index starts at 1
            return false;
        LogEntry prev_entry = getLogEntry(prev_index);
        return prev_entry == null || (prev_entry.term != prev_term);
    }


    private int findIndexWithTerm(int start_index, int prev_term) {

        for (LogEntry prev_entry = getLogEntry(start_index); prev_entry == null || (prev_entry.term != prev_term); prev_entry = getLogEntry(--start_index)) {
            if (start_index == firstAppended) break;
        }
        return start_index;
    }

    private LogEntry getLogEntry(int index) {
        byte[] entryBytes = db.get(fromIntToByteArray(index));
        LogEntry entry = null;
        try {
            if (entryBytes != null) entry =Util.streamableFromByteBuffer(LogEntry.class, entryBytes);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return entry;
    }

    private void appendEntryIfAbsent(int index, LogEntry entry, WriteBatch batch) throws Exception {
        if (db.get(fromIntToByteArray(index))!= null) {
            log.trace("Entry %d: %s can't be appended, index already present", index, entry);
            throw new IllegalStateException("Entry at index " + index + " already exists");
        } else {
            appendEntry(index, entry, batch);
        }
    }

    private void appendEntry(int index, LogEntry entry, WriteBatch batch) throws Exception {
        log.trace("Appending entry %d: %s", index, entry);
        batch.put(fromIntToByteArray(index), Util.streamableToByteBuffer(entry));
    }


    private void updateCurrentTerm(int index, WriteBatch batch) {
        currentTerm=index;
        log.trace("Updating currentTerm: %d", index);
        batch.put(CURRENTTERM, fromIntToByteArray(currentTerm));
    }

    private void updateLastAppended(int index, WriteBatch batch) {
        lastAppended= index;
        log.trace("Updating lastAppended: %d", index);
        batch.put(LASTAPPENDED, fromIntToByteArray(lastAppended));
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
            db.write(batch);
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

        assert (firstAppended == loggedFirstAppended);
        assert (lastAppended == loggedLastAppended);
        assert (currentTerm == loggedCurrentTerm);
        assert (commitIndex == loggedCommitIndex);
        if (votedFor != null) {
            assert (votedFor.equals(loggedVotedForAddress));
        }

        LogEntry lastAppendedEntry = getLogEntry(lastAppended);
        assert (lastAppendedEntry==null || lastAppendedEntry.term == currentTerm);

    }

}
