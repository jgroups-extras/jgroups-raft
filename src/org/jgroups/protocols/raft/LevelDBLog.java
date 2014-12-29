package org.jgroups.protocols.raft;

import org.apache.commons.io.FileUtils;
import org.iq80.leveldb.*;
import org.jgroups.Address;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.Util;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static org.fusesource.leveldbjni.JniDBFactory.*;
import static org.jgroups.util.IntegerHelper.fromByteArrayToInt;
import static org.jgroups.util.IntegerHelper.fromIntToByteArray;

/**
 * Created by ugol on 03/12/14.
 */

public class LevelDBLog implements Log {

    protected final org.jgroups.logging.Log log= LogFactory.getLog(this.getClass());

    private static final byte[] FIRSTAPPLIED = "FA".getBytes();
    private static final byte[] LASTAPPLIED = "LA".getBytes();
    private static final byte[] CURRENTTERM = "CT".getBytes();
    private static final byte[] COMMITINDEX = "CX".getBytes();
    private static final byte[] VOTEDFOR = "VF".getBytes();

    private DB db;
    private File dbFileName;

    private Integer currentTerm = 0;
    private Address votedFor = null;

    private Integer commitIndex = 0;
    private Integer lastApplied = 0;
    private Integer firstApplied = -1;

    @Override
    public void init(String log_name, Map<String,String> args) throws Exception {

        Options options = new Options();
        options.createIfMissing(true);

        //String dir=Util.checkForMac()? File.separator + "tmp" : System.getProperty("java.io.tmpdir", File.separator + "tmp");
        //filename=dir + File.separator + log_name;

        this.dbFileName = new File(log_name);
        db = factory.open(dbFileName, options);
        log.info("LOG " + db + " is open");

        if (isANewRAFTLog()) {
            log.info("LOG " + dbFileName + " is new, must be initialized");
            initLogWithMetadata();
        } else {
            log.info("LOG " + dbFileName + " is existent, must not be initialized");
            readMetadataFromLog();
        }

        if (log.isDebugEnabled()) log.debug("Checking for consistency");
        checkForConsistency();

    }

    @Override
    public void close() {
        try {
            if (log.isDebugEnabled()) log.debug("Closing DB: " + db);

            if (db!= null) db.close();
            currentTerm = 0;
            votedFor = null;
            commitIndex = 0;
            lastApplied = 0;
            firstApplied = -1;
        } catch (IOException e) {
            //@todo proper logging, etc
            e.printStackTrace();
        }
    }

    @Override
    public void delete() {
        this.close();
        try {
            if (log.isDebugEnabled()) log.debug("Deleting DB directory: " + dbFileName);

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
        if (log.isDebugEnabled()) log.debug("Updating current term: " + currentTerm);
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
            if (log.isDebugEnabled()) log.debug("Updating Voted for: " + votedFor);
            db.put(VOTEDFOR, Util.objectToByteBuffer(member));
        } catch (Exception e) {
            e.printStackTrace(); // todo: better error handling
        }
        return this;
    }

    @Override
    public int firstApplied() {
        return firstApplied;
    }

    @Override
    public int commitIndex() {
        return commitIndex;
    }

    @Override
    public Log commitIndex(int new_index) {
        commitIndex = new_index;
        if (log.isDebugEnabled()) log.debug("Updating commit index: " + commitIndex);
        db.put(COMMITINDEX, fromIntToByteArray(commitIndex));
        return this;
    }

    @Override
    public int lastApplied() {
        return lastApplied;
    }

    @Override
    public void append(int index, boolean overwrite, LogEntry... entries) {
        WriteBatch batch = db.createWriteBatch();

        if (log.isDebugEnabled()) log.debug("Appending " + entries.length + " entries");
        for (LogEntry entry : entries) {
            try {
                updateFirstApplied(index, batch);

                if (overwrite) {
                    appendEntry(index, entry, batch);
                } else {
                    appendEntryIfAbsent(index, entry, batch);
                }

                updateLastApplied(index, batch);
                updateCurrentTerm(entry.term, batch);

                if (log.isDebugEnabled()) log.debug("Flushing batch to DB: " + batch);
                db.write(batch);
                index++;
            }
            catch(Exception ex) {
                ex.printStackTrace(); // todo: better error handling
            } finally {
                try {
                    if (log.isDebugEnabled()) log.debug("Closing batch: " + batch);
                    batch.close();
                } catch (IOException e) {
                    e.printStackTrace(); // todo: better error handling
                }
            }
        }
    }

    @Override
    public AppendResult append(int prev_index, int prev_term, LogEntry[] entries) {

        if (checkIfPreviousEntryHasDifferentTerm(prev_index, prev_term)) {
            return new AppendResult(false, prev_index);
        }
        append(prev_index+1, true, entries);
        return new AppendResult(true, lastApplied);

        /*
        // @todo wrong impl, see paper
        int index = findIndexWithTerm(prev_index, prev_term);
        if (index != prev_index) {
            return new AppendResult(false, index);
        } else {
            append(index+1, true, entries);
            return new AppendResult(true, lastApplied);
        }
        */
    }

    @Override
    public LogEntry get(int index) {
        return getLogEntry(index);
    }

    @Override
    public void forEach(Function function, int start_index, int end_index) {

        start_index = Math.max(start_index, firstApplied);
        end_index = Math.min(end_index, lastApplied);

        for (int i=start_index; i<=end_index; i++) {
            LogEntry entry = getLogEntry(i);
            function.apply(i, entry.term, entry.command, entry.offset, entry.length);
        }

    }

    @Override
    public void forEach(Function function) {

        if (firstApplied == -1) {
            return;
        }
        this.forEach(function, firstApplied, lastApplied);

    }

    @Override
    public void truncate(int upto_index) {
        if ((upto_index< firstApplied) || (upto_index>lastApplied)) {
            //@todo wrong index, must throw runtime exception
            return;
        }

        WriteBatch batch=null;
        try {
            batch = db.createWriteBatch();
            for (int index = firstApplied; index < upto_index; index++) {
                batch.delete(fromIntToByteArray(index));
            }

            firstApplied = upto_index;
            batch.put(FIRSTAPPLIED, fromIntToByteArray(upto_index));
            db.write(batch);
        }
        finally {
            Util.close(batch);
        }
    }

    @Override
    public void deleteAllEntriesStartingFrom(int start_index) {

        if ((start_index< firstApplied) || (start_index>lastApplied)) {
            //@todo wrong index, must throw runtime exception
            return;
        }
        WriteBatch batch=null;
        try {
            batch = db.createWriteBatch();
            for (int index = start_index; index <= lastApplied; index++) {
                batch.delete(fromIntToByteArray(index));
            }
            LogEntry last = getLogEntry(start_index-1);

            if (last == null) {
                updateCurrentTerm(0, batch);
            } else {
                updateCurrentTerm(last.term, batch);
            }
            updateLastApplied(start_index - 1, batch);
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

        byte[] firstAppliedBytes = db.get(FIRSTAPPLIED);
        log.info("First Applied: " + fromByteArrayToInt(firstAppliedBytes));
        byte[] lastAppliedBytes = db.get(LASTAPPLIED);
        log.info("Last Applied: " + fromByteArrayToInt(lastAppliedBytes));
        byte[] currentTermBytes = db.get(CURRENTTERM);
        log.info("Current Term: " + fromByteArrayToInt(currentTermBytes));
        byte[] commitIndexBytes = db.get(COMMITINDEX);
        log.info("Commit Index: " + fromByteArrayToInt(commitIndexBytes));
        Address votedFor = (Address)Util.objectFromByteBuffer(db.get(VOTEDFOR));
        log.info("Voted for: " + votedFor);
    }

    private boolean checkIfPreviousEntryHasDifferentTerm(int prev_index, int prev_term) {

        if (log.isDebugEnabled()) log.debug("Checking term (" + prev_term + ") of previous entry (" + prev_index + ")");
        if(prev_index == 0) // index starts at 1
            return false;
        LogEntry prev_entry = getLogEntry(prev_index);
        return prev_entry == null || (prev_entry.term != prev_term);
    }


    private int findIndexWithTerm(int start_index, int prev_term) {

        for (LogEntry prev_entry = getLogEntry(start_index); prev_entry == null || (prev_entry.term != prev_term); prev_entry = getLogEntry(--start_index)) {
            if (start_index == firstApplied) break;
        }
        return start_index;
    }

    private LogEntry getLogEntry(int index) {
        byte[] entryBytes = db.get(fromIntToByteArray(index));
        LogEntry entry = null;
        try {
            if (log.isDebugEnabled()) log.debug("Getting Entry @ index: " + index);
            if (entryBytes != null) entry = (LogEntry) Util.streamableFromByteBuffer(LogEntry.class, entryBytes);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (log.isDebugEnabled()) log.debug("Returning Entry " + entry);
        return entry;
    }

    private void appendEntryIfAbsent(int index, LogEntry entry, WriteBatch batch) throws Exception {
        if (db.get(fromIntToByteArray(index))!= null) {
            if (log.isDebugEnabled()) log.debug("Entry " + index + ": " + entry + " can't be appended, index already present");
            throw new IllegalStateException("Entry at index " + index + " already exists");
        } else {
            appendEntry(index, entry, batch);
        }
    }

    private void appendEntry(int index, LogEntry entry, WriteBatch batch) throws Exception {
        if (log.isDebugEnabled()) log.debug("Appending entry " + index + ": " + entry);
        batch.put(fromIntToByteArray(index), Util.streamableToByteBuffer(entry));
    }


    private void updateCurrentTerm(int index, WriteBatch batch) {
        currentTerm=index;
        if (log.isDebugEnabled()) log.debug("Updating currentTerm: " + index);
        batch.put(CURRENTTERM, fromIntToByteArray(currentTerm));
    }

    private void updateLastApplied(int index, WriteBatch batch) {
        lastApplied = index;
        if (log.isDebugEnabled()) log.debug("Updating lastApplied: " + index);
        batch.put(LASTAPPLIED, fromIntToByteArray(lastApplied));
    }

    private void updateFirstApplied(int index, WriteBatch batch) {
        if (firstApplied == -1) {
            firstApplied = index;
            if (log.isDebugEnabled()) log.debug("Updating firstApplied: " + index);
            batch.put(FIRSTAPPLIED, fromIntToByteArray(firstApplied));
        }
    }

    private boolean isANewRAFTLog() {
        if (log.isDebugEnabled()) log.debug("Check if log is new");
        return (db.get(FIRSTAPPLIED) == null);
    }

    private void initLogWithMetadata() {

        if (log.isDebugEnabled()) log.debug("Initializing log with empty Metadata");
        WriteBatch batch = db.createWriteBatch();
        try {
            batch.put(FIRSTAPPLIED, fromIntToByteArray(-1));
            batch.put(LASTAPPLIED, fromIntToByteArray(0));
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

        firstApplied = fromByteArrayToInt(db.get(FIRSTAPPLIED));
        if (log.isDebugEnabled()) log.debug("FirstApplied: " + firstApplied);

        lastApplied = fromByteArrayToInt(db.get(LASTAPPLIED));
        if (log.isDebugEnabled()) log.debug("LastApplied: " + lastApplied);

        currentTerm = fromByteArrayToInt(db.get(CURRENTTERM));
        if (log.isDebugEnabled()) log.debug("CurrentTerm: " + currentTerm);

        commitIndex = fromByteArrayToInt(db.get(COMMITINDEX));
        if (log.isDebugEnabled()) log.debug("CommitIndex: " + commitIndex);

        votedFor = (Address)Util.objectFromByteBuffer(db.get(VOTEDFOR));
        if (log.isDebugEnabled()) log.debug("VotedFor: " + votedFor);

    }

    private void checkForConsistency() throws Exception {

        int loggedFirstApplied = fromByteArrayToInt(db.get(FIRSTAPPLIED));
        if (log.isDebugEnabled()) log.debug("FirstApplied in DB is: " + loggedFirstApplied);

        int loggedLastApplied = fromByteArrayToInt(db.get(LASTAPPLIED));
        if (log.isDebugEnabled()) log.debug("LastApplied in DB is: " + loggedLastApplied);

        int loggedCurrentTerm = fromByteArrayToInt(db.get(CURRENTTERM));
        if (log.isDebugEnabled()) log.debug("CurrentTerm in DB is: " + loggedCurrentTerm);

        int loggedCommitIndex = fromByteArrayToInt(db.get(COMMITINDEX));
        if (log.isDebugEnabled()) log.debug("CommitIndex in DB is: " + loggedCommitIndex);

        Address loggedVotedForAddress = (Address)Util.objectFromByteBuffer(db.get(VOTEDFOR));
        if (log.isDebugEnabled()) log.debug("VotedFor in DB is: " + loggedVotedForAddress);

        assert (firstApplied == loggedFirstApplied);
        assert (lastApplied == loggedLastApplied);
        assert (currentTerm == loggedCurrentTerm);
        assert (commitIndex == loggedCommitIndex);
        if (votedFor != null) {
            assert (votedFor.equals(loggedVotedForAddress));
        }

        LogEntry lastAppendedEntry = getLogEntry(lastApplied);
        if (log.isDebugEnabled()) log.debug("Last appended entry: " + lastAppendedEntry);
        assert (lastAppendedEntry==null || lastAppendedEntry.term == currentTerm);
        log.info("End of consistency check");

    }

}
