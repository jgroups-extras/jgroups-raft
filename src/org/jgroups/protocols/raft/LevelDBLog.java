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
 * Implementation of ${link #Log}
 * @author Ugo Landini
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
    private Integer firstApplied = 0;

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
            lastApplied = 0;
            firstApplied = 0;
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
        log.trace("Updating commit index: %d", commitIndex);
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

        log.trace("Appending %d entries", entries.length);
        try {
            for(LogEntry entry : entries) {
                if(overwrite) {
                    appendEntry(index, entry, batch);
                }
                else {
                    appendEntryIfAbsent(index, entry, batch);
                }

                updateLastApplied(index, batch);
                updateCurrentTerm(entry.term, batch);

                log.trace("Flushing batch to DB: %s", batch);
                db.write(batch);
                index++;
            }
        }
        catch(Exception ex) {
            ex.printStackTrace(); // todo: better error handling
        }
        finally {
            log.trace("Closing batch: %s", batch);
            Util.close(batch);
        }
    }

    @Override
    public LogEntry get(int index) {
        return getLogEntry(index);
    }

    @Override
    public void forEach(Function function, int start_index, int end_index) {
        start_index = Math.max(start_index, Math.max(firstApplied,1));
        end_index = Math.min(end_index, lastApplied);

        for (int i=start_index; i<=end_index; i++) {
            LogEntry entry = getLogEntry(i);
            if(!function.apply(i, entry.term, entry.command, entry.offset, entry.length, entry.internal))
                break;
        }

    }

    @Override
    public void forEach(Function function) {
        this.forEach(function, Math.max(1, firstApplied), lastApplied);
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
            if(commitIndex > lastApplied)
                commitIndex(lastApplied);
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

    @Override
    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append("firstApplied=").append(firstApplied).append(", lastApplied=").append(lastApplied)
          .append(", commitIndex=").append(commitIndex).append(", currentTerm=").append(currentTerm);
        return sb.toString();
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
            if (start_index == firstApplied) break;
        }
        return start_index;
    }

    private LogEntry getLogEntry(int index) {
        byte[] entryBytes = db.get(fromIntToByteArray(index));
        LogEntry entry = null;
        try {
            if (entryBytes != null) entry = (LogEntry) Util.streamableFromByteBuffer(LogEntry.class, entryBytes);
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

    private void updateLastApplied(int index, WriteBatch batch) {
        lastApplied = index;
        log.trace("Updating lastApplied: %d", index);
        batch.put(LASTAPPLIED, fromIntToByteArray(lastApplied));
    }

  /*  private void updateFirstApplied(int index, WriteBatch batch) {
        if (firstApplied == 0) {
            firstApplied = index;
            log.trace("Updating firstApplied: %d", index);
            batch.put(FIRSTAPPLIED, fromIntToByteArray(firstApplied));
        }
    }*/

    private boolean isANewRAFTLog() {
        return (db.get(FIRSTAPPLIED) == null);
    }

    private void initLogWithMetadata() {

        log.debug("Initializing log with empty Metadata");
        WriteBatch batch = db.createWriteBatch();
        try {
            batch.put(FIRSTAPPLIED, fromIntToByteArray(0));
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
        lastApplied = fromByteArrayToInt(db.get(LASTAPPLIED));
        currentTerm = fromByteArrayToInt(db.get(CURRENTTERM));
        commitIndex = fromByteArrayToInt(db.get(COMMITINDEX));
        votedFor = (Address)Util.objectFromByteBuffer(db.get(VOTEDFOR));
        log.debug("read metadata from log: firstApplied=%d, lastApplied=%d, currentTerm=%d, commitIndex=%d, votedFor=%s",
                  firstApplied, lastApplied, currentTerm, commitIndex, votedFor);
    }

    private void checkForConsistency() throws Exception {

        int loggedFirstApplied = fromByteArrayToInt(db.get(FIRSTAPPLIED));
        log.trace("FirstApplied in DB is: %d", loggedFirstApplied);

        int loggedLastApplied = fromByteArrayToInt(db.get(LASTAPPLIED));
        log.trace("LastApplied in DB is: %d", loggedLastApplied);

        int loggedCurrentTerm = fromByteArrayToInt(db.get(CURRENTTERM));
        log.trace("CurrentTerm in DB is: %d", loggedCurrentTerm);

        int loggedCommitIndex = fromByteArrayToInt(db.get(COMMITINDEX));
        log.trace("CommitIndex in DB is: %d", loggedCommitIndex);

        Address loggedVotedForAddress = (Address)Util.objectFromByteBuffer(db.get(VOTEDFOR));
        log.trace("VotedFor in DB is: %s", loggedVotedForAddress);

        assert (firstApplied == loggedFirstApplied);
        assert (lastApplied == loggedLastApplied);
        assert (currentTerm == loggedCurrentTerm);
        assert (commitIndex == loggedCommitIndex);
        if (votedFor != null) {
            assert (votedFor.equals(loggedVotedForAddress));
        }

        LogEntry lastAppendedEntry = getLogEntry(lastApplied);
        assert (lastAppendedEntry==null || lastAppendedEntry.term == currentTerm);

    }

}
