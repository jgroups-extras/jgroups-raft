package org.jgroups.protocols.raft;

import org.apache.commons.io.FileUtils;
import org.iq80.leveldb.*;
import org.jgroups.Address;
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

    private static final byte[] FIRSTAPPLIED = "FA".getBytes();
    private static final byte[] LASTAPPLIED = "LA".getBytes();
    private static final byte[] CURRENTTERM = "CT".getBytes();
    private static final byte[] COMMITINDEX = "CX".getBytes();
    private static final byte[] VOTEDFOR = "VF".getBytes();

    private DB db;
    private File dbFileName;

    private Integer currentTerm = 0;
    private Address votedFor;

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

        if (isANewRAFTLog()) {
            System.out.println("LOG is new, must be initialized");
            initLogWithMetadata();
        } else {
            System.out.println("LOG is existent, must not be initialized");
            readMetadataFromLog();
        }
        checkForConsistency();

    }

    @Override
    public void close() {
        try {
            db.close();
        } catch (IOException e) {
            //@todo proper logging, etc
            e.printStackTrace();
        }
    }

    @Override
    public void delete() {
        this.close();
        try {
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
            db.put(VOTEDFOR, Util.streamableToByteBuffer(member));
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
                db.write(batch);
                index++;
            }
            catch(Exception ex) {
                ex.printStackTrace(); // todo: better error handling
            } finally {
                try {
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
                batch.delete(db.get(fromIntToByteArray(index)));
            }
            LogEntry last = getLogEntry(upto_index);

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
                batch.delete(db.get(fromIntToByteArray(index)));
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

        System.out.println("-----------------");
        System.out.println("RAFT Log Metadata");
        System.out.println("-----------------");

        byte[] firstAppliedBytes = db.get(FIRSTAPPLIED);
        System.out.println("First Applied: " + fromByteArrayToInt(firstAppliedBytes));
        byte[] lastAppliedBytes = db.get(LASTAPPLIED);
        System.out.println("Last Applied: " + fromByteArrayToInt(lastAppliedBytes));
        byte[] currentTermBytes = db.get(CURRENTTERM);
        System.out.println("Current Term: " + fromByteArrayToInt(currentTermBytes));
        byte[] commitIndexBytes = db.get(COMMITINDEX);
        System.out.println("Commit Index: " + fromByteArrayToInt(commitIndexBytes));
        //Address votedFor = (Address)Util.streamableFromByteBuffer(Address.class, db.get(VOTEDFOR));
        //System.out.println("Voted for: " + votedFor);
    }

    private boolean checkIfPreviousEntryHasDifferentTerm(int prev_index, int prev_term) {
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
            throw new IllegalStateException("Entry at index " + index + " already exists");
        } else {
            appendEntry(index, entry, batch);
        }
    }

    private void appendEntry(int index, LogEntry entry, WriteBatch batch) throws Exception {
        batch.put(fromIntToByteArray(index), Util.streamableToByteBuffer(entry));
    }


    private void updateCurrentTerm(int index, WriteBatch batch) {
        currentTerm=index;
        batch.put(CURRENTTERM, fromIntToByteArray(currentTerm));
    }

    private void updateLastApplied(int index, WriteBatch batch) {
        lastApplied = index;
        batch.put(LASTAPPLIED, fromIntToByteArray(lastApplied));
    }

    private void updateFirstApplied(int index, WriteBatch batch) {
        if (firstApplied == -1) {
            firstApplied = index;
            batch.put(FIRSTAPPLIED, fromIntToByteArray(firstApplied));
        }
    }

    private boolean isANewRAFTLog() {
        return (db.get(FIRSTAPPLIED) == null);
    }

    private void initLogWithMetadata() {
        WriteBatch batch = db.createWriteBatch();
        try {
            batch.put(FIRSTAPPLIED, fromIntToByteArray(-1));
            batch.put(LASTAPPLIED, fromIntToByteArray(0));
            batch.put(CURRENTTERM, fromIntToByteArray(0));
            batch.put(COMMITINDEX, fromIntToByteArray(0));
            //batch.put(VOTEDFOR, Util.streamableToByteBuffer(Util.createRandomAddress("")));
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
        //votedFor = (Address)Util.streamableFromByteBuffer(Address.class, db.get(VOTEDFOR));

    }

    private void checkForConsistency() throws Exception {

        int loggedFirstApplied = fromByteArrayToInt(db.get(FIRSTAPPLIED));
        int loggedLastApplied = fromByteArrayToInt(db.get(LASTAPPLIED));
        int loggedCurrentTerm = fromByteArrayToInt(db.get(CURRENTTERM));
        int loggedCommitIndex = fromByteArrayToInt(db.get(COMMITINDEX));

        assert (firstApplied == loggedFirstApplied);
        assert (lastApplied == loggedLastApplied);
        assert (currentTerm == loggedCurrentTerm);
        assert (commitIndex == loggedCommitIndex);

        //check if last appended entry contains the correct term
        //LogEntry lastAppendedEntry = getLogEntry(lastApplied);
        //assert (lastAppendedEntry.term == currentTerm);

    }

}
