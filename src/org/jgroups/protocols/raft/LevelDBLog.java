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
            initLog();
        } else {
            System.out.println("LOG is existent, must not be initialized");
            initCommitAndTermFromLog();
            //checkForConsistency();
        }

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

        if (firstApplied == -1) {
            return firstApplied;
        }

        DBIterator iterator = db.iterator();
        try {
            iterator.seek(FIRSTAPPLIED);
            byte[] keyBytes = iterator.peekNext().getValue();
            return fromByteArrayToInt(keyBytes);
        } finally {
            try {
                iterator.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    @Override
    public int commitIndex() {
        return commitIndex;
    }

    @Override
    public Log commitIndex(int new_index) {
        commitIndex = new_index;
        try {
            db.put(COMMITINDEX, fromIntToByteArray(commitIndex));
        } catch (Exception e) {
            e.printStackTrace(); // todo: better error handling
        }
        return this;
    }

    @Override
    public int lastApplied() {
        return lastApplied;
    }

    @Override
    public void append(int index, boolean overwrite, LogEntry... entries) {
        append(entries);

    }

    @Override
    public AppendResult append(int prev_index, int prev_term, LogEntry[] entries) {

        // consistency check
        append(entries);
        return new AppendResult(true, lastApplied);

    }

    private void append(LogEntry[] entries) {
        WriteBatch batch = db.createWriteBatch();

        for (LogEntry entry : entries) {
            try {
                lastApplied++;
                if (firstApplied == -1) {
                    firstApplied = lastApplied;
                    batch.put(FIRSTAPPLIED, fromIntToByteArray(firstApplied));
                }
                byte[] lastAppliedBytes = fromIntToByteArray(lastApplied);
                batch.put(lastAppliedBytes, Util.streamableToByteBuffer(entry));
                currentTerm=entry.term;
                batch.put(LASTAPPLIED, lastAppliedBytes);
                batch.put(CURRENTTERM, fromIntToByteArray(currentTerm));
                db.write(batch);
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
    public void forEach(Function function, int start_index, int end_index) {

        start_index = Math.max(start_index, firstApplied);
        end_index = Math.min(end_index, lastApplied);

        DBIterator iterator = db.iterator();

        for (int i=start_index; i<=end_index; i++) {
            iterator.seek(fromIntToByteArray(i));
            String key = asString(iterator.peekNext().getKey());
            String value = asString(iterator.peekNext().getValue());
            System.out.println(key + ":" + value);
            //function.apply(...)
        }

    }

    @Override
    public void forEach(Function function) {

        if (firstApplied == -1) {
            return;
        }
        this.forEach(function, firstApplied, lastApplied);

    }

    // Useful Debug methods
    public byte[] print(byte[] bytes) {
        return db.get(bytes);
    }

    public void printMetadata() {

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
        // @todo add VOTEDFOR
    }

    private boolean isANewRAFTLog() {
        return (db.get(FIRSTAPPLIED) == null);
    }

    private void initLog() {
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

    private void initCommitAndTermFromLog() throws Exception {

        firstApplied = fromByteArrayToInt(db.get(FIRSTAPPLIED));
        lastApplied = fromByteArrayToInt(db.get(LASTAPPLIED));
        currentTerm = fromByteArrayToInt(db.get(CURRENTTERM));
        commitIndex = fromByteArrayToInt(db.get(COMMITINDEX));
        //byte[] votedForBytes = db.get(VOTEDFOR);
        //votedFor = (Address)Util.streamableFromByteBuffer(Address.class, votedForBytes);

    }

    private void checkForConsistency() throws Exception {
        DBIterator iterator = db.iterator();

        try {
            iterator.seekToLast();
        } catch (java.util.NoSuchElementException nse) {
            assert (0 == commitIndex);
            assert (0 == currentTerm);
            iterator.close();
            return;
        }
        try {
            byte[] keyBytes = iterator.peekNext().getKey();
            int commitIndexInLog = fromByteArrayToInt(keyBytes);
            assert (commitIndexInLog == commitIndex);

            //get the term from the serialized logentry
            byte[] entryBytes = iterator.peekNext().getValue();
            LogEntry entry = (LogEntry) Util.streamableFromByteBuffer(LogEntry.class, entryBytes);
            int currentTermInLog = entry != null ? entry.term : 0;
            assert (currentTermInLog == currentTerm);
        }
        finally {
            try {
                iterator.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

}
