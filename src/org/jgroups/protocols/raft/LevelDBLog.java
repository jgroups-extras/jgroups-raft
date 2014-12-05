package org.jgroups.protocols.raft;

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

    private static final byte[] LASTAPPLIED = "LA".getBytes();
    private static final byte[] CURRENTTERM = "CT".getBytes();
    private static final byte[] COMMITINDEX = "CX".getBytes();
    private static final byte[] VOTEDFOR = "VF".getBytes();

    private DB db;
    private Integer currentTerm = 0;
    private Integer commitIndex = 0;
    private Integer lastApplied = 0;

    @Override
    public void init(String log_name, Map<String,String> args) throws Exception {

        Logger debugLogger = new Logger() {
            public void log(String message) {
                System.out.println(message);
            }
        };

        Options options = new Options();
        options.createIfMissing(true);

        // to help debugging
        options.logger(debugLogger);

        try {
            db = factory.open(new File(log_name + ".db"), options);
        } catch (IOException e) {
            //@todo proper logging, etc
            e.printStackTrace();
        }

        initCommitAndTermFromLog();
        checkForConsistency();

    }

    private void initCommitAndTermFromLog() throws Exception {

        currentTerm = fromByteArrayToInt(db.get(CURRENTTERM));
        commitIndex = fromByteArrayToInt(db.get(COMMITINDEX));
        lastApplied = fromByteArrayToInt(db.get(LASTAPPLIED));

    }


    private void checkForConsistency() throws Exception {
        DBIterator iterator = db.iterator();
        try {
            iterator.seekToLast();
            byte[] keyBytes = iterator.peekNext().getKey();
            int commitIndexInLog = fromByteArrayToInt(keyBytes);
            assert (commitIndexInLog == commitIndex);

            //get the term from the serialized logentry
            byte[] entryBytes = iterator.peekNext().getValue();
            LogEntry entry =(LogEntry)Util.streamableFromByteBuffer(LogEntry.class, entryBytes);
            int currentTermInLog = entry != null ? entry.term : 0;
            assert(currentTermInLog == currentTerm);

        } finally {
            try {
                iterator.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


    }

    @Override
    public void destroy() {
        try {
            db.close();
        } catch (IOException e) {
            //@todo proper logging, etc
            e.printStackTrace();
        }
    }

    @Override
    public int currentTerm() {
        return currentTerm;
    }

    @Override
    public Log currentTerm(int new_term) {
        return null;
    }

    @Override
    public Address votedFor() {
        return null;
    }

    @Override
    public Log votedFor(Address member) {
        return null;
    }

    @Override
    public int first() {

        DBIterator iterator = db.iterator();
        try {
            iterator.seekToFirst();
            byte[] keyBytes = iterator.peekNext().getKey();
            return new Integer(asString(keyBytes));
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
        return null;
    }

    @Override
    public int lastApplied() {
        return lastApplied;
    }

    @Override
    public AppendResult append(int prev_index, int prev_term, LogEntry[] entries) {

        WriteBatch batch = db.createWriteBatch();

        for (LogEntry entry : entries) {
            try {
                byte[] lastAppliedBytes = fromIntToByteArray(lastApplied);
                batch.put(lastAppliedBytes, Util.streamableToByteBuffer(entry));
                currentTerm=entry.term;
                batch.put(LASTAPPLIED, lastAppliedBytes);
                batch.put(CURRENTTERM, fromIntToByteArray(currentTerm));
                lastApplied++;
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

        return new AppendResult(true, lastApplied);

    }

    @Override
    public void forEach(Function function, int start_index, int end_index) {

        DBIterator iterator = db.iterator();

        int index = start_index;
        try {
            for(iterator.seek(bytes(Integer.toString(start_index))); iterator.hasNext() && (index < end_index); iterator.next()) {
                index++;
                String key = asString(iterator.peekNext().getKey());
                String value = asString(iterator.peekNext().getValue());
                System.out.println(key + ":" + value);
                //function.apply(...)
            }
        } finally {
            try {
                iterator.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    @Override
    public void forEach(Function function) {

        this.forEach(function, 1, Integer.MAX_VALUE);

    }
}
