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

        //String dir=Util.checkForMac()? File.separator + "tmp" : System.getProperty("java.io.tmpdir", File.separator + "tmp");
        //filename=dir + File.separator + log_name;

        this.dbFileName = new File(log_name);
        try {
            db = factory.open(dbFileName, options);
            try (DBIterator iterator = db.iterator()) {
                iterator.seekToFirst();
                if (!iterator.hasNext()) {
                    WriteBatch batch = db.createWriteBatch();

                    try {
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
            }


        } catch (IOException e) {
            //@todo proper logging, etc
            e.printStackTrace();
        }

        initCommitAndTermFromLog();
        //checkForConsistency();

    }

    private void initCommitAndTermFromLog() throws Exception {

        currentTerm = fromByteArrayToInt(db.get(CURRENTTERM));
        commitIndex = fromByteArrayToInt(db.get(COMMITINDEX));
        lastApplied = fromByteArrayToInt(db.get(LASTAPPLIED));
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
    public int first() {

        DBIterator iterator = db.iterator();
        try {
            iterator.seek(VOTEDFOR);
            //iterator.next();
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
    public void append(int index, LogEntry... entries) {
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
