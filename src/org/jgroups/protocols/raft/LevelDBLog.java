package org.jgroups.protocols.raft;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Logger;
import org.iq80.leveldb.Options;
import org.jgroups.Address;
import org.jgroups.util.Util;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static org.fusesource.leveldbjni.JniDBFactory.*;

/**
 * Created by ugol on 03/12/14.
 */

public class LevelDBLog implements Log {

    private DB db;
    private Integer currentTerm = 0;
    private Integer commitIndex = 0;
    private Integer lastApplied = 0;

    @Override
    public void init(Map<String, String> args) throws Exception {

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
            //@todo get the name of the log file from node name
            db = factory.open(new File("leveldb-name.db"), options);
        } catch (IOException e) {
            //@todo proper logging, etc
            e.printStackTrace();
        }

        initCommitAndTermFromLog();

    }

    private void initCommitAndTermFromLog() throws Exception {
        DBIterator iterator = db.iterator();
        try {
            iterator.seekToLast();
            byte[] keyBytes = iterator.peekNext().getKey();
            commitIndex = new Integer(asString(keyBytes));

            //get the term from the serialized logentry
            byte[] entryBytes = iterator.peekNext().getValue();
            LogEntry entry =(LogEntry)Util.streamableFromByteBuffer(LogEntry.class, entryBytes);
            this.currentTerm = entry.term;

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

        //WriteBatch batch = db.createWriteBatch();

        for (LogEntry entry : entries) {
            try {
                db.put(bytes(lastApplied.toString()), Util.streamableToByteBuffer(entry));
                lastApplied++;
                currentTerm=entry.term;
            }
            catch(Exception ex) {
                ex.printStackTrace(); // todo: better error handling
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
