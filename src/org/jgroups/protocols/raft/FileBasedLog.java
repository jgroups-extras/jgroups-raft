package org.jgroups.protocols.raft;

import org.apache.commons.io.FileUtils;
import org.jgroups.Address;
import org.jgroups.raft.filelog.LogEntryStorage;
import org.jgroups.raft.filelog.MetadataStorage;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.function.ObjIntConsumer;

/**
 * A {@link Log} implementation stored in a file.
 *
 * @author Pedro Ruivo
 * @since 0.5.4
 */
public class FileBasedLog implements Log {

   private final Object metadataLock = new Object();

   private File logDir;
   private Address votedFor;
   private int commitIndex;
   private int currentTerm;

   private static final boolean DEFAULT_FSYNC = true;
   private boolean fsync = DEFAULT_FSYNC;
   private MetadataStorage metadataStorage;
   private LogEntryStorage logEntryStorage;

   @Override
   public void init(String log_name, Map<String, String> args) throws Exception {
      logDir = new File(log_name);
      if (!logDir.exists() && !logDir.mkdirs()) {
         throw new IllegalArgumentException("Unable to create directory " + logDir.getAbsolutePath());
      } else if (!logDir.isDirectory()) {
         throw new IllegalArgumentException("File " + logDir.getAbsolutePath() + " is not a directory!");
      }

      metadataStorage = new MetadataStorage(logDir, fsync);
      metadataStorage.open();

      logEntryStorage = new LogEntryStorage(logDir, fsync);
      logEntryStorage.open();

      commitIndex = metadataStorage.getCommitIndex();
      currentTerm = metadataStorage.getCurrentTerm();
      votedFor = metadataStorage.getVotedFor();

      logEntryStorage.reload(commitIndex);
   }

   @Override
   public Log useFsync(boolean value) {
      this.fsync = value;
      if (metadataStorage != null) {
         metadataStorage.useFsync(value);
      }
      if (logEntryStorage != null) {
         logEntryStorage.useFsync(value);
      }
      return this;
   }

   @Override
   public boolean useFsync() {
      return fsync;
   }

   @Override
   public void close() {
      try {
         MetadataStorage metadataStorage = this.metadataStorage;
         if (metadataStorage != null) {
            metadataStorage.close();
         }
         LogEntryStorage entryStorage = logEntryStorage;
         if (entryStorage != null) {
            entryStorage.close();
         }
      } catch (IOException e) {
         throw new IllegalStateException(e);
      }
   }

   @Override
   public void delete() {
      try {
         MetadataStorage storage = metadataStorage;
         if (storage != null) {
            storage.delete();
         }
         LogEntryStorage entryStorage = logEntryStorage;
         if (entryStorage != null) {
            entryStorage.delete();
         }
         if (logDir != null) {
            FileUtils.deleteDirectory(logDir);
            logDir = null;
         }
      } catch (IOException e) {
         throw new IllegalStateException(e);
      }
   }

   @Override
   public int currentTerm() {
      return currentTerm;
   }

   @Override
   public Log currentTerm(int new_term) {
      //assert new_term >= currentTerm;
      try {
         checkMetadataStarted().setCurrentTerm(new_term);
         currentTerm = new_term;
         return this;
      } catch (IOException e) {
         throw new IllegalStateException(e);
      }
   }

   @Override
   public Address votedFor() {
      return votedFor;
   }

   @Override
   public Log votedFor(Address member) {
      try {
         checkMetadataStarted().setVotedFor(member);
         votedFor = member;
         return this;
      } catch (IOException e) {
         throw new IllegalStateException(e);
      }
   }

   @Override
   public int commitIndex() {
      return commitIndex;
   }

   @Override
   public Log commitIndex(int new_index) {
      assert new_index >= commitIndex;
      try {
         checkMetadataStarted().setCommitIndex(new_index);
         commitIndex = new_index;
         return this;
      } catch (IOException e) {
         throw new IllegalStateException();
      }
   }

   @Override
   public int firstAppended() {
      return checkLogEntryStorageStarted().getFirstAppended();
   }

   @Override
   public int lastAppended() {
      return checkLogEntryStorageStarted().getLastAppended();
   }

   @Override
   public void append(int index, boolean overwrite, LogEntry... entries) {
      assert index > firstAppended();
      assert index > commitIndex();
      LogEntryStorage storage = checkLogEntryStorageStarted();
      try {
         int term = storage.write(index, entries, overwrite);
         if (currentTerm != term) {
            currentTerm(term);
         }
      } catch (IOException e) {
         e.printStackTrace();
      }
   }

   @Override
   public LogEntry get(int index) {
      try {
         return checkLogEntryStorageStarted().getLogEntry(index);
      } catch (IOException e) {
         return null;
      }
   }

   @Override
   public void truncate(int index) {
      assert index >= firstAppended();
      try {
         checkLogEntryStorageStarted().removeOld(index);
      } catch (IOException e) {
         e.printStackTrace();
      }
   }

   @Override
   public void deleteAllEntriesStartingFrom(int start_index) {
      assert start_index > commitIndex; // can we delete committed entries!? See org.jgroups.tests.LogTest.testDeleteEntriesFromFirst
      assert start_index >= firstAppended();

      LogEntryStorage storage = checkLogEntryStorageStarted();
      try {
         currentTerm(storage.removeNew(start_index));
      } catch (IOException e) {
         e.printStackTrace();
      }
   }


   @Override
   public void forEach(ObjIntConsumer<LogEntry> function, int start_index, int end_index) {
      try {
         checkLogEntryStorageStarted().forEach(function, start_index, end_index);
      } catch (IOException e) {
         e.printStackTrace();
      }
   }

   @Override
   public void forEach(ObjIntConsumer<LogEntry> function) {
      forEach(function, firstAppended(), lastAppended());
   }

   private MetadataStorage checkMetadataStarted() {
      MetadataStorage storage = metadataStorage;
      if (storage == null) {
         throw new IllegalStateException("Log not initialized");
      }
      return storage;
   }

   private LogEntryStorage checkLogEntryStorageStarted() {
      LogEntryStorage storage = logEntryStorage;
      if (storage == null) {
         throw new IllegalStateException("Log not initialized");
      }
      return storage;
   }
}
