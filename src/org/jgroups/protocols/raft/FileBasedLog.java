package org.jgroups.protocols.raft;

import org.jgroups.Address;
import org.jgroups.raft.filelog.LogEntryStorage;
import org.jgroups.raft.filelog.MetadataStorage;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.function.ObjLongConsumer;

/**
 * A {@link Log} implementation stored in a file.
 *
 * @author Pedro Ruivo
 * @since 0.5.4
 */
public class FileBasedLog implements Log {

   private static final String SNAPSHOT_FILE_NAME = "state_snapshot.raft";

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
         Files.deleteIfExists(snapshotPath());
         if (logDir != null) {
            logDir.delete(); // must be empty in order to be deleted
            logDir = null;
         }
      } catch (IOException e) {
         throw new IllegalStateException(e);
      }
   }

   @Override
   public long currentTerm() {
      return currentTerm;
   }

   public Log _currentTerm(int new_term) {
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
   public Log currentTerm(long new_term) {
      return _currentTerm((int)new_term);
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
   public long commitIndex() {
      return commitIndex;
   }

   public Log _commitIndex(int new_index) {
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
   public Log commitIndex(long new_index) {
      return _commitIndex((int)new_index);
   }

   @Override
   public long firstAppended() {
      return checkLogEntryStorageStarted().getFirstAppended();
   }

   @Override
   public long lastAppended() {
      return checkLogEntryStorageStarted().getLastAppended();
   }

   public void setSnapshot(ByteBuffer sn) {
      Path snapshotPath = snapshotPath();
      try {
         if (Files.exists(snapshotPath)) {
            // write to temporary file first
            Path tmp = Files.createTempFile(logDir.toPath(), null, null);
            writeSnapshot(sn, tmp);
            // do we need atomic move?
            Files.move(tmp, snapshotPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
         } else {
            writeSnapshot(sn, snapshotPath);
         }
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
   }

   public ByteBuffer getSnapshot() {
      Path snapshotPath = snapshotPath();
      if (Files.exists(snapshotPath)) {
         try {
            return ByteBuffer.wrap(Files.readAllBytes(snapshotPath));
         } catch (IOException e) {
            throw new RuntimeException(e);
         }
      }
      return null;
   }

   private Path snapshotPath() {
      return logDir.toPath().resolve(SNAPSHOT_FILE_NAME);
   }

   private static void writeSnapshot(ByteBuffer snapshot, Path path) throws IOException {
      try(ByteChannel ch=Files.newByteChannel(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
         ch.write(snapshot);
      }
   }

   public int _append(int index, LogEntries entries) {
      assert index > firstAppended();
      assert index > commitIndex();
      LogEntryStorage storage = checkLogEntryStorageStarted();
      try {
         int term = storage.write(index, entries);
         if (currentTerm != term) {
            currentTerm(term);
         }
      } catch (IOException e) {
         e.printStackTrace();
      }
      return (int)lastAppended();
   }

   @Override
   public long append(long index, LogEntries entries) {
      return _append((int)index, entries);
   }

   public LogEntry _get(int index) {
      try {
         return checkLogEntryStorageStarted().getLogEntry(index);
      } catch (IOException e) {
         return null;
      }
   }

   @Override
   public LogEntry get(long index) {
      return _get((int)index);
   }


   public void _truncate(int index_exclusive) {
      assert index_exclusive >= firstAppended();
      try {
         checkLogEntryStorageStarted().removeOld(index_exclusive);
      } catch (IOException e) {
         e.printStackTrace();
      }
   }

   @Override
   public void truncate(long index_exclusive) {
      _truncate((int)index_exclusive);
   }

   public void _reinitializeTo(int index, LogEntry entry) {
      try {
         MetadataStorage metadataStorage = checkMetadataStarted();
         checkLogEntryStorageStarted().reinitializeTo(index, entry);

         // update commit index
         metadataStorage.setCommitIndex(index);
         commitIndex = index;

         // update term
         if (currentTerm != entry.term()) {
            metadataStorage.setCurrentTerm((int)entry.term());
            currentTerm =(int)entry.term();
         }
      } catch (IOException e) {
         e.printStackTrace();
      }
   }

   @Override
   public void reinitializeTo(long index, LogEntry entry) {
      _reinitializeTo((int)index, entry);
   }

   public void _deleteAllEntriesStartingFrom(int start_index) {
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
   public void deleteAllEntriesStartingFrom(long start_index) {
      _deleteAllEntriesStartingFrom((int)start_index);
   }


   public void _forEach(ObjLongConsumer<LogEntry> function, int start_index, int end_index) {
      try {
         checkLogEntryStorageStarted().forEach(function, start_index, end_index);
      } catch (IOException e) {
         e.printStackTrace();
      }
   }

   @Override
   public void forEach(ObjLongConsumer<LogEntry> function, long start_index, long end_index) {
      _forEach(function, (int)start_index, (int)end_index);
   }

   public void _forEach(ObjLongConsumer<LogEntry> function) {
      forEach(function, firstAppended(), lastAppended());
   }

   @Override
   public void forEach(ObjLongConsumer<LogEntry> function) {
      _forEach(function);
   }


   public long sizeInBytes() {
      return logEntryStorage.getCachedFileSize();
   }

   @Override
   public String toString() {
      if (logEntryStorage == null)
         return "FileLog: <not initialized yet>";
      return String.format("FileLog: first=%d, commit=%d, last-appended=%d, term=%d (size=%d)",
            firstAppended(), commitIndex(), lastAppended(), currentTerm, size());
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
