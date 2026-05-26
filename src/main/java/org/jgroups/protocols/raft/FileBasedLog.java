package org.jgroups.protocols.raft;

import org.jgroups.Address;
import org.jgroups.raft.filelog.LogDirectoryLock;
import org.jgroups.raft.filelog.LogEntryStorage;
import org.jgroups.raft.filelog.MetadataStorage;
import org.jgroups.raft.filelog.SnapshotStorage;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.ObjLongConsumer;

/**
 * A {@link Log} implementation stored in a file.
 *
 * @author Pedro Ruivo
 * @since 0.5.4
 */
public class FileBasedLog implements Log {

   private File logDir;
   private Address votedFor;
   private long commitIndex;
   private long currentTerm;

   private static final boolean DEFAULT_FSYNC = true;
   private boolean fsync = DEFAULT_FSYNC;
   private MetadataStorage metadataStorage;
   private LogEntryStorage logEntryStorage;
   private SnapshotStorage snapshotStorage;
   private LogDirectoryLock directoryLock;

   @Override
   public void init(String log_name, Map<String, String> args) throws Exception {
      logDir = new File(log_name);
      if (!logDir.exists() && !logDir.mkdirs()) {
         throw new IllegalArgumentException("Unable to create directory " + logDir.getAbsolutePath());
      } else if (!logDir.isDirectory()) {
         throw new IllegalArgumentException("File " + logDir.getAbsolutePath() + " is not a directory!");
      }

      directoryLock = new LogDirectoryLock(logDir);
      if (!directoryLock.tryAcquire()) {
         String message = String.format("Log directory %s is locked by another process. " +
                 "Another RAFT node or CLI command is currently holding the lock for the directory. " +
                 "Ensure no other process is accessing the directory before proceeding. " +
                 "Ensure that each node is configured to use a dedicated directory.", logDir.getAbsolutePath());
         throw new IOException(message);
      }

      metadataStorage = new MetadataStorage(logDir, fsync);
      metadataStorage.open();

      logEntryStorage = new LogEntryStorage(logDir, fsync);
      logEntryStorage.open();

      snapshotStorage = new SnapshotStorage(logDir);

      commitIndex = metadataStorage.getCommitIndex();
      currentTerm = metadataStorage.getCurrentTerm();
      votedFor = metadataStorage.getVotedFor();

      logEntryStorage.reload();
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
   public void close() throws IOException {
      MetadataStorage metadataStorage = this.metadataStorage;
      if (metadataStorage != null) {
         metadataStorage.close();
      }
      LogEntryStorage entryStorage = logEntryStorage;
      if (entryStorage != null) {
         entryStorage.close();
      }

      // Only release lock files after everything is flushed.
      if (directoryLock != null) {
         directoryLock.close();
      }
   }

   @Override
   public long currentTerm() {
      return currentTerm;
   }

   @Override
   public Log currentTerm(long new_term) throws IOException {
      checkMetadataStarted().setCurrentTerm(new_term);
      currentTerm = new_term;
      return this;
   }

   @Override
   public Address votedFor() {
      return votedFor;
   }

   @Override
   public Log votedFor(Address member) throws IOException {
      checkMetadataStarted().setVotedFor(member);
      votedFor = member;
      return this;
   }

   @Override
   public long commitIndex() {
      return commitIndex;
   }

   @Override
   public Log commitIndex(long new_index) throws IOException {
      assert new_index >= commitIndex;
      checkMetadataStarted().setCommitIndex(new_index);
      commitIndex = new_index;
      return this;
   }

   @Override
   public long firstAppended() {
      return checkLogEntryStorageStarted().getFirstAppended();
   }

   @Override
   public long lastAppended() {
      return checkLogEntryStorageStarted().getLastAppended();
   }

   @Override
   public void setSnapshot(ByteBuffer sn) throws IOException {
      snapshotStorage.writeSnapshot(sn);
   }

   @Override
   public ByteBuffer getSnapshot() throws IOException {
      return snapshotStorage.readSnapshot();
   }

   @Override
   public long append(long index, LogEntries entries) throws IOException {
      assert index > firstAppended();
      assert index > commitIndex();
      LogEntryStorage storage = checkLogEntryStorageStarted();
      long term = storage.write(index, entries);
      if (currentTerm != term) {
         currentTerm(term);
      }
      return lastAppended();
   }

   @Override
   public LogEntry get(long index) throws IOException {
      return checkLogEntryStorageStarted().getLogEntry(index);
   }

   @Override
   public void truncate(long index_exclusive) throws IOException {
      assert index_exclusive >= firstAppended();

      if (index_exclusive > commitIndex) {
         index_exclusive=commitIndex;
      }

      checkLogEntryStorageStarted().removeOld(index_exclusive);
   }

   @Override
   public void reinitializeTo(long index, LogEntry entry) throws IOException {
      MetadataStorage metadataStorage = checkMetadataStarted();
      checkLogEntryStorageStarted().reinitializeTo(index, entry);

      // update commit index
      metadataStorage.setCommitIndex(index);
      commitIndex = index;

      // update term
      if (currentTerm != entry.term()) {
         metadataStorage.setCurrentTerm(entry.term());
         currentTerm = entry.term();
      }
   }

   @Override
   public void deleteAllEntriesStartingFrom(long start_index) throws IOException {
      assert start_index > commitIndex; // can we delete committed entries!? See org.jgroups.tests.LogTest.testDeleteEntriesFromFirst
      assert start_index >= firstAppended();

      LogEntryStorage storage = checkLogEntryStorageStarted();
      currentTerm(storage.removeNew(start_index));
   }

   @Override
   public void forEach(ObjLongConsumer<LogEntry> function, long start_index, long end_index) throws IOException {
      checkLogEntryStorageStarted().forEach(function, start_index, end_index);
   }

   @Override
   public void forEach(ObjLongConsumer<LogEntry> function) throws IOException {
      forEach(function, firstAppended(), lastAppended());
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
