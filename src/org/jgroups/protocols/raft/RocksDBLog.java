package org.jgroups.protocols.raft;

import static java.util.Arrays.asList;
import static org.jgroups.raft.util.IntegerHelper.fromByteArrayToInt;
import static org.jgroups.raft.util.IntegerHelper.fromIntToByteArray;
import static org.jgroups.util.Util.objectFromByteBuffer;
import static org.jgroups.util.Util.objectToByteBuffer;
import static org.jgroups.util.Util.streamableFromByteBuffer;
import static org.jgroups.util.Util.streamableToByteBuffer;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.function.ObjIntConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.io.FileUtils;
import org.jgroups.Address;
import org.jgroups.logging.LogFactory;
import org.jgroups.raft.util.IntegerHelper;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

/**
 * RocksDB implementation of {@link Log}.
 * <p>
 * Inspired from {@link LevelDBLog}
 *
 * @author Pedro Ruivo
 */
public class RocksDBLog implements Log {

   // magic number :)
   private static final int ITERATION_BATCH_SIZE = 128;

   private final org.jgroups.logging.Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   private static final byte[] FIRSTAPPENDED = "FA".getBytes();
   private static final byte[] LASTAPPENDED = "LA".getBytes();
   private static final byte[] CURRENTTERM = "CT".getBytes();
   private static final byte[] COMMITINDEX = "CX".getBytes();
   private static final byte[] VOTEDFOR = "VF".getBytes();

   private RocksDB db;
   private File dbFileName;

   private int currentTerm = 0;
   private Address votedFor = null;

   private int commitIndex = 0;
   private int lastAppended = 0;
   private int firstAppended = 0;

   private volatile WriteOptions writeOptions;

   @Override
   public void init(String log_name, Map<String, String> args) throws Exception {
      boolean trace = log.isTraceEnabled();
      Options options = new Options();
      options.setCreateIfMissing(true);

      this.dbFileName = new File(log_name);
      db = RocksDB.open(options, dbFileName.getAbsolutePath());
      if (trace) {
         log.trace("opened %s", db);
      }

      writeOptions = new WriteOptions().setDisableWAL(false);

      if (isANewRAFTLog()) {
         if (trace) {
            log.trace("log %s is new, must be initialized", dbFileName);
         }
         initLogWithMetadata();
      } else {
         if (trace) {
            log.trace("log %s exists, does not have to be initialized", dbFileName);
         }
         readMetadataFromLog();
      }
      checkForConsistency();
   }

   @Override
   public void close() {
      if (log.isTraceEnabled()) {
         log.trace("closing DB: %s", db);
      }

      if (db != null) db.close();
      currentTerm = 0;
      votedFor = null;
      commitIndex = 0;
      lastAppended = 0;
      firstAppended = 0;

   }

   @Override
   public void delete() {
      this.close();
      if (log.isTraceEnabled()) {
         log.trace("deleting DB directory: %s", dbFileName);
      }
      try {
         FileUtils.deleteDirectory(dbFileName);
      } catch (IOException e) {
         log.error("Failed to delete directory " + dbFileName, e);
      }
   }


   @Override
   public int currentTerm() {
      return currentTerm;
   }

   @Override
   public Log currentTerm(int new_term) {
      setInt("term", CURRENTTERM, new_term);
      currentTerm = new_term;
      return this;
   }

   @Override
   public Address votedFor() {
      return votedFor;
   }

   @Override
   public Log votedFor(Address member) {
      if (log.isTraceEnabled()) {
         log.trace("Set voted-for to %s", member);
      }
      try {
         db.put(VOTEDFOR, objectToByteBuffer(member));
      } catch (Exception e) {
         log.error("Failed to set voted-for", e);
      }
      votedFor = member;
      return this;
   }

   @Override
   public int firstAppended() {
      return firstAppended;
   }

   @Override
   public int commitIndex() {
      return commitIndex;
   }

   @Override
   public Log commitIndex(int new_index) {
      setInt("commit-index", COMMITINDEX, new_index);
      commitIndex = new_index;
      return this;
   }

   @Override
   public int lastAppended() {
      return lastAppended;
   }

   @Override
   public void append(int index, boolean overwrite, LogEntry... entries) {
      boolean trace = log.isTraceEnabled();
      if (trace) {
         log.trace("Appending %d entries", entries.length);
      }
      int newTerm = currentTerm;
      try (WriteBatch batch = new WriteBatch()) {
         for (LogEntry entry : entries) {
            if (overwrite) {
               appendEntry(index, entry, batch);
            } else {
               appendEntryIfAbsent(index, entry, batch);
            }

            if (entry.term != newTerm) {
               newTerm = entry.term;
            }

            index++;
         }
         if (trace) {
            log.trace("Flushing batch to DB: %s", batch);
         }

         if (newTerm != currentTerm) {
            updateCurrentTerm(newTerm, batch);
         }
         updateLastAppended(index - 1, batch);
         db.write(writeOptions, batch);
      } catch (RocksDBException | IOException e) {
         log.error("Failed to append entries", e);
      }
   }

   @Override
   public LogEntry get(int index) {
      return getLogEntry(index);
   }

   @Override
   public void forEach(ObjIntConsumer<LogEntry> function, int start_index, int end_index) {
      start_index = Math.max(start_index, firstAppended);
      end_index = Math.min(end_index, lastAppended);

      // in RAFT, indexes start in 1.
      if (start_index <= 0) {
         start_index = 1;
      }

      // nothing to iterate
      if (end_index < start_index) {
         return;
      }

      try {
         while (start_index <= end_index) {
            // IntStream.range: end is exclusive
            int iterationEndIndex = Math.min(start_index + ITERATION_BATCH_SIZE + 1, end_index + 1);

            List<byte[]> keys = IntStream.range(start_index, iterationEndIndex)
                  .mapToObj(IntegerHelper::fromIntToByteArray)
                  .collect(Collectors.toList());

            int index = start_index;
            for (byte[] entry : db.multiGetAsList(keys)) {
               if (entry == null) {
                  // last appended field is updated before the entries are flushed to rocksdb
                  // if we find a null entry, it means the iteration is over
                  return;
               }
               LogEntry logEntry = streamableFromByteBuffer(LogEntry::new, entry);
               function.accept(logEntry, index);
               ++index;
            }
            start_index = iterationEndIndex;
         }
      } catch (Exception e) {
         log.error("Error while iterating over entry from [" + start_index + "," + end_index + "]", e);
      }
   }

   @Override
   public void forEach(ObjIntConsumer<LogEntry> function) {
      this.forEach(function, Math.max(1, firstAppended), lastAppended);
   }

   @Override
   public void truncate(int upto_index) {
      if ((upto_index < firstAppended) || (upto_index > lastAppended)) {
         return;
      }

      try (WriteBatch batch = new WriteBatch()) {
         for (int index = firstAppended; index < upto_index; index++) {
            batch.delete(fromIntToByteArray(index));
         }
         batch.put(FIRSTAPPENDED, fromIntToByteArray(upto_index));
         db.write(writeOptions, batch);
         firstAppended = upto_index;
      } catch (RocksDBException e) {
         log.error("Failed to truncate log to index " + upto_index, e);
      }
   }

   @Override
   public void deleteAllEntriesStartingFrom(int start_index) {
      if ((start_index < firstAppended) || (start_index > lastAppended)) {
         return;
      }

      try (WriteBatch batch = new WriteBatch()) {
         for (int index = start_index; index <= lastAppended; index++) {
            batch.delete(fromIntToByteArray(index));
         }
         LogEntry last = getLogEntry(start_index - 1);

         if (last == null) {
            updateCurrentTerm(0, batch);
         } else {
            updateCurrentTerm(last.term, batch);
         }
         updateLastAppended(start_index - 1, batch);
         if (commitIndex > lastAppended) {
            batch.put(COMMITINDEX, fromIntToByteArray(lastAppended));
            commitIndex = lastAppended;
         }

         db.write(writeOptions, batch);
      } catch (RocksDBException e) {
         log.error("Failed to delete log starting from " + start_index, e);
      }
   }

   // Useful in debugging
   public byte[] print(byte[] bytes) throws RocksDBException {
      return db.get(bytes);
   }

   // Useful in debugging
   public void printMetadata() throws Exception {

      log.info("-----------------");
      log.info("RAFT Log Metadata");
      log.info("-----------------");

      List<byte[]> data = db.multiGetAsList(asList(FIRSTAPPENDED, LASTAPPENDED, CURRENTTERM, COMMITINDEX, VOTEDFOR));

      log.info("First Appended: %d", fromByteArrayToInt(data.get(0)));
      log.info("Last Appended: %d", fromByteArrayToInt(data.get(1)));
      log.info("Current Term: %d", fromByteArrayToInt(data.get(2)));
      log.info("Commit Index: %d", fromByteArrayToInt(data.get(3)));
      log.info("Voted for: %s", objectFromByteBuffer(data.get(4)));
   }

   @Override
   public String toString() {
      return "RocksDBLog{" +
            "currentTerm=" + currentTerm +
            ", votedFor=" + votedFor +
            ", commitIndex=" + commitIndex +
            ", lastAppended=" + lastAppended +
            ", firstAppended=" + firstAppended +
            '}';
   }

   private LogEntry getLogEntry(int index) {
      try {
         byte[] entryBytes = db.get(fromIntToByteArray(index));
         return entryBytes == null ? null : streamableFromByteBuffer(LogEntry::new, entryBytes);
      } catch (Exception e) {
         log.error("Failed to read log entry from index " + index, e);
      }
      return null;
   }

   private void appendEntryIfAbsent(int index, LogEntry entry, WriteBatch batch) throws RocksDBException, IOException {
      if (db.get(fromIntToByteArray(index)) != null) {
         throw new IllegalStateException("Entry at index " + index + " already exists");
      } else {
         appendEntry(index, entry, batch);
      }
   }

   private void appendEntry(int index, LogEntry entry, WriteBatch batch) throws IOException, RocksDBException {
      if (log.isTraceEnabled()) {
         log.trace("Appending entry %d: %s", index, entry);
      }
      batch.put(fromIntToByteArray(index), streamableToByteBuffer(entry));
   }


   private void updateCurrentTerm(int updatedCurrentTerm, WriteBatch batch) throws RocksDBException {
      currentTerm = updatedCurrentTerm;
      if (log.isTraceEnabled()) {
         log.trace("Updating currentTerm: %d", updatedCurrentTerm);
      }
      batch.put(CURRENTTERM, fromIntToByteArray(currentTerm));
   }

   private void updateLastAppended(int updatedLastAppended, WriteBatch batch) throws RocksDBException {
      lastAppended = updatedLastAppended;
      if (log.isTraceEnabled()) {
         log.trace("Updating lastAppended: %d", updatedLastAppended);
      }
      batch.put(LASTAPPENDED, fromIntToByteArray(lastAppended));
   }


   private boolean isANewRAFTLog() throws RocksDBException {
      return db.get(FIRSTAPPENDED) == null;
   }

   private void initLogWithMetadata() throws RocksDBException {
      try (WriteBatch batch = new WriteBatch()) {
         batch.put(FIRSTAPPENDED, fromIntToByteArray(0));
         batch.put(LASTAPPENDED, fromIntToByteArray(0));
         batch.put(CURRENTTERM, fromIntToByteArray(0));
         batch.put(COMMITINDEX, fromIntToByteArray(0));
         db.write(writeOptions, batch);
      }
   }

   private void readMetadataFromLog() throws Exception {
      List<byte[]> data = db.multiGetAsList(asList(FIRSTAPPENDED, LASTAPPENDED, CURRENTTERM, COMMITINDEX, VOTEDFOR));
      firstAppended = fromByteArrayToInt(data.get(0));
      lastAppended = fromByteArrayToInt(data.get(1));
      currentTerm = fromByteArrayToInt(data.get(2));
      commitIndex = fromByteArrayToInt(data.get(3));
      votedFor = objectFromByteBuffer(data.get(4));
      if (log.isDebugEnabled()) {
         log.debug("read metadata from log: firstAppended=%d, lastAppended=%d, currentTerm=%d, commitIndex=%d, votedFor=%s",
               firstAppended, lastAppended, currentTerm, commitIndex, votedFor);
      }
   }

   private void checkForConsistency() throws Exception {
      List<byte[]> data = db.multiGetAsList(asList(FIRSTAPPENDED, LASTAPPENDED, CURRENTTERM, COMMITINDEX, VOTEDFOR));

      int loggedFirstAppended = fromByteArrayToInt(data.get(0));
      log.trace("FirstAppended in DB is: %d", loggedFirstAppended);

      int loggedLastAppended = fromByteArrayToInt(data.get(1));
      log.trace("LastAppended in DB is: %d", loggedLastAppended);

      int loggedCurrentTerm = fromByteArrayToInt(data.get(2));
      log.trace("CurrentTerm in DB is: %d", loggedCurrentTerm);

      int loggedCommitIndex = fromByteArrayToInt(data.get(3));
      log.trace("CommitIndex in DB is: %d", loggedCommitIndex);

      Address loggedVotedForAddress = objectFromByteBuffer(data.get(4));
      log.trace("VotedFor in DB is: %s", loggedVotedForAddress);

      assert (firstAppended == loggedFirstAppended);
      assert (lastAppended == loggedLastAppended);
      assert (currentTerm == loggedCurrentTerm);
      assert (commitIndex == loggedCommitIndex);
      assert votedFor == null || (votedFor.equals(loggedVotedForAddress));

      LogEntry lastAppendedEntry = getLogEntry(lastAppended);
      assert (lastAppendedEntry == null || lastAppendedEntry.term <= currentTerm);
   }

   private void setInt(String field, byte[] key, int value) {
      if (log.isTraceEnabled()) {
         log.trace("Set %s to %s", field, value);
      }
      try {
         db.put(key, fromIntToByteArray(value));
      } catch (RocksDBException e) {
         log.error("Failed to set " + field, e);
      }
   }

}
