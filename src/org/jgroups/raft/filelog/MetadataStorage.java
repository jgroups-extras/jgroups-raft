package org.jgroups.raft.filelog;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.protocols.raft.Log;
import org.jgroups.util.ByteBufferInputStream;
import org.jgroups.util.Util;

/**
 * Stores the RAFT log metadata in a file.
 * <p>
 * The metadata includes the commit index, the current term and the last vote.
 *
 * @author Pedro Ruivo
 * @since 0.5.4
 */
public class MetadataStorage {

   private static final String FILE_NAME = "metadata.raft";

   // page 4 RAft original paper: COMMIT INDEX DOESN'T REQUIRE FDATASYNC
   private static final int COMMIT_INDEX_POS = 0;
   // page 4 RAft original paper: RARE BUT REQUIRES FDATASYNC
   private static final int CURRENT_TERM_POS = COMMIT_INDEX_POS + Global.INT_SIZE;
   // Check if file length is != from the last FSYNC: this is variable-sized!
   private static final int VOTED_FOR_POS = CURRENT_TERM_POS + Global.INT_SIZE;
   private final FileStorage fileStorage;
   // This won't need a sys-call for frequently accessed data
   private MappedByteBuffer commitAndTermBytes;
   private boolean fsync;

   public MetadataStorage(File parentDir, boolean fsync) {
      fileStorage = new FileStorage(new File(parentDir, FILE_NAME));
      this.fsync = fsync;
   }

   public void useFsync(boolean value) {
      fsync = value;
   }

   public boolean useFsync() {
      return fsync;
   }

   public void open() throws IOException {
      fileStorage.open();
      commitAndTermBytes = FileChannel.open(fileStorage.getStorageFile().toPath(),
                                            StandardOpenOption.READ, StandardOpenOption.WRITE)
         .map(FileChannel.MapMode.READ_WRITE, 0, VOTED_FOR_POS);
   }

   public void close() throws IOException {
      fileStorage.close();
      commitAndTermBytes = null;
   }

   public void delete() throws IOException {
      fileStorage.delete();
      commitAndTermBytes = null;
   }

   public int getCommitIndex() {
      return commitAndTermBytes.getInt(COMMIT_INDEX_POS);
   }

   public void setCommitIndex(int commitIndex) throws IOException {
      commitAndTermBytes.putInt(COMMIT_INDEX_POS, commitIndex);
   }

   public int getCurrentTerm() {
      return commitAndTermBytes.getInt(CURRENT_TERM_POS);
   }

   public void setCurrentTerm(int term) throws IOException {
      commitAndTermBytes.putInt(CURRENT_TERM_POS, term);
      if (fsync) {
         commitAndTermBytes.force();
      }
   }

   public Address getVotedFor() throws IOException, ClassNotFoundException {
      // most addresses are quite small
      final ByteBuffer dataLengthBuffer = fileStorage.read(VOTED_FOR_POS, Global.INT_SIZE);
      if (dataLengthBuffer.remaining() != Global.INT_SIZE) {
         // corrupted?
         return null;
      }
      final int addressLength = dataLengthBuffer.getInt(0);
      final ByteBuffer addressLengthBuffer = fileStorage.read(VOTED_FOR_POS + Global.INT_SIZE, addressLength);
      if (addressLengthBuffer.remaining() != addressLength) {
         //uname to read "addressLength" bytes
         return null;
      }
      return readAddress(addressLengthBuffer);
   }

   public void setVotedFor(Address address) throws IOException {
      if (address == null) {
         fileStorage.truncateTo(VOTED_FOR_POS);
         return;
      }
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      Util.writeAddress(address, new DataOutputStream(baos));

      byte[] data = baos.toByteArray();
      ByteBuffer buffer = fileStorage.ioBufferWith(Global.INT_SIZE + data.length);
      buffer.putInt(data.length);
      buffer.put(data);
      buffer.flip();
      fileStorage.write(VOTED_FOR_POS);
   }

   private static Address readAddress(ByteBuffer buffer) throws IOException, ClassNotFoundException {
      return Util.readAddress(new ByteBufferInputStream(buffer));
   }
}
