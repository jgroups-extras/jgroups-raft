package org.jgroups.raft.filelog;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.jgroups.Address;
import org.jgroups.Global;
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

   public MetadataStorage(File parentDir) {
      fileStorage = new FileStorage(new File(parentDir, FILE_NAME));
   }

   public void open() throws IOException {
      fileStorage.open();
   }

   public void close() throws IOException {
      fileStorage.close();
   }

   public void delete() throws IOException {
      fileStorage.delete();
   }

   public int getCommitIndex() throws IOException {
      return readIntOrZero(COMMIT_INDEX_POS);
   }

   public void setCommitIndex(int commitIndex) throws IOException {
      writeInt(commitIndex, COMMIT_INDEX_POS);
   }

   public int getCurrentTerm() throws IOException {
      return readIntOrZero(CURRENT_TERM_POS);
   }

   public void setCurrentTerm(int term) throws IOException {
      writeInt(term, CURRENT_TERM_POS);
      fileStorage.flush();
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

   private int readIntOrZero(long position) throws IOException {
      ByteBuffer data = fileStorage.read(position, Global.INT_SIZE);
      return data.remaining() == Global.INT_SIZE ? data.getInt(0) : 0;
   }

   private void writeInt(int value, long position) throws IOException {
      ByteBuffer data = fileStorage.ioBufferWith(Global.INT_SIZE);
      data.putInt(value);
      data.flip();
      fileStorage.write(position);
   }

   private static Address readAddress(ByteBuffer buffer) throws IOException, ClassNotFoundException {
      return Util.readAddress(new ByteBufferInputStream(buffer));
   }
}
