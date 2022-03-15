package org.jgroups.raft.filelog;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.util.ByteBufferInputStream;
import org.jgroups.util.Util;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Stores the RAFT log metadata in a file.
 * <p>
 * The metadata includes the commit index, the current term and the last vote.
 *
 * @author Pedro Ruivo
 * @since 0.5.4
 */
public class MetadataStorage extends BaseStorage {

   private static final String FILE_NAME = "metadata.raft";

   private static final int COMMIT_INDEX_POS = 0;
   private static final int CURRENT_TERM_POS = COMMIT_INDEX_POS + Global.INT_SIZE;
   private static final int VOTED_FOR_POS = CURRENT_TERM_POS + Global.INT_SIZE;

   public MetadataStorage(File parentDir) {
      super(new File(parentDir, FILE_NAME));
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
   }

   public Address getVotedFor() throws IOException, ClassNotFoundException {
      FileChannel fChannel = checkOpen();
      // most addresses are quite small
      ByteBuffer data = ByteBuffer.allocate(Global.INT_SIZE);
      fChannel.read(data, VOTED_FOR_POS);
      data.flip();
      if (data.remaining() != Global.INT_SIZE) {
         // corrupted?
         return null;
      }
      int addressLength = data.getInt();
      data = ByteBuffer.allocate(addressLength);
      fChannel.read(data, VOTED_FOR_POS + Global.INT_SIZE);
      data.flip();
      if (data.remaining() != addressLength) {
         //uname to read "addressLength" bytes
         return null;
      }
      return readAddress(data);
   }

   public void setVotedFor(Address address) throws IOException {
      FileChannel fChannel = checkOpen();
      if (address == null) {
         fChannel.truncate(VOTED_FOR_POS);
         return;
      }
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      Util.writeAddress(address, new DataOutputStream(baos));

      byte[] data = baos.toByteArray();
      ByteBuffer buffer = ByteBuffer.allocate(Global.INT_SIZE + data.length);
      buffer.putInt(data.length);
      buffer.put(data);
      buffer.flip();
      fChannel.write(buffer, VOTED_FOR_POS);
   }

   private int readIntOrZero(long position) throws IOException {
      FileChannel fChannel = checkOpen();
      ByteBuffer data = ByteBuffer.allocate(Global.INT_SIZE);
      int read = fChannel.read(data, position);
      data.flip();
      return read == Global.INT_SIZE ? data.getInt() : 0;
   }

   private void writeInt(int value, long position) throws IOException {
      FileChannel fChannel = checkOpen();
      ByteBuffer data = ByteBuffer.allocate(Global.INT_SIZE);
      data.putInt(value);
      data.flip();
      fChannel.write(data, position);
   }

   private static Address readAddress(ByteBuffer buffer) throws IOException, ClassNotFoundException {
      return Util.readAddress(new ByteBufferInputStream(buffer));
   }
}
