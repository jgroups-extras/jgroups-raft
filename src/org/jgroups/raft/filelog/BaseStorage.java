package org.jgroups.raft.filelog;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Objects;

import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.raft.util.pmem.FileProvider;

/**
 * Base class to store data in a file.
 *
 * @author Pedro Ruivo
 * @since 0.5.4
 */
public abstract class BaseStorage {

   protected static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   private final File storageFile;
   private volatile FileChannel channel;

   BaseStorage(File storageFile) {
      this.storageFile = Objects.requireNonNull(storageFile);
   }

   public synchronized void open() throws IOException {
      if (channel == null) {
         channel = FileProvider.openChannel(storageFile, 1024, true, true);
      }
   }

   public synchronized void close() throws IOException {
      if (channel != null) {
         channel.close();
         channel = null;
      }
   }

   public synchronized void delete() throws IOException {
      if (channel != null) {
         channel.close();
         channel = null;
      }
      if (storageFile.exists()) {
         if (!storageFile.delete()) {
            log.warn("Failed to delete file " + storageFile.getAbsolutePath());
         }
      }
   }

   protected FileChannel checkOpen() throws IOException {
      FileChannel fileChannel = channel;
      if (fileChannel == null) {
         throw new IOException("File " + storageFile.getAbsolutePath() + " not open!");
      }
      return fileChannel;
   }

   protected synchronized void truncateUntil(long position) throws IOException {
      FileChannel existing = checkOpen();
      File tmpFile = new File(storageFile.getParentFile(), storageFile.getName() + ".tmp");
      FileChannel newChannel = FileProvider.openChannel(tmpFile, (int) existing.size(), true, true);
      existing.transferTo(position, existing.size(), newChannel);
      newChannel.close();
      existing.close();
      Files.move(tmpFile.toPath(), storageFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
      channel = null;
      open();
   }

}
