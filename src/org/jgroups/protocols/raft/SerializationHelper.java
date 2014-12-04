package org.jgroups.protocols.raft;

import java.io.*;

/**
 * Created by ugol on 04/12/14.
 */
public class SerializationHelper {

    public static byte[] asBytes(LogEntry entry) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        byte[] entry_in_bytes = new byte[]{};

        try {

            out = new ObjectOutputStream(bos);
            out.writeObject(entry);
            bos.flush();
            entry_in_bytes = bos.toByteArray();

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (out != null) {
                    out.close();
                }
            } catch (IOException ex) {
                // ignore close exception
            }
            try {
                bos.close();
            } catch (IOException ex) {
                // ignore close exception
            }
        }
        return entry_in_bytes;
    }

    public static LogEntry asLogEntry(byte[] bytes) {
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ObjectInput in = null;
        LogEntry entry = null;
        try {
            in = new ObjectInputStream(bis);
            entry = (LogEntry) in.readObject();
        } catch (ClassNotFoundException | IOException e) {
            e.printStackTrace();
        } finally {
            try {
                bis.close();
            } catch (IOException ex) {
                // ignore close exception
            }
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException ex) {
                // ignore close exception
            }
        }
        return entry;
    }
}
