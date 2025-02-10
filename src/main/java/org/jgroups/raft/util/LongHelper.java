package org.jgroups.raft.util;

/**
 * @author Bela Ban
 * @since  1.0.9
 */
public class LongHelper {

    public static byte[] fromLongToByteArray(long value) {
        return new byte[] {
          (byte)(value >>> 56),
          (byte)(value >>> 48),
          (byte)(value >>> 40),
          (byte)(value >>> 32),
          (byte)(value >>> 24),
          (byte)(value >>> 16),
          (byte)(value >>> 8),
          (byte)value};
    }

    public static long fromByteArrayToLong(byte[] b) {
        if((b == null) || (b.length != Long.BYTES))
            return 0;
        return ((long)b[7] & 0xff) +
          (((long)b[6] & 0xff) << 8) +
          (((long)b[5] & 0xff) << 16) +
          (((long)b[4] & 0xff) << 24) +
          (((long)b[3] & 0xff) << 32) +
          (((long)b[2] & 0xff) << 40) +
          (((long)b[1] & 0xff) << 48) +
          ((long)b[0] << 56);
    }
}
