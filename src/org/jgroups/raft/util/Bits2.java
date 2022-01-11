package org.jgroups.raft.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * These methods should be moved to JGroups 4.x (5 already has them)
 * @author Bela Ban
 */
public class Bits2 {


    public static void writeIntCompressed(int num, DataOutput out) throws IOException {
        if(num == 0) {
            out.write(0);
            return;
        }
        final byte bytes_needed=bytesRequiredFor(num);
        out.write(bytes_needed);
        for(int i=0; i < bytes_needed; i++)
            out.write(getByteAt(num, i));
    }

    public static int readIntCompressed(DataInput in) throws IOException {
        byte len=in.readByte();
        if(len == 0)
            return 0;
        return makeInt(in, len);
    }

    /** Writes a long to out in variable-length encoding */
    public static void writeLongCompressed(final long num, final DataOutput out) throws IOException {
        if(num == 0) {
            out.write(0);
            return;
        }
        final byte bytes_needed=bytesRequiredFor(num);
        out.write(bytes_needed);
        for(int i=0; i < bytes_needed; i++)
            out.write(getByteAt(num, i));
    }

    public static long readLongCompressed(DataInput in) throws IOException {
        byte len=in.readByte();
        if(len == 0)
            return 0;
        return makeLong(in, len);
    }

    protected static byte bytesRequiredFor(int number) {
        if(number >> 24 != 0) return 4;
        if(number >> 16 != 0) return 3;
        if(number >>  8 != 0) return 2;
        return 1;
    }


    protected static byte bytesRequiredFor(long number) {
        if(number >> 56 != 0) return 8;
        if(number >> 48 != 0) return 7;
        if(number >> 40 != 0) return 6;
        if(number >> 32 != 0) return 5;
        if(number >> 24 != 0) return 4;
        if(number >> 16 != 0) return 3;
        if(number >>  8 != 0) return 2;
        return 1;
    }

    static protected byte getByteAt(long num, int index) {
        return (byte)((num >> (index * 8)));
    }


    public static int makeInt(DataInput in, int bytes_to_read) throws IOException {
        int retval=0;
        for(int i=0; i < bytes_to_read; i++) {
            byte b=in.readByte();
            retval |= ((int)b & 0xff) << (i * 8);
        }
        return retval;
    }

    public static long makeLong(DataInput in, int bytes_to_read) throws IOException {
        long retval=0;
        for(int i=0; i < bytes_to_read; i++) {
            byte b=in.readByte();
            retval |= ((long)b & 0xff) << (i * 8);
        }
        return retval;
    }

}
