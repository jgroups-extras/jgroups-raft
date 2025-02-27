package org.jgroups.raft;

import org.jgroups.Global;
import org.jgroups.util.SizeStreamable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Options to pass to {@link Settable#setAsync(byte[], int, int)} call
 * @author Bela Ban
 * @since  1.0.9
 */
public class Options implements SizeStreamable {

    public static final Options DEFAULT_OPTIONS = new Options();

    protected boolean ignore_return_value;

    public boolean ignoreReturnValue() {return ignore_return_value;}

    public Options ignoreReturnValue(boolean ignore) {this.ignore_return_value=ignore; return this;}

    public static Options create(boolean ignore_retval) {
        return new Options().ignoreReturnValue(ignore_retval);
    }

    public void writeTo(DataOutput out) throws IOException {
        out.writeBoolean(ignore_return_value);
    }

    public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        ignore_return_value=in.readBoolean();
    }

    public int serializedSize() {
        return Global.BYTE_SIZE;
    }

    public String toString() {
        return String.format("%s", ignore_return_value? "[ignore-retval]" : "");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Options)) return false;

        Options options = (Options) o;

        return ignore_return_value == options.ignore_return_value;
    }

    @Override
    public int hashCode() {
        return ignore_return_value ? 1 : 0;
    }
}
