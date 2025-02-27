package org.jgroups.protocols.raft;

import org.jgroups.util.Bits;
import org.jgroups.util.Streamable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * The result of an AppendEntries request
 * @author Bela Ban
 * @since  0.1
 */
public class AppendResult implements Streamable {

    public enum Result {OK, FAIL_ENTRY_NOT_FOUND, FAIL_CONFLICTING_PREV_TERM}

    /** True if the append succeeded, false otherwise */
    protected Result  result;

    /** The index of the last appended entry if success == true. If success is false, the first index for
     * non-matching term. If index == 0, this means the follower doesn't have a log and needs to run the
     * InstallSnapshot protocol to fetch the initial snapshot */
    protected long    index;

    /** The commit_index of the follower */
    protected long    commit_index;

    /** Ignored if success == true. If success is false, the non-matching term. */
    protected long    non_matching_term; // todo: needed ?

    public AppendResult() {}

    public AppendResult(Result result, long index) {
        this.result=result;
        this.index=index;
    }

    public AppendResult(Result result, long index, long non_matching_term) {
        this(result, index);
        this.non_matching_term=non_matching_term;
    }

    public boolean      success()            {return result != null && result == Result.OK;}
    public long         index()              {return index;}
    public long         commitIndex()        {return commit_index;}
    public long         nonMatchingTerm()    {return non_matching_term;}
    public AppendResult commitIndex(long ci) {this.commit_index=ci; return this;}

    public int size() {
        return Bits.size(result.ordinal()) + Bits.size(index) + Bits.size(commit_index) + Bits.size(non_matching_term);
    }

    public void writeTo(DataOutput out) throws IOException {
        Bits.writeIntCompressed(result.ordinal(), out);
        Bits.writeLongCompressed(index, out);
        Bits.writeLongCompressed(commit_index, out);
        Bits.writeLongCompressed(non_matching_term, out);
    }

    public void readFrom(DataInput in) throws IOException {
        int ordinal=Bits.readIntCompressed(in);
        result=Result.values()[ordinal];
        index=Bits.readLongCompressed(in);
        commit_index=Bits.readLongCompressed(in);
        non_matching_term=Bits.readLongCompressed(in);
    }


    public String toString() {
        return String.format("%b%s, index=%d, commit-index=%d%s",
                             success(), success()? "" : String.format(" (%s)", result), index, commit_index,
                             non_matching_term> 0? String.format(", non-matching-term=%d", non_matching_term) : "");
    }
}
