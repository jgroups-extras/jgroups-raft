package org.jgroups.protocols.raft;

import org.jgroups.Global;
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
    /** True if the append succeeded, false otherwise */
    protected boolean success;

    /** The index of the last appended entry if success == true. If success is false, the first index for
     * non-matching term. If index == 0, this means the follower doesn't have a log and needs to run the
     * InstallSnapshot protocol to fetch the initial snapshot */
    protected int     index;

    /** The commit_index of the follower */
    protected int     commit_index;

    /** Ignored if success == true. If success is false, the non-matching term. */
    protected int     non_matching_term; // todo: needed ?

    public AppendResult() {}

    public AppendResult(boolean success, int index) {
        this.success=success;
        this.index=index;
    }

    public AppendResult(boolean success, int index, int non_matching_term) {
        this.success=success;
        this.index=index;
        this.non_matching_term = non_matching_term;
    }

    public boolean success()         {return success;}
    public int     index()           {return index;}
    public int     commitIndex()     {return commit_index;}
    public int     nonMatchingTerm() {return non_matching_term;}


    public AppendResult commitIndex(int ci) {this.commit_index=ci; return this;}

    public int size() {
        return Global.BYTE_SIZE + Bits.size(index) + Bits.size(commit_index) + Bits.size(non_matching_term);
    }

    public void writeTo(DataOutput out) throws IOException {
        out.writeBoolean(success);
        Bits.writeIntCompressed(index, out);
        Bits.writeIntCompressed(commit_index, out);
        Bits.writeIntCompressed(non_matching_term, out);
    }

    public void readFrom(DataInput in) throws IOException {
        success=in.readBoolean();
        index=Bits.readIntCompressed(in);
        commit_index=Bits.readIntCompressed(in);
        non_matching_term=Bits.readIntCompressed(in);
    }

    public boolean isSuccess() {
        return success;
    }

    public int getIndex() {
        return index;
    }

    public int getMatchingTerm() {
        return non_matching_term;
    }

    public String toString() {
        return success + ", index=" + index + ", commit-index=" + commit_index;
    }
}
