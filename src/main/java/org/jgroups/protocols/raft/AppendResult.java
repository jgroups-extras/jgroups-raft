package org.jgroups.protocols.raft;

import org.jgroups.util.Bits;
import org.jgroups.util.Streamable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * The result of an AppendEntries request
 *
 * @author Bela Ban
 * @since 0.1
 */
public class AppendResult implements Streamable {

    /**
     * True if the append succeeded, false otherwise
     */
    protected Result result;
    /**
     * The index of the last appended entry if success == true. If success is false, the first index for
     * non-matching term. If index == 0, this means the follower doesn't have a log and needs to run the
     * InstallSnapshot protocol to fetch the initial snapshot
     */
    protected long index;
    /**
     * The commit_index of the follower
     */
    protected long commit_index;

    /**
     * The index of the entry before the entries of the request this result answers.
     *
     * <p>
     * Tells the leader which request a rejection answers, so it can tell a rejection of a request sent before its
     * catch-up from one sent after the catch-up was lost.
     * </p>
     */
    private long prevLogIndex;

    public AppendResult() { }

    public AppendResult(Result result, long index) {
        this.result = result;
        this.index = index;
    }

    public AppendResult(Result result, long index, long prevLogIndex) {
        this(result, index);
        this.prevLogIndex = prevLogIndex;
    }

    public boolean success() {
        return result != null && result == Result.OK;
    }

    public long index() {
        return index;
    }

    public long commitIndex() {
        return commit_index;
    }

    public long prevLogIndex() {
        return prevLogIndex;
    }

    public AppendResult commitIndex(long ci) {
        this.commit_index = ci;
        return this;
    }

    public Result result() {
        return result;
    }

    public int size() {
        return Bits.size(result.ordinal())
                + Bits.size(index)
                + Bits.size(commit_index)
                + Bits.size(prevLogIndex);
    }

    @Override
    public void writeTo(DataOutput out) throws IOException {
        Bits.writeIntCompressed(result.ordinal(), out);
        Bits.writeLongCompressed(index, out);
        Bits.writeLongCompressed(commit_index, out);
        Bits.writeLongCompressed(prevLogIndex, out);
    }

    @Override
    public void readFrom(DataInput in) throws IOException {
        int ordinal = Bits.readIntCompressed(in);
        result = Result.values()[ordinal];
        index = Bits.readLongCompressed(in);
        commit_index = Bits.readLongCompressed(in);
        prevLogIndex = Bits.readLongCompressed(in);
    }

    @Override
    public String toString() {
        return String.format("%b%s, index=%d, commit-index=%d, prev-log-index=%d",
                success(), success() ? "" : String.format(" (%s)", result), index, commit_index,
                prevLogIndex);
    }

    public enum Result {OK, FAIL_ENTRY_NOT_FOUND, FAIL_CONFLICTING_PREV_TERM}
}
