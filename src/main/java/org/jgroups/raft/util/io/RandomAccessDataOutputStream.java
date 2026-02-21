package org.jgroups.raft.util.io;

import org.jgroups.util.ByteArrayDataOutputStream;

import java.io.IOException;
import java.util.Objects;

/**
 * Adapter that wraps {@link ByteArrayDataOutputStream} as a {@link RandomAccessOutput}.
 *
 * <p>
 * This adapter enables the binary serialization framework to write directly to JGroups' {@link ByteArrayDataOutputStream}
 * without intermediate buffering. It delegates all {@link java.io.DataOutput} operations to the underlying stream while
 * exposing position manipulation capabilities required for wire format compatibility.
 * </p>
 *
 * @author José Bolina
 * @since 2.0
 * @see RandomAccessOutput
 */
public class RandomAccessDataOutputStream implements RandomAccessOutput {
    private final ByteArrayDataOutputStream output;

    /**
     * Creates a new adapter wrapping the given output stream.
     *
     * @param output The {@link ByteArrayDataOutputStream} to wrap
     * @throws NullPointerException if output is null
     */
    public RandomAccessDataOutputStream(ByteArrayDataOutputStream output) {
        this.output = Objects.requireNonNull(output, "Array output can not be null");
    }

    @Override
    public int position() {
        return output.position();
    }

    @Override
    public void position(int pos) {
        output.position(pos);
    }

    @Override
    public void write(int i) throws IOException {
        output.write(i);
    }

    @Override
    public void write(byte[] bytes) throws IOException {
        output.write(bytes);
    }

    @Override
    public void write(byte[] bytes, int i, int i1) throws IOException {
        output.write(bytes, i, i1);
    }

    @Override
    public void writeBoolean(boolean b) throws IOException {
        output.writeBoolean(b);
    }

    @Override
    public void writeByte(int i) throws IOException {
        output.writeByte(i);
    }

    @Override
    public void writeShort(int i) {
        output.writeShort(i);
    }

    @Override
    public void writeChar(int i) {
        output.writeChar(i);
    }

    @Override
    public void writeInt(int i) throws IOException {
        output.writeInt(i);
    }

    @Override
    public void writeLong(long l) throws IOException {
        output.writeLong(l);
    }

    @Override
    public void writeFloat(float v) {
        output.writeFloat(v);
    }

    @Override
    public void writeDouble(double v) {
        output.writeDouble(v);
    }

    @Override
    public void writeBytes(String s) {
        output.writeBytes(s);
    }

    @Override
    public void writeChars(String s) {
        output.writeChars(s);
    }

    @Override
    public void writeUTF(String s) throws IOException {
        output.writeUTF(s);
    }
}
