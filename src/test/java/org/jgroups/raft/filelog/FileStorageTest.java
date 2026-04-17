package org.jgroups.raft.filelog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.jgroups.Global;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class FileStorageTest {

    private Path tempDir;
    private FileStorage storage;

    @BeforeMethod
    public void setUp() throws IOException {
        tempDir = Files.createTempDirectory("file-storage-test");
    }

    @AfterMethod
    public void tearDown() throws IOException {
        if (storage != null) {
            storage.close();
        }
        if (tempDir != null && Files.exists(tempDir)) {
            try (Stream<Path> walk = Files.walk(tempDir)) {
                walk.sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            }
        }
    }

    public void testOpenCreatesFile() throws IOException {
        storage = createStorage();
        assertThat(storageFile()).doesNotExist();

        storage.open();

        assertThat(storageFile()).exists();
        assertThat(storage.isOpened()).isTrue();
    }

    public void testInitialFileSizeIsZero() throws IOException {
        storage = createStorage();
        storage.open();
        assertThat(storage.getCachedFileSize()).isZero();
    }

    public void testWriteAndReadRoundtrip() throws IOException {
        storage = createStorage();
        storage.open();

        byte[] data = {1, 2, 3, 4, 5};
        writeBytes(0, data);

        ByteBuffer read = storage.read(0, data.length);
        byte[] result = new byte[read.remaining()];
        read.get(result);
        assertThat(result).isEqualTo(data);
    }

    public void testWriteReturnsNumberOfBytesWritten() throws IOException {
        storage = createStorage();
        storage.open();

        byte[] data = new byte[64];
        int written = writeBytes(0, data);
        assertThat(written).isEqualTo(data.length);
    }

    public void testWriteGrowsFileSize() throws IOException {
        storage = createStorage();
        storage.open();

        byte[] data = new byte[100];
        writeBytes(0, data);

        assertThat(storage.getCachedFileSize()).isGreaterThanOrEqualTo(data.length);
    }

    public void testWriteAtOffset() throws IOException {
        storage = createStorage();
        storage.open();

        byte[] first = {1, 2, 3};
        writeBytes(0, first);

        byte[] second = {4, 5, 6};
        writeBytes(3, second);

        ByteBuffer read = storage.read(0, 6);
        byte[] result = new byte[6];
        read.get(result);
        assertThat(result).isEqualTo(new byte[]{1, 2, 3, 4, 5, 6});
    }

    public void testOverwriteExistingData() throws IOException {
        storage = createStorage();
        storage.open();

        writeBytes(0, new byte[]{1, 2, 3, 4});
        writeBytes(0, new byte[]{5, 6, 7, 8});

        ByteBuffer read = storage.read(0, 4);
        byte[] result = new byte[4];
        read.get(result);
        assertThat(result).isEqualTo(new byte[]{5, 6, 7, 8});
    }

    public void testWriteWithoutPreparedBufferThrows() throws IOException {
        storage = createStorage();
        storage.open();

        assertThatThrownBy(() -> storage.write(0))
                .isInstanceOf(IllegalStateException.class);
    }

    public void testIoBufferReuseWhenCapacitySufficient() throws IOException {
        storage = createStorage();
        storage.open();

        ByteBuffer first = storage.ioBufferWith(32);
        ByteBuffer second = storage.ioBufferWith(16);
        assertThat(second).isSameAs(first);
    }

    public void testIoBufferReallocationWhenCapacityInsufficient() throws IOException {
        storage = createStorage();
        storage.open();

        ByteBuffer first = storage.ioBufferWith(16);
        ByteBuffer second = storage.ioBufferWith(32);
        assertThat(second).isNotSameAs(first);
        assertThat(second.capacity()).isGreaterThanOrEqualTo(32);
    }

    public void testFlushStateAfterWrite() throws IOException {
        storage = createStorage();
        storage.open();

        assertThat(storage.hasPendingFlush()).isFalse();

        writeBytes(0, new byte[]{1, 2, 3});
        assertThat(storage.hasPendingFlush()).isTrue();

        storage.flush();
        assertThat(storage.hasPendingFlush()).isFalse();
    }

    public void testFlushIsNoOpWhenNothingPending() throws IOException {
        storage = createStorage();
        storage.open();

        storage.flush();
        assertThat(storage.hasPendingFlush()).isFalse();
    }

    public void testTruncateTo() throws IOException {
        storage = createStorage();
        storage.open();

        writeBytes(0, new byte[100]);
        storage.truncateTo(50);

        assertThat(storage.getCachedFileSize()).isEqualTo(50);
        assertThat(storage.hasPendingFlush()).isTrue();
    }

    public void testCloseIsIdempotent() throws IOException {
        storage = createStorage();
        storage.open();

        storage.close();
        storage.close();
        assertThat(storage.isOpened()).isFalse();
    }

    public void testDataPersistsAcrossCloseAndReopen() throws IOException {
        storage = createStorage();
        storage.open();

        byte[] data = {10, 20, 30, 40, 50};
        writeBytes(0, data);
        storage.flush();
        storage.close();

        storage = createStorage();
        storage.open();

        ByteBuffer read = storage.read(0, data.length);
        byte[] result = new byte[read.remaining()];
        read.get(result);
        assertThat(result).isEqualTo(data);
    }

    public void testWriteAheadBytesGrowsFileBeyondData() throws IOException {
        storage = new FileStorage(storageFile(), 1024);
        storage.open();

        writeBytes(0, new byte[10]);

        assertThat(storage.getCachedFileSize()).isGreaterThan(10);
    }

    public void testShortWriteIsDetected() throws IOException {
        AtomicInteger writeCount = new AtomicInteger();
        FileHandleProvider shortWriteProvider = file -> {
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            FileChannel real = raf.getChannel();
            FileChannel wrapped = new DelegatingFileChannel(real) {
                @Override
                public int write(ByteBuffer src, long position) throws IOException {
                    if (writeCount.getAndIncrement() == 0 && src.remaining() > 1) {
                        int half = src.remaining() / 2;
                        int originalLimit = src.limit();
                        src.limit(src.position() + half);
                        int written = super.write(src, position);
                        src.limit(originalLimit);
                        return written;
                    }
                    return super.write(src, position);
                }
            };
            return new FileHandleProvider.Handle(wrapped, raf);
        };

        storage = new FileStorage(storageFile(), 0, shortWriteProvider);
        storage.open();

        byte[] data = new byte[64];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }
        writeBytes(0, data);

        ByteBuffer read = storage.read(0, data.length);
        byte[] result = new byte[read.remaining()];
        read.get(result);
        assertThat(result).isEqualTo(data);
    }

    public void testShortWriteWhenGrowingFile() throws IOException {
        AtomicInteger writeCount = new AtomicInteger();
        FileHandleProvider shortWriteProvider = file -> {
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            FileChannel real = raf.getChannel();
            FileChannel wrapped = new DelegatingFileChannel(real) {
                @Override
                public int write(ByteBuffer src, long position) throws IOException {
                    if (writeCount.getAndIncrement() == 0 && src.remaining() > 1) {
                        int half = src.remaining() / 2;
                        int originalLimit = src.limit();
                        src.limit(src.position() + half);
                        int written = super.write(src, position);
                        src.limit(originalLimit);
                        return written;
                    }
                    return super.write(src, position);
                }
            };
            return new FileHandleProvider.Handle(wrapped, raf);
        };

        storage = new FileStorage(storageFile(), 0, shortWriteProvider);
        storage.open();

        byte[] data = new byte[64];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }
        writeBytes(0, data);

        ByteBuffer read = storage.read(0, data.length);
        byte[] result = new byte[read.remaining()];
        read.get(result);
        assertThat(result).isEqualTo(data);
    }

    public void testRepeatedShortWritesComplete() throws IOException {
        FileHandleProvider oneByteProvider = file -> {
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            FileChannel real = raf.getChannel();
            FileChannel wrapped = new DelegatingFileChannel(real) {
                @Override
                public int write(ByteBuffer src, long position) throws IOException {
                    if (src.remaining() > 1) {
                        int originalLimit = src.limit();
                        src.limit(src.position() + 1);
                        int written = super.write(src, position);
                        src.limit(originalLimit);
                        return written;
                    }
                    return super.write(src, position);
                }
            };
            return new FileHandleProvider.Handle(wrapped, raf);
        };

        storage = new FileStorage(storageFile(), 0, oneByteProvider);
        storage.open();

        byte[] data = new byte[20];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i + 1);
        }
        writeBytes(0, data);

        ByteBuffer read = storage.read(0, data.length);
        byte[] result = new byte[read.remaining()];
        read.get(result);
        assertThat(result).isEqualTo(data);
    }

    public void testShortWritePreservesFlushState() throws IOException {
        AtomicInteger writeCount = new AtomicInteger();
        FileHandleProvider shortWriteProvider = file -> {
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            FileChannel real = raf.getChannel();
            FileChannel wrapped = new DelegatingFileChannel(real) {
                @Override
                public int write(ByteBuffer src, long position) throws IOException {
                    if (writeCount.getAndIncrement() == 0 && src.remaining() > 1) {
                        int half = src.remaining() / 2;
                        int originalLimit = src.limit();
                        src.limit(src.position() + half);
                        int written = super.write(src, position);
                        src.limit(originalLimit);
                        return written;
                    }
                    return super.write(src, position);
                }
            };
            return new FileHandleProvider.Handle(wrapped, raf);
        };

        storage = new FileStorage(storageFile(), 0, shortWriteProvider);
        storage.open();

        writeBytes(0, new byte[32]);
        assertThat(storage.hasPendingFlush()).isTrue();
    }

    public void testZeroBytesWrittenThrows() throws IOException {
        FileHandleProvider zeroWriteProvider = file -> {
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            FileChannel real = raf.getChannel();
            FileChannel wrapped = new DelegatingFileChannel(real) {
                @Override
                public int write(ByteBuffer src, long position) throws IOException {
                    return 0;
                }
            };
            return new FileHandleProvider.Handle(wrapped, raf);
        };

        storage = new FileStorage(storageFile(), 0, zeroWriteProvider);
        storage.open();

        byte[] data = new byte[10];
        assertThatThrownBy(() -> writeBytes(0, data))
                .isInstanceOf(IOException.class);
    }

    private int writeBytes(long position, byte[] data) throws IOException {
        ByteBuffer buffer = storage.ioBufferWith(data.length);
        buffer.put(data);
        buffer.flip();
        return storage.write(position);
    }

    private FileStorage createStorage() {
        return new FileStorage(storageFile());
    }

    private File storageFile() {
        return tempDir.resolve("test.raft").toFile();
    }

    private static abstract class DelegatingFileChannel extends FileChannel {
        private final FileChannel delegate;

        DelegatingFileChannel(FileChannel delegate) {
            this.delegate = delegate;
        }

        @Override
        public int read(ByteBuffer dst) throws IOException { return delegate.read(dst); }

        @Override
        public long read(ByteBuffer[] dsts, int offset, int length) throws IOException { return delegate.read(dsts, offset, length); }

        @Override
        public int read(ByteBuffer dst, long position) throws IOException { return delegate.read(dst, position); }

        @Override
        public int write(ByteBuffer src) throws IOException { return delegate.write(src); }

        @Override
        public long write(ByteBuffer[] srcs, int offset, int length) throws IOException { return delegate.write(srcs, offset, length); }

        @Override
        public int write(ByteBuffer src, long position) throws IOException { return delegate.write(src, position); }

        @Override
        public long position() throws IOException { return delegate.position(); }

        @Override
        public FileChannel position(long newPosition) throws IOException { return delegate.position(newPosition); }

        @Override
        public long size() throws IOException { return delegate.size(); }

        @Override
        public FileChannel truncate(long size) throws IOException { return delegate.truncate(size); }

        @Override
        public void force(boolean metaData) throws IOException { delegate.force(metaData); }

        @Override
        public long transferTo(long position, long count, WritableByteChannel target) throws IOException { return delegate.transferTo(position, count, target); }

        @Override
        public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException { return delegate.transferFrom(src, position, count); }

        @Override
        public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException { return delegate.map(mode, position, size); }

        @Override
        public FileLock lock(long position, long size, boolean shared) throws IOException { return delegate.lock(position, size, shared); }

        @Override
        public FileLock tryLock(long position, long size, boolean shared) throws IOException { return delegate.tryLock(position, size, shared); }

        @Override
        protected void implCloseChannel() throws IOException { delegate.close(); }
    }
}
