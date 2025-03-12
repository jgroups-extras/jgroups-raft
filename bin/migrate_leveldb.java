///usr/bin/env jbang "$0" "$@" ; exit $?
//DEPS info.picocli:picocli:4.6.3
//DEPS org.jgroups:jgroups-raft:1.0.14.Final

import org.jgroups.protocols.raft.LevelDBLog;
import org.jgroups.protocols.raft.FileBasedLog;
import org.jgroups.protocols.raft.LogEntries;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.Callable;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "migrate_leveldb", mixinStandardHelpOptions = true, version = "migrate_leveldb 0.1",
        description = "Migrates data created by LevelDBLog to the FileBasedLog format.")
class migrate_leveldb implements Callable<Integer> {
    private static final String ANIMATION = "|/-\\";

    @Parameters(index = "0", description = "Path to the folder containing the LevelDBLog data")
    private String path;

    @Option(names = "--force", description = "Force the migration even with file-based data existent")
    private boolean force;

    public static void main(String... args) {
        int exitCode = new CommandLine(new migrate_leveldb()).execute(args);
        System.exit(exitCode);
    }

    @SuppressWarnings("removal")
    @Override
    public Integer call() throws Exception {
        System.out.printf("Verifying data located: %s%n", path);

        LevelDBLog src = new LevelDBLog();
        FileBasedLog dst = new FileBasedLog();

        src.init(path, null);
        dst.init(path, null);

        try (src; dst) {
            if (dst.sizeInBytes() > 0) {
                if (!force) {
                    System.out.println("There is already a file-based storage with data in place. Aborting migration...");
                    return 1;
                }

                Path target = Path.of(path, "temp");
                Path source = Path.of(path);

                if (Files.exists(target)) {
                    System.out.printf("Temporary folder already exists at '%s'. Aborting migration...%n", target);
                    return 1;
                }

                System.out.printf("Created data backup in: %s%n", target);

                Files.walkFileTree(source, new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                        Files.createDirectories(target.resolve(source.relativize(dir).toString()));
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        Files.copy(file, target.resolve(source.relativize(file).toString()));
                        return FileVisitResult.CONTINUE;
                    }
                });
            }

            ByteBuffer bb = src.getSnapshot();
            if (bb != null) {
                System.out.println("Migrating snapshot to file-based log");
                dst.setSnapshot(bb);
            }

            long first = src.firstAppended();
            long last = src.lastAppended();
            System.out.printf("Migrating entries in range: [%d; %d]%n", first, last);

            dst.reinitializeTo(first, src.get(first));
            src.forEach((entry, idx) -> {
                if (idx == first) return;
                LogEntries entries = new LogEntries();
                entries.add(entry);
                dst.append(idx, entries);
                printProgress(first, last, idx);
            });
        } catch (Throwable t) {
            System.out.println("Failed migrating data");
            t.printStackTrace(System.err);
            dst.delete();
            return 1;
        }

        outWrite(new byte[] { '\r' });
        return 0;
    }

    private static void printProgress(long start, long end, long curr) {
        float v = (float) (curr - start) / (end - start);
        int p = (int) (v * 100);
        String data = "\r" + ANIMATION.charAt(p % ANIMATION.length()) + ": Migrating " + p + "%";
        outWrite(data.getBytes());
    }

    private static void outWrite(byte[] bytes) {
        try {
            System.out.write(bytes);
        } catch (IOException ignore) { }
    }
}
