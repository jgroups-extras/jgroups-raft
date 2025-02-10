package org.jgroups.perf.jmh;

import org.jgroups.protocols.raft.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * @author Pedro Ruivo
 * @since 0.5.4
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 10, time = 5)
@Measurement(iterations = 10, time = 5)
@Fork(1)
public class LogJmhBenchmark {

   public static void main(String[] args) throws RunnerException {
      Options opt = new OptionsBuilder()
            .include(LogJmhBenchmark.class.getCanonicalName())
            .forks(1)
            .build();

      new Runner(opt).run();
   }

   @Benchmark
   public void append(ExecutionPlan plan) {
      plan.log.append(plan.index, plan.entries);
      plan.index+= plan.batchSize;
   }

   @State(Scope.Benchmark)
   public static class ExecutionPlan {

      @Param({"10", "100", "4096"})
      private int dataSize;
      @Param({"leveldb", "file"})
      private String logType;
      @Param({"/tmp/tmp_raft_bench", "./tmp_raft_bench"})
      private String baseDir;
      @Param({"1","3"})
      private int batchSize;
      private LogEntries entries;
      private int index;
      private Log log;

      @Setup(Level.Trial)
      public void setUp() throws Exception {
         index = 1;
         byte[] data = new byte[dataSize];
         Arrays.fill(data, (byte) 1);
         entries = new LogEntries();
         for(int i=0; i < batchSize; i++)
            entries.add(new LogEntry(1, data));
         if ("leveldb".equals(logType)) {
            log = new LevelDBLog();
         } else if ("file".equals(logType)) {
            log = new FileBasedLog();
         } else {
            throw new IllegalArgumentException();
         }
         new File(baseDir).mkdirs();
         log.init(baseDir + "/raft_" + logType, Collections.emptyMap());
      }

      @TearDown
      public void stop() throws Exception {
         if (log != null) {
            log.delete();
         }
      }
   }
}