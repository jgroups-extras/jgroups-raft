package org.jgroups.perf;

import org.jgroups.protocols.raft.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
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
@Warmup(iterations = 2, time = 5)
@Measurement(iterations = 4, time = 5)
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
   public void append(Blackhole bh, ExecutionPlan plan) {
      plan.log.append(plan.index, false, plan.entry);
      ++plan.index;
   }

   @State(Scope.Benchmark)
   public static class ExecutionPlan {

      @Param({"10", "100"})
      private int dataSize;
      @Param({"leveldb", "file"})
      private String logType;
      @Param({"/tmp"})
      private String baseDir;
      private LogEntry entry;
      private int index;
      private Log log;

      @Setup(Level.Trial)
      public void setUp() throws Exception {
         index = 1;
         byte[] data = new byte[dataSize];
         Arrays.fill(data, (byte) 1);
         entry = new LogEntry(1, data);
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