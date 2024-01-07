# JGroups Raft benchmarks

This subproject includes all the benchmarks created utilizing JMH.

## How to

This specific repository uses the latest Java version available. Currently, Java 21.
To compile the project and runs the benchmarks.
First, execute:

```bash
$ mvn clean verify
```

The output is located at `./target/benchmarks.jar`.
Now, to execute a benchmark, check the current implementations and select the name.
For example, for `MyBenchmark`:

```bash
$ java -jar target/benchmarks.jar "MyBenchmark"
```

To pass configuration for `MyBenchmark`, execute:

```bash
$ java -jar target/benchmarks.jar -pkey1=value1 -pkey2=value2 "MyBenchmark"
```

For more options, related to JMH:

```bash
$ java -jar target/benchmarks.jar -h
```

## Current benchmarks

The current list of benchmarks include:

1. `DataReplicationBenchmark`: Benchmark the complete data replication utilizing `RaftHandle`;
