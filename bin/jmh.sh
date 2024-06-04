#!/bin/bash

BASEDIR=$(dirname "$0")

function help() {
  echo ""
  echo "tip: pass '-h' for JMH options"
  exit 1
}

function list_benchmarks() {
  echo "Available benchmarks:"
  printf "\tDataReplicationBenchmark\n"
  printf "\tLogJmhBenchmark\n"
  printf "\tStorageAppenderBenchmark\n"
  help;
}

BENCHMARK="$1"
shift;

if [ "$BENCHMARK" = "-list" ]; then
    list_benchmarks
    exit 0;
fi

# shellcheck disable=SC2086,SC2048
"$BASEDIR"/test-run.sh -ea org.openjdk.jmh.Main "$BENCHMARK" $*
