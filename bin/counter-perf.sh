#!/bin/bash

BASEDIR=$(dirname "$0")

# shellcheck disable=SC2086,SC2048
"$BASEDIR"/test-run.sh -ea org.jgroups.perf.counter.CounterPerf -props raft.xml $*

