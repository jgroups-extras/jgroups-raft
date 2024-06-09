#!/bin/bash

BASEDIR=$(dirname "$0")

# shellcheck disable=SC2086,SC2048
"$BASEDIR"/test-run.sh -ea org.jgroups.perf.Main org.jgroups.perf.replication.ReplicationPerf -props raft-benchmark.xml $*
