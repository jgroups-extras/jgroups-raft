#!/bin/bash


`dirname $0`/run.sh org.jgroups.perf.CounterPerf -props raft.xml  $*

