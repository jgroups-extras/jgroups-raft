#!/bin/bash


`dirname $0`/run.sh -ea org.jgroups.perf.CounterPerf -props raft.xml  $*

