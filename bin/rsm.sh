#!/bin/bash
### ReplicatedStateMachineDemo

export raft_id="$1"; shift;
export JAVA_OPTS="-Draft_id=$raft_id $JAVA_OPTS"

`dirname $0`/run.sh org.jgroups.raft.demos.ReplicatedStateMachineDemo -props raft.xml  $*

