#!/bin/bash
### ReplicatedStateMachineDemo

export raft_id="$1"; shift;
export JAVA_OPTS="-Draft_id=$raft_id $JAVA_OPTS"

`dirname $0`/test-run.sh org.jgroups.raft.ReplicatedStateMachineDemo -props raft.xml  $*

