#!/bin/bash

## Removes the ELECTION protocols from all running nodes
probe ELECTION.no_elections=true
probe ELECTION.stopElectionTimer[]
probe ELECTION.stopHeartbeatTimer[]
probe rp=ELECTION
