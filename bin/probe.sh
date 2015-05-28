#!/bin/sh

# Discovers all UDP-based members running on a certain mcast address (use -help for help)
# Probe [-help] [-addr <addr>] [-port <port>] [-ttl <ttl>] [-timeout <timeout>]



`dirname $0`/run.sh org.jgroups.tests.Probe $*
