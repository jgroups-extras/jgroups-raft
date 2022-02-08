#!/bin/bash


`dirname $0`/run.sh -ea -Dlog4j.configurationFile=$HOME/log4j2.xml org.jgroups.raft.util.AnalyzeLog  $*
