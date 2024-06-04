#!/bin/bash

# Same as `run.sh` but for running classes in the test sources.

function check_if_exists_or_exit() {
  [ ! -d "$@" ] && help "Missing '$@' directory. Run: 'mvn clean package -DskipTests' before running"
}

MCAST_ADDR=232.5.5.5
BIN_DIR=$(dirname "$0")

# Project built with Maven.
LIB=${BIN_DIR}/../target/libs
CLASSES=${BIN_DIR}/../target/classes
TEST_CLASSES=${BIN_DIR}/../target/test-classes
CONF=${BIN_DIR}/../conf

check_if_exists_or_exit ${LIBS}
check_if_exists_or_exit ${CLASSES}
check_if_exists_or_exit ${TEST_CLASSES}

CP="$CLASSES:$CONF:$TEST_CLASSES:$LIB/*"
LOG="-Dlog4j.configurationFile=log4j2.xml"

JG_FLAGS="-Djgroups.udp.mcast_addr=$MCAST_ADDR"
JG_FLAGS="$JG_FLAGS -Djava.net.preferIPv4Stack=true"

FLAGS="-server -Xmx600M -Xms600M"
FLAGS="$FLAGS -XX:CompileThreshold=10000 -XX:ThreadStackSize=64K -XX:SurvivorRatio=8"
FLAGS="$FLAGS -XX:TargetSurvivorRatio=90 -XX:MaxTenuringThreshold=15"
FLAGS="$FLAGS -Xshare:off"

#GC="-XX:+UseParNewGC -XX:+UseConcMarkSweepGC" ## concurrent mark and sweep (CMS) collector

EXPERIMENTAL="$EXPERIMENTAL -XX:+EliminateLocks"
#DEBUG="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:8787"

# shellcheck disable=SC2086,SC2048
java -cp $CP $DEBUG $LOG $GC $JG_FLAGS $FLAGS $EXPERIMENTAL $JAVA_OPTIONS $*
