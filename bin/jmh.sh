#!/bin/bash

BASEDIR=$(dirname $0)
ROOTDIR=$(realpath ${BASEDIR}/..)
LIBS="${ROOTDIR}/target/libs"
CLASSES="${ROOTDIR}/target/classes"
TEST_CLASSES="${ROOTDIR}/target/test-classes"
CONFDIR="${ROOTDIR}/conf"

function help() {
  echo "$@"
  echo ""
  echo "tip: run '$0 -h' for JMH options"
  exit 1
}

function check_if_exists_or_exit() {
  [ ! -d "$@" ] && help "Missing '$@' directory. Run: 'mvn clean package -DskipTests' before running the JMH benchmark"
}

check_if_exists_or_exit ${LIBS}
check_if_exists_or_exit ${CLASSES}
check_if_exists_or_exit ${TEST_CLASSES}

java -cp "${LIBS}/*":${CLASSES}:${TEST_CLASSES}:${CONFDIR} ${JAVA_OPTIONS} org.openjdk.jmh.Main org.jgroups.perf.LogJmhBenchmark $@
