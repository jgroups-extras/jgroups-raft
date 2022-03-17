#!/bin/bash

### Configurable properties:

## bind address, set the network interface to use for clustering traffic
#BIND_ADDR=192.168.1.5
#BIND_ADDR=match-interface:en.*
#BIND_ADDR=site_local

################# CHANGE THIS ##############################
#BIND_ADDR=match-address:192.168.1.*
#BIND_ADDR=127.0.0.1
############################################################

MCAST_ADDR=232.5.5.5




LIB=`dirname $0`/../lib
CLASSES=`dirname $0`/../classes
CONF=`dirname $0`/../conf

CP=$CLASSES:$CONF:$LIB/*
LOG="-Dlog4j.configurationFile=$CONF/log4j2.xml"


JG_FLAGS="-Djgroups.udp.mcast_addr=$MCAST_ADDR"
JG_FLAGS="$JG_FLAGS -Djava.net.preferIPv4Stack=true"
FLAGS="-server -Xmx600M -Xms600M"
FLAGS="$FLAGS -XX:CompileThreshold=10000 -XX:ThreadStackSize=64K -XX:SurvivorRatio=8"
FLAGS="$FLAGS -XX:TargetSurvivorRatio=90 -XX:MaxTenuringThreshold=15"
FLAGS="$FLAGS -Xshare:off"
#GC="-XX:+UseParNewGC -XX:+UseConcMarkSweepGC" ## concurrent mark and sweep (CMS) collector

JMX="-Dcom.sun.management.jmxremote"
EXPERIMENTAL="$EXPERIMENTAL -XX:+EliminateLocks"
#JMC="-XX:+UnlockCommercialFeatures -XX:+FlightRecorder"
# DEBUG="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:8787"

java -cp $CP $DEBUG $LOG $GC $JG_FLAGS $FLAGS $EXPERIMENTAL $JMX $JMC $*

