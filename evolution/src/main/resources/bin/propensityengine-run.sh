#!/usr/bin/env bash

#################################################################################
#
#  propensityengine-run.sh
#
#################################################################################

#
#  sets
#

set -o errexit \
    -o verbose \
    -o xtrace

#
#  log4j-evol.properties
#

cat /etc/kafka/log4j-evol.properties | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' > /etc/kafka/log4j-evol-final.properties

#
#  wait for zookeeper
#

echo waiting for zookeeper cluster ...
for ZOOKEEPER_SERVER in `echo $ZOOKEEPER_SERVERS | sed 's/,/ /g' | uniq`
do
  cub zk-ready $ZOOKEEPER_SERVER $CUB_ZOOKEEPER_TIMEOUT
  echo zookeeper $ZOOKEEPER_SERVER ready
done  
echo zookeeper cluster ready

#
#  wait for kafka
#

cub kafka-ready -b $BROKER_SERVERS $BROKER_COUNT $CUB_KAFKA_TIMEOUT

#
#  wait for schema registry
#

echo waiting for schema registry cluster ...
for REGISTRY_SERVER in `echo $REGISTRY_SERVERS | sed 's/,/ /g' | uniq`
do
  export REGISTRY_HOST=`echo $REGISTRY_SERVER | cut -d: -f1`
  export REGISTRY_PORT=`echo $REGISTRY_SERVER | cut -d: -f2`
  /app/bin/ev-cub sr-ready $REGISTRY_HOST $REGISTRY_PORT $CUB_SCHEMA_REGISTRY_TIMEOUT
  echo schema registry $REGISTRY_SERVER ready
done 
echo schema registry cluster ready

#
#  wait for deployment
#

echo waiting for deployment ...
DEPLOYED=0
while [ $DEPLOYED = 0 ]; do
  DEPLOYED=`zookeeper-shell $ZOOKEEPER_SERVERS get ${zookeeper.root}/deployed | grep deployed | wc -l`
  if [ $DEPLOYED = 0 ]; then
    echo deployment not yet ready ...
    sleep 5
  fi
done
echo deployment complete

#
#  run
#

exec kafka-run-class -name propensityengine -loggc com.evolving.nglm.evolution.PropensityEngine /app/runtime $BROKER_SERVERS $KEY $KAFKA_REPLICATION_FACTOR $KAFKA_STREAMS_STANDBY_REPLICAS $PROPENSITYENGINE_STREAMTHREADS
