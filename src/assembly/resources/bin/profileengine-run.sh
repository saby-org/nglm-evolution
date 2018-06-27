#!/usr/bin/env bash

#################################################################################
#
#  profileengine-run.sh
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
#  wait for kafka / schema registry
#

cub kafka-ready -b $BROKER_SERVERS $BROKER_COUNT $CUB_KAFKA_TIMEOUT
cub sr-ready $MASTER_REGISTRY_HOST $MASTER_REGISTRY_PORT $CUB_SCHEMA_REGISTRY_TIMEOUT

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

exec kafka-run-class -name profileengine -loggc com.evolving.nglm.evolution.ProfileEngine /app/runtime $BROKER_SERVERS $KEY $KAFKA_REPLICATION_FACTOR $KAFKA_STREAMS_STANDBY_REPLICAS ${profileengine.streamthreads}
