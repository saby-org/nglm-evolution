#!/usr/bin/env bash

#################################################################################
#
#  evolutionengine-run.sh
#
#################################################################################

#
#  sets
#

set -o errexit \
    -o verbose \
    -o xtrace

#
#


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

exec kafka-run-class -name evolutionengine -loggc com.evolving.nglm.evolution.EvolutionEngine /app/runtime $BROKER_SERVERS $KEY $SUBSCRIBERPROFILE_HOST $SUBSCRIBERPROFILE_PORT $INTERNAL_PORT $KAFKA_REPLICATION_FACTOR $KAFKA_STREAMS_STANDBY_REPLICAS $EVOLUTIONENGINE_IN_MEMORY_STATE_STORES $EVOLUTIONENGINE_ROCKSDB_CACHE_MB $EVOLUTIONENGINE_ROCKSDB_MEMTABLE_MB
