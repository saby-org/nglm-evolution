#!/usr/bin/env bash

#################################################################################
#
#  commoditydeliverymanager-run.sh
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
#  wait for services
#

/app/bin/ev-cub zk-ready $ZOOKEEPER_SERVERS $CUB_ZOOKEEPER_MIN_NODES $CUB_ZOOKEEPER_TIMEOUT
/app/bin/ev-cub kafka-ready -b $BROKER_SERVERS $CUB_BROKER_MIN_NODES $CUB_KAFKA_TIMEOUT
/app/bin/ev-cub sr-ready $REGISTRY_SERVERS $CUB_REGISTRY_MIN_NODES $CUB_SCHEMA_REGISTRY_TIMEOUT

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

exec kafka-run-class -name commodityDeliveryManager -loggc com.evolving.nglm.evolution.CommodityDeliveryManager $KEY
