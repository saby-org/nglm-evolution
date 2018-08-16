#!/usr/bin/env bash

#################################################################################
#
#  subscribergroup-run.sh
#
#################################################################################

#
#  sets
#

set -o errexit \
    -o verbose \
    -o xtrace

#
#  environment
#

export BROKER_SERVERS=$1
export ZOOKEEPER_SERVERS=$2
export REDIS_SENTINELS=$3
export REGISTRY_URL=$4
export NUM_THREADS=$5
export KAFKA_HEAP_OPTS=$6
export CONSUMER_GROUP_ID=$7
export TRACE_LEVEL=$8
export ARGUMENT_1=$9
export ARGUMENT_2=${10}
export ARGUMENT_3=${11}
export ARGUMENT_4=${12}
export KAFKA_OPTS="-Dbroker.servers=$BROKER_SERVERS -Dnglm.schemaRegistryURL=$REGISTRY_URL -Dredis.sentinels=$REDIS_SENTINELS -Dnglm.converter=Avro -Dzookeeper.connect=$ZOOKEEPER_SERVERS -Dnglm.zookeeper.root=${zookeeper.root}"

#
#  log4j-evol.properties
#

cat /etc/kafka/log4j-evol.properties | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' > /etc/kafka/log4j-evol-final.properties

#
#  run
#

exec kafka-run-class -name subscriberGroupLoader -loggc com.evolving.nglm.evolution.SubscriberGroupLoader $NUM_THREADS $CONSUMER_GROUP_ID "$ARGUMENT_1" "$ARGUMENT_2" "$ARGUMENT_3" "$ARGUMENT_4"
