#!/usr/bin/env bash

#################################################################################
#
#  storeconfiguration-run.sh
#
#################################################################################

#
#  sets
#

set -o errexit

#
#  environment
#

export OPERATION=$1
export GUIMANAGER_HOST=$2
export GUIMANAGER_PORT=$3
export CONFIGURATION_STORE_FILE=$4
export TRACE_LEVEL=$5

#
#  log4j-evol.properties
#

cat /etc/kafka/log4j-evol.properties | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' > /etc/kafka/log4j-evol-final.properties

#
#  run
#

exec kafka-run-class -name storeConfiguration -loggc com.evolving.nglm.evolution.StoreConfiguration $OPERATION $GUIMANAGER_HOST $GUIMANAGER_PORT $CONFIGURATION_STORE_FILE
