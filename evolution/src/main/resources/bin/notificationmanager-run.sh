#!/usr/bin/env bash

#################################################################################
#
#  notificationger-run.sh
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

exec kafka-run-class -name notificationmanager -loggc com.evolving.nglm.evolution.NotificationManager $KEY $PLUGIN_NAME $PLUGIN_CONFIGURATION
