#!/usr/bin/env bash

#################################################################################
#
#  thirdpartymanager-run.sh
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

exec kafka-run-class -name thirdPartyManager -loggc com.evolving.nglm.evolution.ThirdPartyManager $KEY $BROKER_SERVERS $API_PORT $GUI_FWK_API_SERVER $THREADPOOL_SIZE $ELASTICSEARCH_HOST $ELASTICSEARCH_PORT
