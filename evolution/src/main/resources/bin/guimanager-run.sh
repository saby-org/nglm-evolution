#!/usr/bin/env bash

#################################################################################
#
#  guimanager-run.sh
#
#################################################################################

#
#  sets
#

set -o errexit \
    -o verbose \
    -o xtrace

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

case "${ENTRYPOINT}" in

  "guimanager")
    exec kafka-run-class -name guiManager -loggc com.evolving.nglm.evolution.GUIManager 001 $BROKER_SERVERS $GUIMANAGER_PORT $ELASTICSEARCH_HOST $ELASTICSEARCH_PORT $ELASTICSEARCH_USERNAME $ELASTICSEARCH_USERPASSWORD
    ;;
  "reportmanager")
    exec kafka-run-class -name reportmanager -loggc com.evolving.nglm.evolution.reports.ReportManager $BROKER_SERVERS ${MASTER_ESROUTER_SERVER}:${ELASTICSEARCH_USERNAME}:${ELASTICSEARCH_USERPASSWORD} $KAFKA_REPLICATION_FACTOR $SUBSCRIBER_PARTITIONS $KAFKA_STREAMS_STANDBY_REPLICAS
    ;;
  "reportscheduler")
    exec kafka-run-class -name reportscheduler -loggc com.evolving.nglm.evolution.reports.ReportScheduler
    ;;
  "thirdpartymanager")
    exec kafka-run-class -name thirdPartyManager -loggc com.evolving.nglm.evolution.ThirdPartyManager $KEY $BROKER_SERVERS $API_PORT $GUI_FWK_API_SERVER $THREADPOOL_SIZE $ELASTICSEARCH_HOST $ELASTICSEARCH_PORT $ELASTICSEARCH_USERNAME $ELASTICSEARCH_USERPASSWORD $GUIMANAGER_HOST $GUIMANAGER_PORT
    ;;
  "dnboproxy")
    exec kafka-run-class -name dnboproxy -loggc com.evolving.nglm.evolution.DNBOProxy $KEY $API_PORT $DNBOPROXY_THREADS
    ;;
  "datacubemanager")  
    exec kafka-run-class -name datacubemanager -loggc com.evolving.nglm.evolution.datacubes.DatacubeManager /app/runtime $BROKER_SERVERS 001 $ELASTICSEARCH_HOST $ELASTICSEARCH_PORT $ELASTICSEARCH_USERNAME $ELASTICSEARCH_USERPASSWORD
    ;;
  "notificationmanagermail")
    exec kafka-run-class -name notificationmanagermail -loggc com.evolving.nglm.evolution.MailNotificationManager $KEY $PLUGIN_NAME $PLUGIN_CONFIGURATION
    ;;
  "notificationmanagerpush")
    exec kafka-run-class -name notificationmanagerpush -loggc com.evolving.nglm.evolution.PushNotificationManager $KEY $PLUGIN_NAME $PLUGIN_CONFIGURATION
    ;;
  "notificationmanagersms")
   exec kafka-run-class -name notificationmanagersms -loggc com.evolving.nglm.evolution.SMSNotificationManager $KEY $PLUGIN_NAME $PLUGIN_CONFIGURATION 
    ;;
  "notificationmanager")
   exec kafka-run-class -name notificationmanagersms -loggc com.evolving.nglm.evolution.NotificationManager $KEY $PLUGIN_NAME $PLUGIN_CONFIGURATION 
    ;;  
  "emptyfulfillmentmanager")
    exec kafka-run-class -name emptyFulfillmentManager -loggc com.evolving.nglm.evolution.EmptyFulfillmentManager $KEY $PLUGIN_NAME
    ;;
  "infulfillmentmanager")
    exec kafka-run-class -name inFulfillmentManager -loggc com.evolving.nglm.evolution.INFulfillmentManager $KEY $PLUGIN_NAME $PLUGIN_CONFIGURATION
    ;;
  "commoditydeliverymanager")
    exec kafka-run-class -name commodityDeliveryManager -loggc com.evolving.nglm.evolution.CommodityDeliveryManager $KEY $COMMODITYDELIVERYMANAGER_INSTANCES
    ;;
  "purchasefulfillment")
    exec kafka-run-class -name purchaseFulfillmentManager -loggc com.evolving.nglm.evolution.PurchaseFulfillmentManager $KEY $ELASTICSEARCH_HOST $ELASTICSEARCH_PORT $ELASTICSEARCH_USERNAME $ELASTICSEARCH_USERPASSWORD
    ;;
  "ucgengine")
    exec kafka-run-class -name ucgengine -loggc com.evolving.nglm.evolution.UCGEngine /app/runtime $BROKER_SERVERS $ELASTICSEARCH_HOST $ELASTICSEARCH_PORT $ELASTICSEARCH_USERNAME $ELASTICSEARCH_USERPASSWORD
    ;;
  "evolutionengine")
    exec kafka-run-class -name evolutionengine -loggc com.evolving.nglm.evolution.EvolutionEngine /app/runtime $BROKER_SERVERS $KEY $SUBSCRIBERPROFILE_HOST $SUBSCRIBERPROFILE_PORT $INTERNAL_PORT $KAFKA_REPLICATION_FACTOR $KAFKA_STREAMS_STANDBY_REPLICAS $ELASTICSEARCH_HOST $ELASTICSEARCH_PORT $ELASTICSEARCH_USERNAME $ELASTICSEARCH_USERPASSWORD $EVOLUTIONENGINE_IN_MEMORY_STATE_STORES $EVOLUTIONENGINE_ROCKSDB_CACHE_MB $EVOLUTIONENGINE_ROCKSDB_MEMTABLE_MB 
    ;;
  "extractmanager")
    exec kafka-run-class -name extractmanager -loggc com.evolving.nglm.evolution.extracts.ExtractManager $BROKER_SERVERS $MASTER_ESROUTER_SERVER:${ELASTICSEARCH_USERNAME}:${ELASTICSEARCH_USERPASSWORD} $KAFKA_REPLICATION_FACTOR $SUBSCRIBER_PARTITIONS $KAFKA_STREAMS_STANDBY_REPLICAS
    ;;
  *)
    echo -n "unknown"
    ;;
esac
