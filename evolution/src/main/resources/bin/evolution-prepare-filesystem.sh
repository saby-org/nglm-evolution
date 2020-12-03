#################################################################################
#
#  evolution-prepare-filesystem.sh
#
#################################################################################


#########################################
#
#  create required filesystem structure
#
#########################################

#
#  nglm runtime
#

for SWARM_HOST in $SWARM_HOSTS
do
   ssh $SWARM_HOST "
      mkdir -p $NGLM_STREAMS_RUNTIME
   "
done

#
#  nglm data
#

ssh $MASTER_SWARM_HOST "
   mkdir -p $NGLM_DATA
"

#
#  nglm file upload
#

ssh $GUIMANAGER_HOST "
   mkdir -p $NGLM_UPLOADED
   mkdir -p $NGLM_REPORTS  
"
cat $DEPLOY_ROOT/config/logger/log4j-guimanager.xml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' > $DEPLOY_ROOT/config/logger/log4j-guimanager-001.xml
scp $DEPLOY_ROOT/config/logger/log4j-guimanager-001.xml $GUIMANAGER_HOST:$NGLM_CONFIG_LOGS/log4j-guimanager.xml
rm -f $DEPLOY_ROOT/config/logger/log4j-guimanager-001.xml

#
#  prometheus runtime volume(s)
#

ssh $MASTER_SWARM_HOST "
   mkdir -p $NGLM_CORE_RUNTIME/prometheus/data-application
"

#
#  evolutionengine
#

EVOLUTIONENGINE_CONFIGURATION=`echo $EVOLUTIONENGINE_CONFIGURATION | sed 's/ /\n/g' | uniq`
for TUPLE in $EVOLUTIONENGINE_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export SUBSCRIBERPROFILE_PORT=`echo $TUPLE | cut -d: -f3`
   export INTERNAL_PORT=`echo $TUPLE | cut -d: -f4`
   export MONITORING_PORT=`echo $TUPLE | cut -d: -f5`
   export DEBUG_PORT=`echo $TUPLE | cut -d: -f6`
   cat $DEPLOY_ROOT/config/logger/log4j-evolutionengine.xml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' > $DEPLOY_ROOT/config/logger/log4j-evolutionengine-$KEY.xml
   scp $DEPLOY_ROOT/config/logger/log4j-evolutionengine-$KEY.xml $HOST:$NGLM_CONFIG_LOGS/log4j-evolutionengine-$KEY.xml
   rm -f $DEPLOY_ROOT/config/logger/log4j-evolutionengine-$KEY.xml

   ssh $HOST "
      mkdir -p $NGLM_STREAMS_RUNTIME/streams-evolutionengine-$KEY      
   "   
done

#
#  thirdpartymanager
#

THIRDPARTYMANAGER_CONFIGURATION=`echo $THIRDPARTYMANAGER_CONFIGURATION | sed 's/ /\n/g' | uniq`
for TUPLE in $THIRDPARTYMANAGER_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export API_PORT=`echo $TUPLE | cut -d: -f3`
   export MONITORING_PORT=`echo $TUPLE | cut -d: -f4`
   export THREADPOOL_SIZE=`echo $TUPLE | cut -d: -f5`
   export DEBUG_PORT=`echo $TUPLE | cut -d: -f6`
   cat $DEPLOY_ROOT/config/logger/log4j-thirdpartyevent.xml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' > $DEPLOY_ROOT/config/logger/log4j-thirdpartyevent-$KEY.xml
   scp $DEPLOY_ROOT/config/logger/log4j-thirdpartyevent-$KEY.xml $HOST:$NGLM_CONFIG_LOGS/log4j-thirdpartyevent-$KEY.xml
   rm -f $DEPLOY_ROOT/config/logger/log4j-thirdpartyevent-$KEY.xml
 done

 #
 #  infulfillmentmanager
 #

if [ "$INFULFILLMENTMANAGER_ENABLED" = "true" ]; then 

  for TUPLE in $INFULFILLMENTMANAGER_CONFIGURATION
  do
     export KEY=`echo $TUPLE | cut -d: -f1`
     export HOST=`echo $TUPLE | cut -d: -f2`

     cat $DEPLOY_ROOT/config/logger/log4j-infulfillment.xml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' > $DEPLOY_ROOT/config/logger/log4j-infulfillment-$KEY.xml
     scp $DEPLOY_ROOT/config/logger/log4j-infulfillment-$KEY.xml $HOST:$NGLM_CONFIG_LOGS/log4j-infulfillment-$KEY.xml
     rm -f $DEPLOY_ROOT/config/logger/log4j-infulfillment-$KEY.xml

  done

fi 

#
#  commoditydeliverymanager
#

if [ "$COMMODITYDELIVERYMANAGER_ENABLED" = "true" ]; then

  for TUPLE in $COMMODITYDELIVERYMANAGER_CONFIGURATION
  do
     export KEY=`echo $TUPLE | cut -d: -f1`
     export HOST=`echo $TUPLE | cut -d: -f2`

     cat $DEPLOY_ROOT/config/logger/log4j-commoditydelivery.xml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' > $DEPLOY_ROOT/config/logger/log4j-commoditydelivery-$KEY.xml
     scp $DEPLOY_ROOT/config/logger/log4j-commoditydelivery-$KEY.xml $HOST:$NGLM_CONFIG_LOGS/log4j-commoditydelivery-$KEY.xml
     rm -f $DEPLOY_ROOT/config/logger/log4j-commoditydelivery-$KEY.xml

  done

fi

#
#  purchasefulfillmentmanager
#

if [ "$PURCHASEFULFILLMENTMANAGER_ENABLED" = "true" ]; then

  for TUPLE in $PURCHASEFULFILLMENTMANAGER_CONFIGURATION
  do
     export KEY=`echo $TUPLE | cut -d: -f1`
     export HOST=`echo $TUPLE | cut -d: -f2`

     cat $DEPLOY_ROOT/config/logger/log4j-purchasefulfillment.xml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' > $DEPLOY_ROOT/config/logger/log4j-purchasefulfillment-$KEY.xml
     scp $DEPLOY_ROOT/config/logger/log4j-purchasefulfillment-$KEY.xml $HOST:$NGLM_CONFIG_LOGS/log4j-purchasefulfillment-$KEY.xml
     rm -f $DEPLOY_ROOT/config/logger/log4j-purchasefulfillment-$KEY.xml

  done

fi

#
#  notificationmanagersms
#

if [ "$NOTIFICATIONMANAGER_SMS_ENABLED" = "true" ]; then

  for TUPLE in $NOTIFICATIONMANAGER_SMS_CONFIGURATION
  do
     export KEY=`echo $TUPLE | cut -d: -f1`
     export HOST=`echo $TUPLE | cut -d: -f2`
     
     cat $DEPLOY_ROOT/config/logger/log4j-sms.xml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' > $DEPLOY_ROOT/config/logger/log4j-sms-$KEY.xml
     scp $DEPLOY_ROOT/config/logger/log4j-sms-$KEY.xml $HOST:$NGLM_CONFIG_LOGS/log4j-sms-$KEY.xml
     rm -f $DEPLOY_ROOT/config/logger/log4j-sms-$KEY.xml
     
  done
  
fi

#
#  notificationmanagermail
#

if [ "$NOTIFICATIONMANAGER_MAIL_ENABLED" = "true" ]; then

  for TUPLE in $NOTIFICATIONMANAGER_MAIL_CONFIGURATION
  do
     export KEY=`echo $TUPLE | cut -d: -f1`
     export HOST=`echo $TUPLE | cut -d: -f2`

     cat $DEPLOY_ROOT/config/logger/log4j-mail.xml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' > $DEPLOY_ROOT/config/logger/log4j-mail-$KEY.xml
     scp $DEPLOY_ROOT/config/logger/log4j-mail-$KEY.xml $HOST:$NGLM_CONFIG_LOGS/log4j-mail-$KEY.xml
     rm -f $DEPLOY_ROOT/config/logger/log4j-mail-$KEY.xml

  done

fi

#
#  notificationmanagerpush
#

if [ "$NOTIFICATIONMANAGER_PUSH_ENABLED" = "true" ]; then

  for TUPLE in $NOTIFICATIONMANAGER_PUSH_CONFIGURATION
  do
     export KEY=`echo $TUPLE | cut -d: -f1`
     export HOST=`echo $TUPLE | cut -d: -f2`

     cat $DEPLOY_ROOT/config/logger/log4j-push.xml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' > $DEPLOY_ROOT/config/logger/log4j-push-$KEY.xml
     scp $DEPLOY_ROOT/config/logger/log4j-push-$KEY.xml $HOST:$NGLM_CONFIG_LOGS/log4j-push-$KEY.xml
     rm -f $DEPLOY_ROOT/config/logger/log4j-push-$KEY.xml

  done

fi

#
#  notificationmanager
#

if [ "$NOTIFICATIONMANAGER_ENABLED" = "true" ]; then

  for TUPLE in $NOTIFICATIONMANAGER_CONFIGURATION
  do  
     export KEY=`echo $TUPLE | cut -d: -f1`
     export HOST=`echo $TUPLE | cut -d: -f2`

     cat $DEPLOY_ROOT/config/logger/log4j-notificationmanager.xml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' > $DEPLOY_ROOT/config/logger/log4j-notificationmanager-$KEY.xml
     scp $DEPLOY_ROOT/config/logger/log4j-notificationmanager-$KEY.xml $HOST:$NGLM_CONFIG_LOGS/log4j-notificationmanager-$KEY.xml
     rm -f $DEPLOY_ROOT/config/logger/log4j-notificationmanager-$KEY.xml

  done   

fi



#
#  dnboproxy
#

DNBOPROXY_CONFIGURATION=`echo $DNBOPROXY_CONFIGURATION | sed 's/ /\n/g' | uniq`
for TUPLE in $DNBOPROXY_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export API_PORT=`echo $TUPLE | cut -d: -f3`
   export MONITORING_PORT=`echo $TUPLE | cut -d: -f4`
   #
   # nothing to prepare
   #
done

#
#  subscriber group
#

ssh $MASTER_SWARM_HOST "
   mkdir -p $NGLM_SUBSCRIBERGROUP_DATA
"

#
#  storeconfiguration
#

ssh $MASTER_SWARM_HOST "
   mkdir -p $NGLM_STORECONFIGURATION_DATA
"

#
#  Report Manager
#

REPORTMANAGER_CONFIGURATION=`echo $REPORTMANAGER_CONFIGURATION | sed 's/ /\n/g' | uniq`
for TUPLE in $REPORTMANAGER_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export MONITORING_PORT=`echo $TUPLE | cut -d: -f3`
   export DEBUG_PORT=`echo $TUPLE | cut -d: -f4`
   cat $DEPLOY_ROOT/config/logger/log4j-reportmanager.xml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' > $DEPLOY_ROOT/config/logger/log4j-reportmanager-$KEY.xml
   scp $DEPLOY_ROOT/config/logger/log4j-reportmanager-$KEY.xml $HOST:$NGLM_CONFIG_LOGS/log4j-reportmanager-$KEY.xml
   rm -f $DEPLOY_ROOT/config/logger/log4j-reportmanager-$KEY.xml
      ssh $HOST "
      mkdir -p $NGLM_REPORTS
   "
done

#
#  Report Scheduler
#

REPORTSCHEDULER_CONFIGURATION=`echo $REPORTSCHEDULER_CONFIGURATION | sed 's/ /\n/g' | uniq`
for TUPLE in $REPORTSCHEDULER_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export MONITORING_PORT=`echo $TUPLE | cut -d: -f3`
   export DEBUG_PORT=`echo $TUPLE | cut -d: -f4`
   cat $DEPLOY_ROOT/config/logger/log4j-reportscheduler.xml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' > $DEPLOY_ROOT/config/logger/log4j-reportscheduler-$KEY.xml
   scp $DEPLOY_ROOT/config/logger/log4j-reportscheduler-$KEY.xml $HOST:$NGLM_CONFIG_LOGS/log4j-reportscheduler-$KEY.xml
   rm -f $DEPLOY_ROOT/config/logger/log4j-reportscheduler-$KEY.xml

      ssh $HOST "
      mkdir -p $NGLM_REPORTS
   "
done

#
# Datacube Manager
#

if [ "$DATACUBEMANAGER_ENABLED" = "true" ]; then
   export HOST=$DATACUBEMANAGER_HOST
   cat $DEPLOY_ROOT/config/logger/log4j-datacubemanager.xml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' > $DEPLOY_ROOT/config/logger/log4j-datacubemanager-001.xml
   scp $DEPLOY_ROOT/config/logger/log4j-datacubemanager-001.xml $HOST:$NGLM_CONFIG_LOGS/log4j-datacubemanager.xml
   rm -f $DEPLOY_ROOT/config/logger/log4j-datacubemanager-001.xml
fi


#
#  Extract Manager
#
EXTRACTMANAGER_CONFIGURATION=`echo $EXTRACTMANAGER_CONFIGURATION | sed 's/ /\n/g' | uniq`
for TUPLE in $EXTRACTMANAGER_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export MONITORING_PORT=`echo $TUPLE | cut -d: -f3`
   export DEBUG_PORT=`echo $TUPLE | cut -d: -f4`
      ssh $HOST "
      mkdir -p $NGLM_EXTRACTS
   "
done

#
#  MySQL - GUI
#

MYSQL_GUI_CONFIGURATION=`echo $MYSQL_GUI_CONFIGURATION | sed 's/ /\n/g' | uniq`
for TUPLE in $MYSQL_GUI_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   ssh $HOST "
      mkdir -p $NGLM_MYSQL_RUNTIME/mysql-gui-$KEY \
        $NGLM_MYSQL_RUNTIME/mysql-gui-$KEY/.mysql-keyring
   "
done

#
# FWK components
#

GUI_FWK_API_CONFIGURATION=`echo $GUI_FWK_API_CONFIGURATION | sed 's/ /\n/g' | uniq`
for TUPLE in $GUI_FWK_API_CONFIGURATION
do
  export KEY=`echo $TUPLE | cut -d: -f1`
  export HOST=`echo $TUPLE | cut -d: -f2`
  export HOST_IP=`echo $TUPLE | cut -d: -f3`
  export HOST_EXTERNAL_IP=`echo $TUPLE | cut -d: -f4`
  export PORT=`echo $TUPLE | cut -d: -f5`
  ssh $HOST "
      mkdir -p $NGLM_GUI_RUNTIME/fwk-api-$KEY/mnt
   "
done

GUI_FWK_AUTH_CONFIGURATION=`echo $GUI_FWK_AUTH_CONFIGURATION | sed 's/ /\n/g' | uniq`
for TUPLE in $GUI_FWK_AUTH_CONFIGURATION
do
  export KEY=`echo $TUPLE | cut -d: -f1`
  export HOST=`echo $TUPLE | cut -d: -f2`
  export HOST_IP=`echo $TUPLE | cut -d: -f3`
  export HOST_EXTERNAL_IP=`echo $TUPLE | cut -d: -f4`
  export PORT=`echo $TUPLE | cut -d: -f5`
  ssh $HOST "
      mkdir -p $NGLM_GUI_RUNTIME/fwkauth-api-$KEY/mnt
   "
done

GUI_ITM_API_CONFIGURATION=`echo $GUI_ITM_API_CONFIGURATION | sed 's/ /\n/g' | uniq`
for TUPLE in $GUI_ITM_API_CONFIGURATION
do
  export KEY=`echo $TUPLE | cut -d: -f1`
  export HOST=`echo $TUPLE | cut -d: -f2`
  export HOST_IP=`echo $TUPLE | cut -d: -f3`
  export HOST_EXTERNAL_IP=`echo $TUPLE | cut -d: -f4`
  export PORT=`echo $TUPLE | cut -d: -f5`
  ssh $HOST "
      mkdir -p $NGLM_GUI_RUNTIME/itm-api-$KEY/mnt
   "
done

GUI_JMR_API_CONFIGURATION=`echo $GUI_JMR_API_CONFIGURATION | sed 's/ /\n/g' | uniq`
for TUPLE in $GUI_JMR_API_CONFIGURATION
do
  export KEY=`echo $TUPLE | cut -d: -f1`
  export HOST=`echo $TUPLE | cut -d: -f2`
  export HOST_IP=`echo $TUPLE | cut -d: -f3`
  export HOST_EXTERNAL_IP=`echo $TUPLE | cut -d: -f4`
  export PORT=`echo $TUPLE | cut -d: -f5`
  ssh $HOST "
      mkdir -p $NGLM_GUI_RUNTIME/jmr-api-$KEY/mnt
   "
done

GUI_OPC_API_CONFIGURATION=`echo $GUI_OPC_API_CONFIGURATION | sed 's/ /\n/g' | uniq`
for TUPLE in $GUI_OPC_API_CONFIGURATION
do
  export KEY=`echo $TUPLE | cut -d: -f1`
  export HOST=`echo $TUPLE | cut -d: -f2`
  export HOST_IP=`echo $TUPLE | cut -d: -f3`
  export HOST_EXTERNAL_IP=`echo $TUPLE | cut -d: -f4`
  export PORT=`echo $TUPLE | cut -d: -f5`
  ssh $HOST "
      mkdir -p $NGLM_GUI_RUNTIME/opc-api-$KEY/mnt
   "
done

GUI_CSR_API_CONFIGURATION=`echo $GUI_CSR_API_CONFIGURATION | sed 's/ /\n/g' | uniq`
for TUPLE in $GUI_CSR_API_CONFIGURATION
do
  export KEY=`echo $TUPLE | cut -d: -f1`
  export HOST=`echo $TUPLE | cut -d: -f2`
  export HOST_IP=`echo $TUPLE | cut -d: -f3`
  export HOST_EXTERNAL_IP=`echo $TUPLE | cut -d: -f4`
  export PORT=`echo $TUPLE | cut -d: -f5`
  ssh $HOST "
      mkdir -p $NGLM_GUI_RUNTIME/csr-api-$KEY/mnt
   "
done

GUI_IAR_API_CONFIGURATION=`echo $GUI_IAR_API_CONFIGURATION | sed 's/ /\n/g' | uniq`
for TUPLE in $GUI_IAR_API_CONFIGURATION
do
  export KEY=`echo $TUPLE | cut -d: -f1`
  export HOST=`echo $TUPLE | cut -d: -f2`
  export HOST_IP=`echo $TUPLE | cut -d: -f3`
  export HOST_EXTERNAL_IP=`echo $TUPLE | cut -d: -f4`
  export PORT=`echo $TUPLE | cut -d: -f5`
  ssh $HOST "
      mkdir -p $NGLM_GUI_RUNTIME/iar-api-$KEY/mnt
   "
done

GUI_OPR_API_CONFIGURATION=`echo $GUI_OPR_API_CONFIGURATION | sed 's/ /\n/g' | uniq`
for TUPLE in $GUI_OPR_API_CONFIGURATION
do
  export KEY=`echo $TUPLE | cut -d: -f1`
  export HOST=`echo $TUPLE | cut -d: -f2`
  export HOST_IP=`echo $TUPLE | cut -d: -f3`
  export HOST_EXTERNAL_IP=`echo $TUPLE | cut -d: -f4`
  export PORT=`echo $TUPLE | cut -d: -f5`
  ssh $HOST "
      mkdir -p $NGLM_GUI_RUNTIME/opr-api-$KEY/mnt
   "
done

GUI_STG_API_CONFIGURATION=`echo $GUI_STG_API_CONFIGURATION | sed 's/ /\n/g' | uniq`
for TUPLE in $GUI_STG_API_CONFIGURATION
do
  export KEY=`echo $TUPLE | cut -d: -f1`
  export HOST=`echo $TUPLE | cut -d: -f2`
  export HOST_IP=`echo $TUPLE | cut -d: -f3`
  export HOST_EXTERNAL_IP=`echo $TUPLE | cut -d: -f4`
  export PORT=`echo $TUPLE | cut -d: -f5`
  ssh $HOST "
      mkdir -p $NGLM_GUI_RUNTIME/stg-api-$KEY/mnt
   "
done

GUI_SBM_API_CONFIGURATION=`echo $GUI_SBM_API_CONFIGURATION | sed 's/ /\n/g' | uniq`
for TUPLE in $GUI_SBM_API_CONFIGURATION
do
  export KEY=`echo $TUPLE | cut -d: -f1`
  export HOST=`echo $TUPLE | cut -d: -f2`
  export HOST_IP=`echo $TUPLE | cut -d: -f3`
  export HOST_EXTERNAL_IP=`echo $TUPLE | cut -d: -f4`
  export PORT=`echo $TUPLE | cut -d: -f5`
  ssh $HOST "
      mkdir -p $NGLM_GUI_RUNTIME/sbm-api-$KEY/mnt
   "
done

GUI_LPM_API_CONFIGURATION=`echo $GUI_LPM_API_CONFIGURATION | sed 's/ /\n/g' | uniq`
for TUPLE in $GUI_LPM_API_CONFIGURATION
do
  export KEY=`echo $TUPLE | cut -d: -f1`
  export HOST=`echo $TUPLE | cut -d: -f2`
  export HOST_IP=`echo $TUPLE | cut -d: -f3`
  export HOST_EXTERNAL_IP=`echo $TUPLE | cut -d: -f4`
  export PORT=`echo $TUPLE | cut -d: -f5`
  ssh $HOST "
      mkdir -p $NGLM_GUI_RUNTIME/lpm-api-$KEY/mnt
   "
done
