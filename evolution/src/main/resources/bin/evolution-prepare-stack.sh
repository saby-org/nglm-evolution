#################################################################################
#
#  evolution-prepare-stack.sh
#
#################################################################################

#########################################
#
#  construct resources
#
#########################################

#
#  update-subscribergroup.sh
#

cat $DEPLOY_ROOT/bin/resources/update-subscribergroup.sh | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' > $DEPLOY_ROOT/bin/update-subscribergroup.sh
chmod 755 $DEPLOY_ROOT/bin/update-subscribergroup.sh

#
#  storeconfiguration.sh
#

cat $DEPLOY_ROOT/bin/resources/storeconfiguration.sh | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' > $DEPLOY_ROOT/bin/storeconfiguration.sh
chmod 755 $DEPLOY_ROOT/bin/storeconfiguration.sh

#########################################
#
#  construct stack -- application monitoring
#
#########################################

#
#  preamble
#

mkdir -p $DEPLOY_ROOT/stack
cat $DEPLOY_ROOT/docker/stack-preamble.yml > $DEPLOY_ROOT/stack/stack-application-monitoring.yml

#
#  prometheus-application -- services
#

cat $DEPLOY_ROOT/docker/prometheus-application.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-application-monitoring.yml

#
#  kafka-lag-exporter
#

cat $DEPLOY_ROOT/docker/kafka-lag-exporter.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-application-monitoring.yml

#
#  postamble
#

echo >> $DEPLOY_ROOT/stack/stack-application-monitoring.yml
cat $DEPLOY_ROOT/docker/stack-postamble.yml >> $DEPLOY_ROOT/stack/stack-application-monitoring.yml

#########################################
#
#  construct stack -- guimanager
#
#########################################

#
#  preamble
#

mkdir -p $DEPLOY_ROOT/stack
cat $DEPLOY_ROOT/docker/stack-preamble.yml > $DEPLOY_ROOT/stack/stack-guimanager.yml

#
#  guimanager
#

cat $DEPLOY_ROOT/docker/guimanager.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-guimanager.yml
echo >> $DEPLOY_ROOT/stack/stack-guimanager.yml

#
#  postamble
#

cat $DEPLOY_ROOT/docker/stack-postamble.yml >> $DEPLOY_ROOT/stack/stack-guimanager.yml

#########################################
#
#  construct stack -- thirdpartymanager(if necessary)
#
#########################################

if [ "$THIRDPARTYMANAGER_ENABLED" = "true" ]; then

  #
  #  preamble
  #

  mkdir -p $DEPLOY_ROOT/stack
  cat $DEPLOY_ROOT/docker/stack-preamble.yml > $DEPLOY_ROOT/stack/stack-thirdpartymanager.yml

  #
  #  thirdpartymanager
  #

  for TUPLE in $THIRDPARTYMANAGER_CONFIGURATION
  do
     export KEY=`echo $TUPLE | cut -d: -f1`
     export HOST=`echo $TUPLE | cut -d: -f2`
     export API_PORT=`echo $TUPLE | cut -d: -f3`
     export MONITORING_PORT=`echo $TUPLE | cut -d: -f4`
     export THREADPOOL_SIZE=`echo $TUPLE | cut -d: -f5`
     export DEBUG_PORT=`echo $TUPLE | cut -d: -f6`
     cat $DEPLOY_ROOT/docker/thirdpartymanager.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-thirdpartymanager.yml
     echo >> $DEPLOY_ROOT/stack/stack-thirdpartymanager.yml
     
     cat $DEPLOY_ROOT/config/logger/log4j-thirdpartyevent.properties | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' > $DEPLOY_ROOT/config/logger/log4j-thirdpartyevent-$KEY.properties
     scp $DEPLOY_ROOT/config/logger/log4j-thirdpartyevent-$KEY.properties $HOST:$NGLM_CONFIG_LOGS/log4j-thirdpartyevent-$KEY.properties
     rm -f $DEPLOY_ROOT/config/logger/log4j-thirdpartyevent-$KEY.properties
     
  done

  #
  #  postamble
  #

  cat $DEPLOY_ROOT/docker/stack-postamble.yml >> $DEPLOY_ROOT/stack/stack-thirdpartymanager.yml
  
fi

#########################################
#
#  construct stack -- dnboproxy(if necessary)
#
#########################################

if [ "$DNBOPROXY_ENABLED" = "true" ]; then

  #
  #  preamble
  #

  mkdir -p $DEPLOY_ROOT/stack
  cat $DEPLOY_ROOT/docker/stack-preamble.yml > $DEPLOY_ROOT/stack/stack-dnboproxy.yml

  #
  #  dnboproxy
  #

  for TUPLE in $DNBOPROXY_CONFIGURATION
  do
     export KEY=`echo $TUPLE | cut -d: -f1`
     export HOST=`echo $TUPLE | cut -d: -f2`
     export API_PORT=`echo $TUPLE | cut -d: -f3`
     export MONITORING_PORT=`echo $TUPLE | cut -d: -f4`
     export DEBUG_PORT=`echo $TUPLE | cut -d: -f5`
     cat $DEPLOY_ROOT/docker/dnboproxy.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-dnboproxy.yml
     echo >> $DEPLOY_ROOT/stack/stack-dnboproxy.yml
  done

  #
  #  postamble
  #

  cat $DEPLOY_ROOT/docker/stack-postamble.yml >> $DEPLOY_ROOT/stack/stack-dnboproxy.yml
  
fi

#########################################
#
#  construct stack -- evolutionengine
#
#########################################

#
#  preamble
#

mkdir -p $DEPLOY_ROOT/stack
cat $DEPLOY_ROOT/docker/stack-preamble.yml > $DEPLOY_ROOT/stack/stack-evolutionengine.yml

#
#  evolutionengine
#

for TUPLE in $EVOLUTIONENGINE_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export SUBSCRIBERPROFILE_PORT=`echo $TUPLE | cut -d: -f3`
   export INTERNAL_PORT=`echo $TUPLE | cut -d: -f4`
   export MONITORING_PORT=`echo $TUPLE | cut -d: -f5`
   export DEBUG_PORT=`echo $TUPLE | cut -d: -f6`
   cat $DEPLOY_ROOT/docker/evolutionengine.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-evolutionengine.yml
   echo >> $DEPLOY_ROOT/stack/stack-evolutionengine.yml
   
   cat $DEPLOY_ROOT/config/logger/log4j-evolutionengine.properties | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' > $DEPLOY_ROOT/config/logger/log4j-evolutionengine-$KEY.properties
   scp $DEPLOY_ROOT/config/logger/log4j-evolutionengine-$KEY.properties $HOST:$NGLM_CONFIG_LOGS/log4j-evolutionengine-$KEY.properties
   rm -f $DEPLOY_ROOT/config/logger/log4j-evolutionengine-$KEY.properties
   
done

#
#  journeytrafficengine
#

for TUPLE in $JOURNEYTRAFFICENGINE_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export MONITORING_PORT=`echo $TUPLE | cut -d: -f3`
   export DEBUG_PORT=`echo $TUPLE | cut -d: -f4`
   cat $DEPLOY_ROOT/docker/journeytrafficengine.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-evolutionengine.yml
   echo >> $DEPLOY_ROOT/stack/stack-evolutionengine.yml
done

#
#  propensityengine
#

for TUPLE in $PROPENSITYENGINE_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export MONITORING_PORT=`echo $TUPLE | cut -d: -f3`
   export DEBUG_PORT=`echo $TUPLE | cut -d: -f4`
   cat $DEPLOY_ROOT/docker/propensityengine.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-evolutionengine.yml
   echo >> $DEPLOY_ROOT/stack/stack-evolutionengine.yml
done

#
#  postamble
#

cat $DEPLOY_ROOT/docker/stack-postamble.yml >> $DEPLOY_ROOT/stack/stack-evolutionengine.yml

#########################################
#
#  construct stack -- ucgengine
#
#########################################

if [ "$UCGENGINE_ENABLED" = "true" ]; then

  #
  #  preamble
  #

  mkdir -p $DEPLOY_ROOT/stack
  cat $DEPLOY_ROOT/docker/stack-preamble.yml > $DEPLOY_ROOT/stack/stack-ucgengine.yml

  #
  #  ucgengine
  #

  for TUPLE in $UCGENGINE_CONFIGURATION
  do
     export KEY=`echo $TUPLE | cut -d: -f1`
     export HOST=`echo $TUPLE | cut -d: -f2`
     export MONITORING_PORT=`echo $TUPLE | cut -d: -f3`
     export DEBUG_PORT=`echo $TUPLE | cut -d: -f4`
     cat $DEPLOY_ROOT/docker/ucgengine.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-ucgengine.yml
     echo >> $DEPLOY_ROOT/stack/stack-ucgengine.yml
  done

  #
  #  postamble
  #

  cat $DEPLOY_ROOT/docker/stack-postamble.yml >> $DEPLOY_ROOT/stack/stack-ucgengine.yml

fi  

#########################################
#
#  construct stack -- infulfillmentmanager
#
#########################################

if [ "$INFULFILLMENTMANAGER_ENABLED" = "true" ]; then

  #
  #  preamble
  #

  mkdir -p $DEPLOY_ROOT/stack
  cat $DEPLOY_ROOT/docker/stack-preamble.yml > $DEPLOY_ROOT/stack/stack-infulfillmentmanager.yml

  #
  #  infulfillmentmanager
  #

  for TUPLE in $INFULFILLMENTMANAGER_CONFIGURATION
  do
     export KEY=`echo $TUPLE | cut -d: -f1`
     export HOST=`echo $TUPLE | cut -d: -f2`
     export MONITORING_PORT=`echo $TUPLE | cut -d: -f3`
     export DEBUG_PORT=`echo $TUPLE | cut -d: -f4`
     export PLUGIN_NAME=`echo $TUPLE | cut -d: -f5`
     export PLUGIN_CONFIGURATION=`echo $TUPLE | cut -d: -f6-`
     cat $DEPLOY_ROOT/docker/infulfillmentmanager.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-infulfillmentmanager.yml
     echo >> $DEPLOY_ROOT/stack/stack-infulfillmentmanager.yml

     cat $DEPLOY_ROOT/config/logger/log4j-infulfillment.properties | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' > $DEPLOY_ROOT/config/logger/log4j-infulfillment-$KEY.properties
     scp $DEPLOY_ROOT/config/logger/log4j-infulfillment-$KEY.properties $HOST:$NGLM_CONFIG_LOGS/log4j-infulfillment-$KEY.properties
     rm -f $DEPLOY_ROOT/config/logger/log4j-infulfillment-$KEY.properties

  done

  #
  #  postamble
  #

  cat $DEPLOY_ROOT/docker/stack-postamble.yml >> $DEPLOY_ROOT/stack/stack-infulfillmentmanager.yml

fi  

#########################################
#
#  construct stack -- emptyfulfillmentmanager
#
#########################################

if [ "$EMPTYFULFILLMENTMANAGER_ENABLED" = "true" ]; then

  #
  #  preamble
  #

  mkdir -p $DEPLOY_ROOT/stack
  cat $DEPLOY_ROOT/docker/stack-preamble.yml > $DEPLOY_ROOT/stack/stack-emptyfulfillmentmanager.yml

  #
  #  emptyfulfillmentmanager
  #

  for TUPLE in $EMPTYFULFILLMENTMANAGER_CONFIGURATION
  do
     export KEY=`echo $TUPLE | cut -d: -f1`
     export HOST=`echo $TUPLE | cut -d: -f2`
     export MONITORING_PORT=`echo $TUPLE | cut -d: -f3`
     export DEBUG_PORT=`echo $TUPLE | cut -d: -f4`
     export PLUGIN_NAME=`echo $TUPLE | cut -d: -f5`
     export PLUGIN_CONFIGURATION=`echo $TUPLE | cut -d: -f6-`
     cat $DEPLOY_ROOT/docker/emptyfulfillmentmanager.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-emptyfulfillmentmanager.yml
     echo >> $DEPLOY_ROOT/stack/stack-emptyfulfillmentmanager.yml
  done

  #
  #  postamble
  #

  cat $DEPLOY_ROOT/docker/stack-postamble.yml >> $DEPLOY_ROOT/stack/stack-emptyfulfillmentmanager.yml

fi  

#########################################
#
#  construct stack -- commoditydeliverymanager
#
#########################################

if [ "$COMMODITYDELIVERYMANAGER_ENABLED" = "true" ]; then

  #
  #  preamble
  #

  mkdir -p $DEPLOY_ROOT/stack
  cat $DEPLOY_ROOT/docker/stack-preamble.yml > $DEPLOY_ROOT/stack/stack-commoditydeliverymanager.yml

  #
  #  commoditydeliverymanager
  #

  for TUPLE in $COMMODITYDELIVERYMANAGER_CONFIGURATION
  do
     export KEY=`echo $TUPLE | cut -d: -f1`
     export HOST=`echo $TUPLE | cut -d: -f2`
     export MONITORING_PORT=`echo $TUPLE | cut -d: -f3`
     export DEBUG_PORT=`echo $TUPLE | cut -d: -f4`
     export PLUGIN_NAME=`echo $TUPLE | cut -d: -f5`
     export PLUGIN_CONFIGURATION=`echo $TUPLE | cut -d: -f6-`
     cat $DEPLOY_ROOT/docker/commoditydeliverymanager.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-commoditydeliverymanager.yml
     echo >> $DEPLOY_ROOT/stack/stack-commoditydeliverymanager.yml

     cat $DEPLOY_ROOT/config/logger/log4j-commoditydelivery.properties | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' > $DEPLOY_ROOT/config/logger/log4j-commoditydelivery-$KEY.properties
     scp $DEPLOY_ROOT/config/logger/log4j-commoditydelivery-$KEY.properties $HOST:$NGLM_CONFIG_LOGS/log4j-commoditydelivery-$KEY.properties
     rm -f $DEPLOY_ROOT/config/logger/log4j-commoditydelivery-$KEY.properties

  done

  #
  #  postamble
  #

  cat $DEPLOY_ROOT/docker/stack-postamble.yml >> $DEPLOY_ROOT/stack/stack-commoditydeliverymanager.yml

fi  

#########################################
#
#  construct stack -- purchasefulfillmentmanager
#
#########################################

if [ "$PURCHASEFULFILLMENTMANAGER_ENABLED" = "true" ]; then

  #
  #  preamble
  #

  mkdir -p $DEPLOY_ROOT/stack
  cat $DEPLOY_ROOT/docker/stack-preamble.yml > $DEPLOY_ROOT/stack/stack-purchasefulfillmentmanager.yml

  #
  #  purchasefulfillmentmanager
  #

  for TUPLE in $PURCHASEFULFILLMENTMANAGER_CONFIGURATION
  do
     export KEY=`echo $TUPLE | cut -d: -f1`
     export HOST=`echo $TUPLE | cut -d: -f2`
     export MONITORING_PORT=`echo $TUPLE | cut -d: -f3`
     export DEBUG_PORT=`echo $TUPLE | cut -d: -f4`
     export PLUGIN_NAME=`echo $TUPLE | cut -d: -f5`
     export PLUGIN_CONFIGURATION=`echo $TUPLE | cut -d: -f6-`
     cat $DEPLOY_ROOT/docker/purchasefulfillmentmanager.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-purchasefulfillmentmanager.yml
     echo >> $DEPLOY_ROOT/stack/stack-purchasefulfillmentmanager.yml

     cat $DEPLOY_ROOT/config/logger/log4j-purchasefulfillment.properties | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' > $DEPLOY_ROOT/config/logger/log4j-purchasefulfillment-$KEY.properties
     scp $DEPLOY_ROOT/config/logger/log4j-purchasefulfillment-$KEY.properties $HOST:$NGLM_CONFIG_LOGS/log4j-purchasefulfillment-$KEY.properties
     rm -f scp $DEPLOY_ROOT/config/logger/log4j-purchasefulfillment-$KEY.properties

  done

  #
  #  postamble
  #

  cat $DEPLOY_ROOT/docker/stack-postamble.yml >> $DEPLOY_ROOT/stack/stack-purchasefulfillmentmanager.yml

fi  

#########################################
#
#  construct stack -- notificationmanager
#
#########################################

if [ "$NOTIFICATIONMANAGER_SMS_ENABLED" = "true" ]; then

  #
  #  preamble
  #

  mkdir -p $DEPLOY_ROOT/stack
  cat $DEPLOY_ROOT/docker/stack-preamble.yml > $DEPLOY_ROOT/stack/stack-notificationmanagersms.yml

  #
  #  notificationmanagersms
  #

  for TUPLE in $NOTIFICATIONMANAGER_SMS_CONFIGURATION
  do
     export KEY=`echo $TUPLE | cut -d: -f1`
     export HOST=`echo $TUPLE | cut -d: -f2`
     export MONITORING_PORT=`echo $TUPLE | cut -d: -f3`
     export DEBUG_PORT=`echo $TUPLE | cut -d: -f4`
     export PLUGIN_NAME=`echo $TUPLE | cut -d: -f5`
     export PLUGIN_CONFIGURATION=`echo $TUPLE | cut -d: -f6-`
     cat $DEPLOY_ROOT/docker/notificationmanagersms.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-notificationmanagersms.yml
     echo >> $DEPLOY_ROOT/stack/stack-notificationmanagersms.yml
     
     cat $DEPLOY_ROOT/config/logger/log4j-sms.properties | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' > $DEPLOY_ROOT/config/logger/log4j-sms-$KEY.properties
     scp $DEPLOY_ROOT/config/logger/log4j-sms-$KEY.properties $HOST:$NGLM_CONFIG_LOGS/log4j-sms-$KEY.properties
     rm -f $DEPLOY_ROOT/config/logger/log4j-sms-$KEY.properties
     
  done

  #
  #  postamble
  #

  cat $DEPLOY_ROOT/docker/stack-postamble.yml >> $DEPLOY_ROOT/stack/stack-notificationmanagersms.yml
  
fi

#
#  notificationmanagermail
#

if [ "$NOTIFICATIONMANAGER_MAIL_ENABLED" = "true" ]; then

  #
  #  preamble
  #

  mkdir -p $DEPLOY_ROOT/stack
  cat $DEPLOY_ROOT/docker/stack-preamble.yml > $DEPLOY_ROOT/stack/stack-notificationmanagermail.yml

  #
  #  notificationmanagermail
  #

  for TUPLE in $NOTIFICATIONMANAGER_MAIL_CONFIGURATION
  do
     export KEY=`echo $TUPLE | cut -d: -f1`
     export HOST=`echo $TUPLE | cut -d: -f2`
     export MONITORING_PORT=`echo $TUPLE | cut -d: -f3`
     export DEBUG_PORT=`echo $TUPLE | cut -d: -f4`
     export PLUGIN_NAME=`echo $TUPLE | cut -d: -f5`
     export PLUGIN_CONFIGURATION=`echo $TUPLE | cut -d: -f6-`
     cat $DEPLOY_ROOT/docker/notificationmanagermail.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-notificationmanagermail.yml
     echo >> $DEPLOY_ROOT/stack/stack-notificationmanagermail.yml

     cat $DEPLOY_ROOT/config/logger/log4j-mail.properties | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' > $DEPLOY_ROOT/config/logger/log4j-mail-$KEY.properties
     scp $DEPLOY_ROOT/config/logger/log4j-mail-$KEY.properties $HOST:$NGLM_CONFIG_LOGS/log4j-mail-$KEY.properties
     rm -f scp $DEPLOY_ROOT/config/logger/log4j-mail-$KEY.properties

  done

  #
  #  postamble
  #

  cat $DEPLOY_ROOT/docker/stack-postamble.yml >> $DEPLOY_ROOT/stack/stack-notificationmanagermail.yml

fi  

#
#  notificationmanagerpush
#

if [ "$NOTIFICATIONMANAGER_PUSH_ENABLED" = "true" ]; then

  #
  #  preamble
  #

  mkdir -p $DEPLOY_ROOT/stack
  cat $DEPLOY_ROOT/docker/stack-preamble.yml > $DEPLOY_ROOT/stack/stack-notificationmanagerpush.yml

  #
  #  notificationmanagerpush
  #

  for TUPLE in $NOTIFICATIONMANAGER_PUSH_CONFIGURATION
  do
     export KEY=`echo $TUPLE | cut -d: -f1`
     export HOST=`echo $TUPLE | cut -d: -f2`
     export MONITORING_PORT=`echo $TUPLE | cut -d: -f3`
     export DEBUG_PORT=`echo $TUPLE | cut -d: -f4`
     export PLUGIN_NAME=`echo $TUPLE | cut -d: -f5`
     export PLUGIN_CONFIGURATION=`echo $TUPLE | cut -d: -f6-`
     cat $DEPLOY_ROOT/docker/notificationmanagerpush.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-notificationmanagerpush.yml
     echo >> $DEPLOY_ROOT/stack/stack-notificationmanagerpush.yml

     cat $DEPLOY_ROOT/config/logger/log4j-push.properties | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' > $DEPLOY_ROOT/config/logger/log4j-push-$KEY.properties
     scp $DEPLOY_ROOT/config/logger/log4j-push-$KEY.properties $HOST:$NGLM_CONFIG_LOGS/log4j-push-$KEY.properties
     rm -f scp $DEPLOY_ROOT/config/logger/log4j-push-$KEY.properties

  done

  #
  #  postamble
  #

  cat $DEPLOY_ROOT/docker/stack-postamble.yml >> $DEPLOY_ROOT/stack/stack-notificationmanagerpush.yml

fi  

#########################################
#
#  construct stack -- reportmanager
#
#########################################

if [ "$REPORTMANAGER_ENABLED" = "true" ]; then

  #
  #  preamble
  #

  mkdir -p $DEPLOY_ROOT/stack
  cat $DEPLOY_ROOT/docker/stack-preamble.yml > $DEPLOY_ROOT/stack/stack-reportmanager.yml

  #
  #  reportmanager
  #

  for TUPLE in $REPORTMANAGER_CONFIGURATION
  do
     export KEY=`echo $TUPLE | cut -d: -f1`
     export HOST=`echo $TUPLE | cut -d: -f2`
     export MONITORING_PORT=`echo $TUPLE | cut -d: -f3`
     export DEBUG_PORT=`echo $TUPLE | cut -d: -f4`
     cat $DEPLOY_ROOT/docker/reportmanager.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-reportmanager.yml
     echo >> $DEPLOY_ROOT/stack/stack-reportmanager.yml
  done

  #
  #  postamble
  #

  cat $DEPLOY_ROOT/docker/stack-postamble.yml >> $DEPLOY_ROOT/stack/stack-reportmanager.yml

fi  

#########################################
#
#  construct stack -- reportscheduler
#
#########################################

if [ "$REPORTSCHEDULER_ENABLED" = "true" ]; then

  #
  #  preamble
  #

  mkdir -p $DEPLOY_ROOT/stack
  cat $DEPLOY_ROOT/docker/stack-preamble.yml > $DEPLOY_ROOT/stack/stack-reportscheduler.yml

  #
  #  reportscheduler
  #

  for TUPLE in $REPORTSCHEDULER_CONFIGURATION
  do
     export KEY=`echo $TUPLE | cut -d: -f1`
     export HOST=`echo $TUPLE | cut -d: -f2`
     export MONITORING_PORT=`echo $TUPLE | cut -d: -f3`
     export DEBUG_PORT=`echo $TUPLE | cut -d: -f4`
     cat $DEPLOY_ROOT/docker/reportscheduler.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-reportscheduler.yml
     echo >> $DEPLOY_ROOT/stack/stack-reportscheduler.yml
  done

  #
  #  postamble
  #

  cat $DEPLOY_ROOT/docker/stack-postamble.yml >> $DEPLOY_ROOT/stack/stack-reportscheduler.yml

fi  

#########################################
#
#  construct stack -- datacubemanager
#
#########################################

if [ "$DATACUBEMANAGER_ENABLED" = "true" ]; then

  #
  #  preamble
  #

  mkdir -p $DEPLOY_ROOT/stack
  cat $DEPLOY_ROOT/docker/stack-preamble.yml > $DEPLOY_ROOT/stack/stack-datacubemanager.yml

  #
  #  datacubemanager
  #

  cat $DEPLOY_ROOT/docker/datacubemanager.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-datacubemanager.yml
  echo >> $DEPLOY_ROOT/stack/stack-datacubemanager.yml

  #
  #  postamble
  #

  cat $DEPLOY_ROOT/docker/stack-postamble.yml >> $DEPLOY_ROOT/stack/stack-datacubemanager.yml

fi  

#########################
#
#  construct stack -- mysql
#
#########################

#
#  preamble
#

mkdir -p $DEPLOY_ROOT/stack
cat $DEPLOY_ROOT/docker/stack-preamble.yml > $DEPLOY_ROOT/stack/stack-mysql.yml

#
#  MySQL GUI
#

for TUPLE in $MYSQL_GUI_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export HOST_IP=`echo $TUPLE | cut -d: -f3`
   export PORT=`echo $TUPLE | cut -d: -f4`
   cat $DEPLOY_ROOT/docker/mysql-gui.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-mysql.yml
   echo >> $DEPLOY_ROOT/stack/stack-mysql.yml
done

#
#  postamble
#

cat $DEPLOY_ROOT/docker/stack-postamble.yml >> $DEPLOY_ROOT/stack/stack-mysql.yml

#########################################
#
#  construct stack -- gui
#
#########################################

#
#  preamble
#

mkdir -p $DEPLOY_ROOT/stack
cat $DEPLOY_ROOT/docker/stack-preamble.yml > $DEPLOY_ROOT/stack/stack-gui.yml

#
#  fwk-web
#

for TUPLE in $GUI_FWK_WEB_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export HOST_IP=`echo $TUPLE | cut -d: -f3`
   export HOST_EXTERNAL_IP=`echo $TUPLE | cut -d: -f4`
   export PORT=`echo $TUPLE | cut -d: -f5`
   cat $DEPLOY_ROOT/docker/fwk-web.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-gui.yml
   echo >> $DEPLOY_ROOT/stack/stack-gui.yml
done

#
#  fwk-api
#

for TUPLE in $GUI_FWK_API_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export HOST_IP=`echo $TUPLE | cut -d: -f3`
   export HOST_EXTERNAL_IP=`echo $TUPLE | cut -d: -f4`
   export PORT=`echo $TUPLE | cut -d: -f5`
   cat $DEPLOY_ROOT/docker/fwk-api.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-gui.yml
   echo >> $DEPLOY_ROOT/stack/stack-gui.yml
done

#
#  fwkauth-api
#

for TUPLE in $GUI_FWK_AUTH_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export HOST_IP=`echo $TUPLE | cut -d: -f3`
   export HOST_EXTERNAL_IP=`echo $TUPLE | cut -d: -f4`
   export PORT=`echo $TUPLE | cut -d: -f5`
   cat $DEPLOY_ROOT/docker/fwkauth-api.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-gui.yml
   echo >> $DEPLOY_ROOT/stack/stack-gui.yml
done

#
#  csr-web
#

for TUPLE in $GUI_CSR_WEB_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export HOST_IP=`echo $TUPLE | cut -d: -f3`
   export HOST_EXTERNAL_IP=`echo $TUPLE | cut -d: -f4`
   export PORT=`echo $TUPLE | cut -d: -f5`
   cat $DEPLOY_ROOT/docker/csr-web.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-gui.yml
   echo >> $DEPLOY_ROOT/stack/stack-gui.yml
done

#
#  csr-api
#

for TUPLE in $GUI_CSR_API_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export HOST_IP=`echo $TUPLE | cut -d: -f3`
   export HOST_EXTERNAL_IP=`echo $TUPLE | cut -d: -f4`
   export PORT=`echo $TUPLE | cut -d: -f5`
   cat $DEPLOY_ROOT/docker/csr-api.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-gui.yml
   echo >> $DEPLOY_ROOT/stack/stack-gui.yml
done

#
#  itm-web
#

for TUPLE in $GUI_ITM_WEB_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export HOST_IP=`echo $TUPLE | cut -d: -f3`
   export HOST_EXTERNAL_IP=`echo $TUPLE | cut -d: -f4`
   export PORT=`echo $TUPLE | cut -d: -f5`
   cat $DEPLOY_ROOT/docker/itm-web.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-gui.yml
   echo >> $DEPLOY_ROOT/stack/stack-gui.yml
done

#
#  itm-api
#

for TUPLE in $GUI_ITM_API_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export HOST_IP=`echo $TUPLE | cut -d: -f3`
   export HOST_EXTERNAL_IP=`echo $TUPLE | cut -d: -f4`
   export PORT=`echo $TUPLE | cut -d: -f5`
   cat $DEPLOY_ROOT/docker/itm-api.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-gui.yml
   echo >> $DEPLOY_ROOT/stack/stack-gui.yml
done

#
#  jmr-web
#

for TUPLE in $GUI_JMR_WEB_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export HOST_IP=`echo $TUPLE | cut -d: -f3`
   export HOST_EXTERNAL_IP=`echo $TUPLE | cut -d: -f4`
   export PORT=`echo $TUPLE | cut -d: -f5`
   cat $DEPLOY_ROOT/docker/jmr-web.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-gui.yml
   echo >> $DEPLOY_ROOT/stack/stack-gui.yml
done

#
#  jmr-api
#

for TUPLE in $GUI_JMR_API_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export HOST_IP=`echo $TUPLE | cut -d: -f3`
   export HOST_EXTERNAL_IP=`echo $TUPLE | cut -d: -f4`
   export PORT=`echo $TUPLE | cut -d: -f5`
   cat $DEPLOY_ROOT/docker/jmr-api.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-gui.yml
   echo >> $DEPLOY_ROOT/stack/stack-gui.yml
done

#
#  opc-web
#

for TUPLE in $GUI_OPC_WEB_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export HOST_IP=`echo $TUPLE | cut -d: -f3`
   export HOST_EXTERNAL_IP=`echo $TUPLE | cut -d: -f4`
   export PORT=`echo $TUPLE | cut -d: -f5`
   cat $DEPLOY_ROOT/docker/opc-web.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-gui.yml
   echo >> $DEPLOY_ROOT/stack/stack-gui.yml
done

#
#  opc-api
#

for TUPLE in $GUI_OPC_API_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export HOST_IP=`echo $TUPLE | cut -d: -f3`
   export HOST_EXTERNAL_IP=`echo $TUPLE | cut -d: -f4`
   export PORT=`echo $TUPLE | cut -d: -f5`
   cat $DEPLOY_ROOT/docker/opc-api.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-gui.yml
   echo >> $DEPLOY_ROOT/stack/stack-gui.yml
done

#
#  iar-web
#

for TUPLE in $GUI_IAR_WEB_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export HOST_IP=`echo $TUPLE | cut -d: -f3`
   export HOST_EXTERNAL_IP=`echo $TUPLE | cut -d: -f4`
   export PORT=`echo $TUPLE | cut -d: -f5`
   cat $DEPLOY_ROOT/docker/iar-web.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-gui.yml
   echo >> $DEPLOY_ROOT/stack/stack-gui.yml
done

#
#  iar-api
#

for TUPLE in $GUI_IAR_API_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export HOST_IP=`echo $TUPLE | cut -d: -f3`
   export HOST_EXTERNAL_IP=`echo $TUPLE | cut -d: -f4`
   export PORT=`echo $TUPLE | cut -d: -f5`
   cat $DEPLOY_ROOT/docker/iar-api.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-gui.yml
   echo >> $DEPLOY_ROOT/stack/stack-gui.yml
done

#
#  opr-web
#

for TUPLE in $GUI_OPR_WEB_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export HOST_IP=`echo $TUPLE | cut -d: -f3`
   export HOST_EXTERNAL_IP=`echo $TUPLE | cut -d: -f4`
   export PORT=`echo $TUPLE | cut -d: -f5`
   cat $DEPLOY_ROOT/docker/opr-web.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-gui.yml
   echo >> $DEPLOY_ROOT/stack/stack-gui.yml
done

#
#  opr-api
#

for TUPLE in $GUI_OPR_API_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export HOST_IP=`echo $TUPLE | cut -d: -f3`
   export HOST_EXTERNAL_IP=`echo $TUPLE | cut -d: -f4`
   export PORT=`echo $TUPLE | cut -d: -f5`
   cat $DEPLOY_ROOT/docker/opr-api.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-gui.yml
   echo >> $DEPLOY_ROOT/stack/stack-gui.yml
done

#
#  stg-web
#

for TUPLE in $GUI_STG_WEB_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export HOST_IP=`echo $TUPLE | cut -d: -f3`
   export HOST_EXTERNAL_IP=`echo $TUPLE | cut -d: -f4`
   export PORT=`echo $TUPLE | cut -d: -f5`
   cat $DEPLOY_ROOT/docker/stg-web.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-gui.yml
   echo >> $DEPLOY_ROOT/stack/stack-gui.yml
done

#
#  stg-api
#

for TUPLE in $GUI_STG_API_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export HOST_IP=`echo $TUPLE | cut -d: -f3`
   export HOST_EXTERNAL_IP=`echo $TUPLE | cut -d: -f4`
   export PORT=`echo $TUPLE | cut -d: -f5`
   cat $DEPLOY_ROOT/docker/stg-api.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-gui.yml
   echo >> $DEPLOY_ROOT/stack/stack-gui.yml
done

#
#  sbm-web
#

for TUPLE in $GUI_SBM_WEB_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export HOST_IP=`echo $TUPLE | cut -d: -f3`
   export HOST_EXTERNAL_IP=`echo $TUPLE | cut -d: -f4`
   export PORT=`echo $TUPLE | cut -d: -f5`
   cat $DEPLOY_ROOT/docker/sbm-web.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-gui.yml
   echo >> $DEPLOY_ROOT/stack/stack-gui.yml
done

#
#  sbm-api
#

for TUPLE in $GUI_SBM_API_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export HOST_IP=`echo $TUPLE | cut -d: -f3`
   export HOST_EXTERNAL_IP=`echo $TUPLE | cut -d: -f4`
   export PORT=`echo $TUPLE | cut -d: -f5`
   cat $DEPLOY_ROOT/docker/sbm-api.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-gui.yml
   echo >> $DEPLOY_ROOT/stack/stack-gui.yml
done

#
#  lpm-web
#

for TUPLE in $GUI_LPM_WEB_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export HOST_IP=`echo $TUPLE | cut -d: -f3`
   export HOST_EXTERNAL_IP=`echo $TUPLE | cut -d: -f4`
   export PORT=`echo $TUPLE | cut -d: -f5`
   cat $DEPLOY_ROOT/docker/lpm-web.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-gui.yml
   echo >> $DEPLOY_ROOT/stack/stack-gui.yml
done

#
#  lpm-api
#

for TUPLE in $GUI_LPM_API_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export HOST_IP=`echo $TUPLE | cut -d: -f3`
   export HOST_EXTERNAL_IP=`echo $TUPLE | cut -d: -f4`
   export PORT=`echo $TUPLE | cut -d: -f5`
   cat $DEPLOY_ROOT/docker/lpm-api.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-gui.yml
   echo >> $DEPLOY_ROOT/stack/stack-gui.yml
done

#
#  gui-audit
#

for TUPLE in $GUI_AUDIT_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export HOST_IP=`echo $TUPLE | cut -d: -f3`
   export PORT=`echo $TUPLE | cut -d: -f5`
   cat $DEPLOY_ROOT/docker/gui-audit.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-gui.yml
   echo >> $DEPLOY_ROOT/stack/stack-gui.yml
done

#
#  csrmockup
#

if [ "$GUI_USE_CSR_MOCKUP" = "true" ]; then

   cat $DEPLOY_ROOT/docker/csrmockup.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-gui.yml
   echo >> $DEPLOY_ROOT/stack/stack-gui.yml

fi   

#
#  postamble
#

cat $DEPLOY_ROOT/docker/stack-postamble.yml >> $DEPLOY_ROOT/stack/stack-gui.yml

###########################################################################
#
#  construct stack -- Upgrade - preamble to a generic upgrade stack for NGLM
#
###########################################################################

#
#  preamble
#

mkdir -p $DEPLOY_ROOT/stack
cat $DEPLOY_ROOT/docker/stack-preamble.yml > $DEPLOY_ROOT/stack/stack-upgrade.yml

for TUPLE in $NGLM_UPGRADE_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   cat $DEPLOY_ROOT/docker/upgrade.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-upgrade.yml
   echo >> $DEPLOY_ROOT/stack/stack-upgrade.yml
done

#
#  postamble
#

cat $DEPLOY_ROOT/docker/stack-postamble.yml >> $DEPLOY_ROOT/stack/stack-upgrade.yml

#########################################
#
#  construct stack -- emulators(if necessary)
#
#########################################

if [ "$FAKEEMULATORS_ENABLED" = "true" ]; then

  #
  #  preamble
  #

  mkdir -p $DEPLOY_ROOT/stack
  cat $DEPLOY_ROOT/docker/stack-preamble.yml > $DEPLOY_ROOT/stack/stack-fake.yml

  #
  #  fake smsc
  #

  for TUPLE in $FAKE_SMSC_CONFIGURATION
  do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export HOST_IP=`echo $TUPLE | cut -d: -f3`
   export SMPP_PORT=`echo $TUPLE | cut -d: -f4`
   export HTTP_PORT=`echo $TUPLE | cut -d: -f5`
   cat $DEPLOY_ROOT/docker/fakesmsc.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-fake.yml
  done

  #
  #  fake smtp
  #

  for TUPLE in $FAKE_SMTP_CONFIGURATION
  do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export SMTP_PORT=`echo $TUPLE | cut -d: -f3`
   cat $DEPLOY_ROOT/docker/fakesmtp.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-fake.yml
  done
  
  #
  #  fake in
  #

  for TUPLE in $FAKE_IN_IN_SERVERS
  do
    export KEY=`echo $TUPLE | cut -d: -f1`
    export HOST=`echo $TUPLE | cut -d: -f2`
    export PORT=`echo $TUPLE | cut -d: -f3`
    export DETERMINISTIC=`echo $TUPLE | cut -d: -f4`
    declare FAKE_IN_IN_SERVERS_$KEY="$HOST:$PORT"
    varname=FAKE_IN_IN_SERVERS_$KEY
    cat $DEPLOY_ROOT/docker/fakein.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-fake.yml
  done
  
  #
  #  postamble
  #

  cat $DEPLOY_ROOT/docker/stack-postamble.yml >> $DEPLOY_ROOT/stack/stack-fake.yml
  
fi

