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
echo >> $DEPLOY_ROOT/stack/stack-application-monitoring.yml

#
#  postamble
#

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
#  criteriaapi
#

cat $DEPLOY_ROOT/docker/criteriaapi.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-guimanager.yml
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

if [ "${thirdpartymanager.enabled}" = "true" ]; then

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
     cat $DEPLOY_ROOT/docker/thirdpartymanager.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-thirdpartymanager.yml
     echo >> $DEPLOY_ROOT/stack/stack-thirdpartymanager.yml
  done

  #
  #  postamble
  #

  cat $DEPLOY_ROOT/docker/stack-postamble.yml >> $DEPLOY_ROOT/stack/stack-thirdpartymanager.yml
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
done

#
#  postamble
#

cat $DEPLOY_ROOT/docker/stack-postamble.yml >> $DEPLOY_ROOT/stack/stack-evolutionengine.yml

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
   export PORT=`echo $TUPLE | cut -d: -f4`
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
   export PORT=`echo $TUPLE | cut -d: -f4`
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
   export PORT=`echo $TUPLE | cut -d: -f4`
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
   export PORT=`echo $TUPLE | cut -d: -f4`
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
   export PORT=`echo $TUPLE | cut -d: -f4`
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
   export PORT=`echo $TUPLE | cut -d: -f4`
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
   export PORT=`echo $TUPLE | cut -d: -f4`
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
   export PORT=`echo $TUPLE | cut -d: -f4`
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
   export PORT=`echo $TUPLE | cut -d: -f4`
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
   export PORT=`echo $TUPLE | cut -d: -f4`
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
   export PORT=`echo $TUPLE | cut -d: -f4`
   cat $DEPLOY_ROOT/docker/opc-api.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-gui.yml
   echo >> $DEPLOY_ROOT/stack/stack-gui.yml
done


#
#  postamble
#

cat $DEPLOY_ROOT/docker/stack-postamble.yml >> $DEPLOY_ROOT/stack/stack-gui.yml
