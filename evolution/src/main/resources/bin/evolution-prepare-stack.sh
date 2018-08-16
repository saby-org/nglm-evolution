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
   export MONITORING_PORT=`echo $TUPLE | cut -d: -f3`
   export DEBUG_PORT=`echo $TUPLE | cut -d: -f4`
   cat $DEPLOY_ROOT/docker/evolutionengine.yml | perl -e 'while ( $line=<STDIN> ) { $line=~s/<_([A-Z_0-9]+)_>/$ENV{$1}/g; print $line; }' | sed 's/\\n/\n/g' | sed 's/^/  /g' >> $DEPLOY_ROOT/stack/stack-evolutionengine.yml
   echo >> $DEPLOY_ROOT/stack/stack-evolutionengine.yml
done

#
#  postamble
#

cat $DEPLOY_ROOT/docker/stack-postamble.yml >> $DEPLOY_ROOT/stack/stack-evolutionengine.yml

