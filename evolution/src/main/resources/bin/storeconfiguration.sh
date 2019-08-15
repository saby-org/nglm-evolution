#################################################################################
#
#  storeconfiguration.sh
#
#################################################################################

#
#  sets
#

set -o errexit 

#
#  arguments
#

export OPERATION=$1
export GUIMANAGER_HOST=$2
export GUIMANAGER_PORT=$3
export CONFIGURATION_STORE_FILE=$4
export INTERNAL_CONFIGURATION_STORE_FILE=nglm-configuration.json

#
#  remove existing file (if store)
#

if [ "$OPERATION" = "store" ]; then
  rm -f $CONFIGURATION_STORE_FILE
  rm -f <_NGLM_STORECONFIGURATION_DATA_>/$INTERNAL_CONFIGURATION_STORE_FILE
fi  

#
#  copy file into /app/data (if load)
#

if [ "$OPERATION" = "load" ]; then
  cp -f $CONFIGURATION_STORE_FILE <_NGLM_STORECONFIGURATION_DATA_>/$INTERNAL_CONFIGURATION_STORE_FILE
fi  

#
#  run
#

docker run --net=host --mount type=bind,source=<_NGLM_STORECONFIGURATION_DATA_>,target=/app/data --rm ${env.DOCKER_REGISTRY}ev-storeconfiguration:${project.name}-${project.version} /app/bin/storeconfiguration-run.sh $OPERATION $GUIMANAGER_HOST $GUIMANAGER_PORT /app/data/$INTERNAL_CONFIGURATION_STORE_FILE <_TRACE_LEVEL_>

#
#  copy file from /app/data (if store)
#

if [ "$OPERATION" = "store" ]; then
  cp <_NGLM_STORECONFIGURATION_DATA_>/nglm-configuration.json $CONFIGURATION_STORE_FILE
fi  
