#################################################################################
#
#  deployment-prepare-stack-environment.sh
#
#################################################################################

#########################################
#
#  service configuration
#
#########################################

#
#  profileengine -- configuration
#

PROFILEENGINE_CONFIGURATION=`echo $PROFILEENGINE_CONFIGURATION | sed 's/ /\n/g' | uniq`
PROFILEENGINE_PROMETHEUS=
for TUPLE in $PROFILEENGINE_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export MONITORING_PORT=`echo $TUPLE | cut -d: -f3`
   if [ -n "$PROFILEENGINE_PROMETHEUS" ]; then
     PROFILEENGINE_PROMETHEUS="$PROFILEENGINE_PROMETHEUS,'$HOST:$MONITORING_PORT'"
   else
     PROFILEENGINE_PROMETHEUS="'$HOST:$MONITORING_PORT'"
   fi
done
export PROFILEENGINE_PROMETHEUS

#
#  subscriberprofile redis -- configuration
#

SUBSCRIBERPROFILE_REDIS_SERVER=
SUBSCRIBERPROFILE_REDIS_SERVER_HOST=
SUBSCRIBERPROFILE_REDIS_SERVER_PORT=
for TUPLE in $REDIS_CONFIGURATION_SUBSCRIBERPROFILE
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export HOST_IP=`echo $TUPLE | cut -d: -f3`
   export PORT=`echo $TUPLE | cut -d: -f4`
   if [ -z "$SUBSCRIBERPROFILE_REDIS_SERVER" ]; then
     SUBSCRIBERPROFILE_REDIS_SERVER="$HOST:$PORT"
     SUBSCRIBERPROFILE_REDIS_SERVER_HOST="$HOST"
     SUBSCRIBERPROFILE_REDIS_SERVER_PORT="$PORT"
   fi
done
export SUBSCRIBERPROFILE_REDIS_SERVER
export SUBSCRIBERPROFILE_REDIS_SERVER_HOST
export SUBSCRIBERPROFILE_REDIS_SERVER_PORT

#
#  subscriberids redis -- configuration
#

SUBSCRIBERIDS_REDIS_SERVER=
SUBSCRIBERIDS_REDIS_SERVER_HOST=
SUBSCRIBERIDS_REDIS_SERVER_PORT=
for TUPLE in $REDIS_CONFIGURATION_SUBSCRIBERIDS
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export HOST_IP=`echo $TUPLE | cut -d: -f3`
   export PORT=`echo $TUPLE | cut -d: -f4`
   if [ -z "$SUBSCRIBERIDS_REDIS_SERVER" ]; then
     SUBSCRIBERIDS_REDIS_SERVER="$HOST:$PORT"
     SUBSCRIBERIDS_REDIS_SERVER_HOST="$HOST"
     SUBSCRIBERIDS_REDIS_SERVER_PORT="$PORT"
   fi
done
export SUBSCRIBERIDS_REDIS_SERVER
export SUBSCRIBERIDS_REDIS_SERVER_HOST
export SUBSCRIBERIDS_REDIS_SERVER_PORT

#########################################
#
#  heap opts
#
#########################################

export GUIMANAGER_HEAP_OPTS="-Xms$GUIMANAGER_MEMORY -Xmx$GUIMANAGER_MEMORY" 
export CRITERIAAPI_HEAP_OPTS="-Xms$CRITERIAAPI_MEMORY -Xmx$CRITERIAAPI_MEMORY" 
export PROFILEENGINE_HEAP_OPTS="-Xms$PROFILEENGINE_MEMORY -Xmx$PROFILEENGINE_MEMORY" 
export SUBSCRIBERGROUP_HEAP_OPTS="-Xms$SUBSCRIBERGROUP_MEMORY -Xmx$SUBSCRIBERGROUP_MEMORY" 

#########################################
#
#  heap opts
#
#########################################

export GUIMANAGER_CONTAINER_MEMORY_LIMIT=$(memory_limit $GUIMANAGER_MEMORY)
export CRITERIAAPI_CONTAINER_MEMORY_LIMIT=$(memory_limit $CRITERIAAPI_MEMORY)
export PROFILEENGINE_CONTAINER_MEMORY_LIMIT=$(memory_limit $PROFILEENGINE_MEMORY)
export SUBSCRIBERGROUP_CONTAINER_MEMORY_LIMIT=$(memory_limit $SUBSCRIBERGROUP_MEMORY)

