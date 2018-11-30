#################################################################################
#
#  evolution-prepare-stack-environment.sh
#
#################################################################################

#########################################
#
#  service configuration
#
#########################################

#
#  evolutionengine -- configuration
#

EVOLUTIONENGINE_CONFIGURATION=`echo $EVOLUTIONENGINE_CONFIGURATION | sed 's/ /\n/g' | uniq`
EVOLUTIONENGINE_PROMETHEUS=
for TUPLE in $EVOLUTIONENGINE_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export MONITORING_PORT=`echo $TUPLE | cut -d: -f3`
   export DEBUG_PORT=`echo $TUPLE | cut -d: -f4`
   if [ -n "$EVOLUTIONENGINE_PROMETHEUS" ]; then
     EVOLUTIONENGINE_PROMETHEUS="$EVOLUTIONENGINE_PROMETHEUS,'$HOST:$MONITORING_PORT'"
   else
     EVOLUTIONENGINE_PROMETHEUS="'$HOST:$MONITORING_PORT'"
   fi
done
export EVOLUTIONENGINE_PROMETHEUS

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

#
#  GUI FWK WEB -- configuration
#

GUI_FWK_WEB_SERVER=
GUI_FWK_WEB_SERVER_HOST=
GUI_FWK_WEB_SERVER_HOST_IP=
GUI_FWK_WEB_SERVER_PORT=
for TUPLE in $GUI_FWK_WEB_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export HOST_IP=`echo $TUPLE | cut -d: -f3`
   export PORT=`echo $TUPLE | cut -d: -f4`
   if [ -z "$GUI_FWK_WEB_SERVER" ]; then
     GUI_FWK_WEB_SERVER="$HOST:$PORT"
     GUI_FWK_WEB_SERVER_HOST="$HOST"
     GUI_FWK_WEB_SERVER_HOST_IP="$HOST_IP"
     GUI_FWK_WEB_SERVER_PORT="$PORT"
   fi
done
export GUI_FWK_WEB_SERVER
export GUI_FWK_WEB_SERVER_HOST
export GUI_FWK_WEB_SERVER_HOST_IP
export GUI_FWK_WEB_SERVER_PORT

#
#  GUI FWK API -- configuration
#

GUI_FWK_API_SERVER=
GUI_FWK_API_SERVER_HOST=
GUI_FWK_API_SERVER_HOST_IP=
GUI_FWK_API_SERVER_PORT=
for TUPLE in $GUI_FWK_API_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export HOST_IP=`echo $TUPLE | cut -d: -f3`
   export PORT=`echo $TUPLE | cut -d: -f4`
   if [ -z "$GUI_FWK_API_SERVER" ]; then
     GUI_FWK_API_SERVER="$HOST:$PORT"
     GUI_FWK_API_SERVER_HOST="$HOST"
     GUI_FWK_API_SERVER_HOST_IP="$HOST_IP"
     GUI_FWK_API_SERVER_PORT="$PORT"
   fi
done
export GUI_FWK_API_SERVER
export GUI_FWK_API_SERVER_HOST
export GUI_FWK_API_SERVER_HOST_IP
export GUI_FWK_API_SERVER_PORT

#
#  GUI FWK AUTH -- configuration
#

GUI_FWK_AUTH_SERVER=
GUI_FWK_AUTH_SERVER_HOST=
GUI_FWK_AUTH_SERVER_HOST_IP=
GUI_FWK_AUTH_SERVER_PORT=
for TUPLE in $GUI_FWK_AUTH_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export HOST_IP=`echo $TUPLE | cut -d: -f3`
   export PORT=`echo $TUPLE | cut -d: -f4`
   if [ -z "$GUI_FWK_AUTH_SERVER" ]; then
     GUI_FWK_AUTH_SERVER="$HOST:$PORT"
     GUI_FWK_AUTH_SERVER_HOST="$HOST"
     GUI_FWK_AUTH_SERVER_HOST_IP="$HOST_IP"
     GUI_FWK_AUTH_SERVER_PORT="$PORT"
   fi
done
export GUI_FWK_AUTH_SERVER
export GUI_FWK_AUTH_SERVER_HOST
export GUI_FWK_AUTH_SERVER_HOST_IP
export GUI_FWK_AUTH_SERVER_PORT

#
#  GUI CSR WEB -- configuration
#

GUI_CSR_WEB_SERVER=
GUI_CSR_WEB_SERVER_HOST=
GUI_CSR_WEB_SERVER_HOST_IP=
GUI_CSR_WEB_SERVER_PORT=
for TUPLE in $GUI_CSR_WEB_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export HOST_IP=`echo $TUPLE | cut -d: -f3`
   export PORT=`echo $TUPLE | cut -d: -f4`
   if [ -z "$GUI_CSR_WEB_SERVER" ]; then
     GUI_CSR_WEB_SERVER="$HOST:$PORT"
     GUI_CSR_WEB_SERVER_HOST="$HOST"
     GUI_CSR_WEB_SERVER_HOST_IP="$HOST_IP"
     GUI_CSR_WEB_SERVER_PORT="$PORT"
   fi
done
export GUI_CSR_WEB_SERVER
export GUI_CSR_WEB_SERVER_HOST
export GUI_CSR_WEB_SERVER_HOST_IP
export GUI_CSR_WEB_SERVER_PORT

#
#  GUI CSR API -- configuration
#

GUI_CSR_API_SERVER=
GUI_CSR_API_SERVER_HOST=
GUI_CSR_API_SERVER_HOST_IP=
GUI_CSR_API_SERVER_PORT=
for TUPLE in $GUI_CSR_API_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export HOST_IP=`echo $TUPLE | cut -d: -f3`
   export PORT=`echo $TUPLE | cut -d: -f4`
   if [ -z "$GUI_CSR_API_SERVER" ]; then
     GUI_CSR_API_SERVER="$HOST:$PORT"
     GUI_CSR_API_SERVER_HOST="$HOST"
     GUI_CSR_API_SERVER_HOST_IP="$HOST_IP"
     GUI_CSR_API_SERVER_PORT="$PORT"
   fi
done
export GUI_CSR_API_SERVER
export GUI_CSR_API_SERVER_HOST
export GUI_CSR_API_SERVER_HOST_IP
export GUI_CSR_API_SERVER_PORT

#
#  GUI ITM WEB -- configuration
#

GUI_ITM_WEB_SERVER=
GUI_ITM_WEB_SERVER_HOST=
GUI_ITM_WEB_SERVER_HOST_IP=
GUI_ITM_WEB_SERVER_PORT=
for TUPLE in $GUI_ITM_WEB_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export HOST_IP=`echo $TUPLE | cut -d: -f3`
   export PORT=`echo $TUPLE | cut -d: -f4`
   if [ -z "$GUI_ITM_WEB_SERVER" ]; then
     GUI_ITM_WEB_SERVER="$HOST:$PORT"
     GUI_ITM_WEB_SERVER_HOST="$HOST"
     GUI_ITM_WEB_SERVER_HOST_IP="$HOST_IP"
     GUI_ITM_WEB_SERVER_PORT="$PORT"
   fi
done
export GUI_ITM_WEB_SERVER
export GUI_ITM_WEB_SERVER_HOST
export GUI_ITM_WEB_SERVER_HOST_IP
export GUI_ITM_WEB_SERVER_PORT

#
#  GUI ITM API -- configuration
#

GUI_ITM_API_SERVER=
GUI_ITM_API_SERVER_HOST=
GUI_ITM_API_SERVER_HOST_IP=
GUI_ITM_API_SERVER_PORT=
for TUPLE in $GUI_ITM_API_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export HOST_IP=`echo $TUPLE | cut -d: -f3`
   export PORT=`echo $TUPLE | cut -d: -f4`
   if [ -z "$GUI_ITM_API_SERVER" ]; then
     GUI_ITM_API_SERVER="$HOST:$PORT"
     GUI_ITM_API_SERVER_HOST="$HOST"
     GUI_ITM_API_SERVER_HOST_IP="$HOST_IP"
     GUI_ITM_API_SERVER_PORT="$PORT"
   fi
done
export GUI_ITM_API_SERVER
export GUI_ITM_API_SERVER_HOST
export GUI_ITM_API_SERVER_HOST_IP
export GUI_ITM_API_SERVER_PORT

#
#  GUI JMR WEB -- configuration
#

GUI_JMR_WEB_SERVER=
GUI_JMR_WEB_SERVER_HOST=
GUI_JMR_WEB_SERVER_HOST_IP=
GUI_JMR_WEB_SERVER_PORT=
for TUPLE in $GUI_JMR_WEB_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export HOST_IP=`echo $TUPLE | cut -d: -f3`
   export PORT=`echo $TUPLE | cut -d: -f4`
   if [ -z "$GUI_JMR_WEB_SERVER" ]; then
     GUI_JMR_WEB_SERVER="$HOST:$PORT"
     GUI_JMR_WEB_SERVER_HOST="$HOST"
     GUI_JMR_WEB_SERVER_HOST_IP="$HOST_IP"
     GUI_JMR_WEB_SERVER_PORT="$PORT"
   fi
done
export GUI_JMR_WEB_SERVER
export GUI_JMR_WEB_SERVER_HOST
export GUI_JMR_WEB_SERVER_HOST_IP
export GUI_JMR_WEB_SERVER_PORT

#
#  GUI JMR API -- configuration
#

GUI_JMR_API_SERVER=
GUI_JMR_API_SERVER_HOST=
GUI_JMR_API_SERVER_HOST_IP=
GUI_JMR_API_SERVER_PORT=
for TUPLE in $GUI_JMR_API_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export HOST_IP=`echo $TUPLE | cut -d: -f3`
   export PORT=`echo $TUPLE | cut -d: -f4`
   if [ -z "$GUI_JMR_API_SERVER" ]; then
     GUI_JMR_API_SERVER="$HOST:$PORT"
     GUI_JMR_API_SERVER_HOST="$HOST"
     GUI_JMR_API_SERVER_HOST_IP="$HOST_IP"
     GUI_JMR_API_SERVER_PORT="$PORT"
   fi
done
export GUI_JMR_API_SERVER
export GUI_JMR_API_SERVER_HOST
export GUI_JMR_API_SERVER_HOST_IP
export GUI_JMR_API_SERVER_PORT

#
#  GUI OPC WEB -- configuration
#

GUI_OPC_WEB_SERVER=
GUI_OPC_WEB_SERVER_HOST=
GUI_OPC_WEB_SERVER_HOST_IP=
GUI_OPC_WEB_SERVER_PORT=
for TUPLE in $GUI_OPC_WEB_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export HOST_IP=`echo $TUPLE | cut -d: -f3`
   export PORT=`echo $TUPLE | cut -d: -f4`
   if [ -z "$GUI_OPC_WEB_SERVER" ]; then
     GUI_OPC_WEB_SERVER="$HOST:$PORT"
     GUI_OPC_WEB_SERVER_HOST="$HOST"
     GUI_OPC_WEB_SERVER_HOST_IP="$HOST_IP"
     GUI_OPC_WEB_SERVER_PORT="$PORT"
   fi
done
export GUI_OPC_WEB_SERVER
export GUI_OPC_WEB_SERVER_HOST
export GUI_OPC_WEB_SERVER_HOST_IP
export GUI_OPC_WEB_SERVER_PORT

#
#  GUI OPC API -- configuration
#

GUI_OPC_API_SERVER=
GUI_OPC_API_SERVER_HOST=
GUI_OPC_API_SERVER_HOST_IP=
GUI_OPC_API_SERVER_PORT=
for TUPLE in $GUI_OPC_API_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export HOST_IP=`echo $TUPLE | cut -d: -f3`
   export PORT=`echo $TUPLE | cut -d: -f4`
   if [ -z "$GUI_OPC_API_SERVER" ]; then
     GUI_OPC_API_SERVER="$HOST:$PORT"
     GUI_OPC_API_SERVER_HOST="$HOST"
     GUI_OPC_API_SERVER_HOST_IP="$HOST_IP"
     GUI_OPC_API_SERVER_PORT="$PORT"
   fi
done
export GUI_OPC_API_SERVER
export GUI_OPC_API_SERVER_HOST
export GUI_OPC_API_SERVER_HOST_IP
export GUI_OPC_API_SERVER_PORT

#
#  thirdpartymanager -- configuration
#

THIRDPARTYMANAGER_CONFIGURATION=`echo $THIRDPARTYMANAGER_CONFIGURATION | sed 's/ /\n/g' | uniq`
THIRDPARTYMANAGER_PROMETHEUS=
for TUPLE in $THIRDPARTYMANAGER_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export API_PORT=`echo $TUPLE | cut -d: -f3`
   export MONITORING_PORT=`echo $TUPLE | cut -d: -f4`
   export THREADPOOL_SIZE=`echo $TUPLE | cut -d: -f5`
   if [ -n "$THIRDPARTYMANAGER_PROMETHEUS" ]; then
     THIRDPARTYMANAGER_PROMETHEUS="$THIRDPARTYMANAGER_PROMETHEUS,'$HOST:$MONITORING_PORT'"
   else
     THIRDPARTYMANAGER_PROMETHEUS="'$HOST:$MONITORING_PORT'"
   fi
done
export THIRDPARTYMANAGER_PROMETHEUS

#########################################
#
#  heap opts
#
#########################################

export GUIMANAGER_HEAP_OPTS="-Xms$GUIMANAGER_MEMORY -Xmx$GUIMANAGER_MEMORY"
export THIRDPARTYMANAGER_HEAP_OPTS="-Xms$THIRDPARTYMANAGER_MEMORY -Xmx$THIRDPARTYMANAGER_MEMORY"
export CRITERIAAPI_HEAP_OPTS="-Xms$CRITERIAAPI_MEMORY -Xmx$CRITERIAAPI_MEMORY"
export EVOLUTIONENGINE_HEAP_OPTS="-Xms$EVOLUTIONENGINE_MEMORY -Xmx$EVOLUTIONENGINE_MEMORY"
export SUBSCRIBERGROUP_HEAP_OPTS="-Xms$SUBSCRIBERGROUP_MEMORY -Xmx$SUBSCRIBERGROUP_MEMORY"

#########################################
#
#  heap opts
#
#########################################

export GUIMANAGER_CONTAINER_MEMORY_LIMIT=$(memory_limit $GUIMANAGER_MEMORY)
export THIRDPARTYMANAGER_CONTAINER_MEMORY_LIMIT=$(memory_limit $THIRDPARTYMANAGER_MEMORY)
export CRITERIAAPI_CONTAINER_MEMORY_LIMIT=$(memory_limit $CRITERIAAPI_MEMORY)
export EVOLUTIONENGINE_CONTAINER_MEMORY_LIMIT=$(memory_limit $EVOLUTIONENGINE_MEMORY)
export SUBSCRIBERGROUP_CONTAINER_MEMORY_LIMIT=$(memory_limit $SUBSCRIBERGROUP_MEMORY)
