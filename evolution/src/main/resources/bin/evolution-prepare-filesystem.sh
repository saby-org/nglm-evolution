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
   export MONITORING_PORT=`echo $TUPLE | cut -d: -f3`
   export DEBUG_PORT=`echo $TUPLE | cut -d: -f4`
   ssh $HOST "
      mkdir -p $NGLM_STREAMS_RUNTIME/streams-evolutionengine-$KEY
   "
done

#
#  subscriber group
#

ssh $MASTER_SWARM_HOST "
   mkdir -p $NGLM_SUBSCRIBERGROUP_DATA
"

#
# FWK components
#

GUI_FWK_API_CONFIGURATION=`echo $GUI_FWK_API_CONFIGURATION | sed 's/ /\n/g' | uniq`
for TUPLE in $GUI_FWK_API_CONFIGURATION
do
  export KEY=`echo $TUPLE | cut -d: -f1`
  export HOST=`echo $TUPLE | cut -d: -f2`
  export HOST_IP=`echo $TUPLE | cut -d: -f3`
  export PORT=`echo $TUPLE | cut -d: -f4`
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
  export PORT=`echo $TUPLE | cut -d: -f4`
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
  export PORT=`echo $TUPLE | cut -d: -f4`
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
  export PORT=`echo $TUPLE | cut -d: -f4`
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
  export PORT=`echo $TUPLE | cut -d: -f4`
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
  export PORT=`echo $TUPLE | cut -d: -f4`
  ssh $HOST "
      mkdir -p $NGLM_GUI_RUNTIME/csr-api-$KEY/mnt
   "
done
