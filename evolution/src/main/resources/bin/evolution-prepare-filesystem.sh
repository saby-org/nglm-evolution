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
   export SUBSCRIBERPROFILE_PORT=`echo $TUPLE | cut -d: -f3`
   export INTERNAL_PORT=`echo $TUPLE | cut -d: -f4`
   export MONITORING_PORT=`echo $TUPLE | cut -d: -f5`
   export DEBUG_PORT=`echo $TUPLE | cut -d: -f6`
   ssh $HOST "
      mkdir -p $NGLM_STREAMS_RUNTIME/streams-evolutionengine-$KEY
   "
done

#
#  propensityengine
#

PROPENSITYENGINE_CONFIGURATION=`echo $PROPENSITYENGINE_CONFIGURATION | sed 's/ /\n/g' | uniq`
for TUPLE in $PROPENSITYENGINE_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export MONITORING_PORT=`echo $TUPLE | cut -d: -f3`
   ssh $HOST "
      mkdir -p $NGLM_STREAMS_RUNTIME/streams-propensityengine-$KEY
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
   #
   # nothing to prepare
   #
done

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
#  Report Manager
#

REPORTMANAGER_CONFIGURATION=`echo $REPORTMANAGER_CONFIGURATION | sed 's/ /\n/g' | uniq`
for TUPLE in $REPORTMANAGER_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export MONITORING_PORT=`echo $TUPLE | cut -d: -f3`
   export DEBUG_PORT=`echo $TUPLE | cut -d: -f4`
      ssh $HOST "
      mkdir -p $NGLM_REPORTS
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

