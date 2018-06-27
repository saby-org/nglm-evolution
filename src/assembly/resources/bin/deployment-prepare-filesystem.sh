#################################################################################
#
#  deployment-prepare-filesystem.sh
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
#  profileengine
#

PROFILEENGINE_CONFIGURATION=`echo $PROFILEENGINE_CONFIGURATION | sed 's/ /\n/g' | uniq`
for TUPLE in $PROFILEENGINE_CONFIGURATION
do
   export KEY=`echo $TUPLE | cut -d: -f1`
   export HOST=`echo $TUPLE | cut -d: -f2`
   export MONITORING_PORT=`echo $TUPLE | cut -d: -f3`
   ssh $HOST "
      mkdir -p $NGLM_STREAMS_RUNTIME/streams-profileengine-$KEY
   "
done

#
#  subscriber group
#

ssh $MASTER_SWARM_HOST "
   mkdir -p $NGLM_SUBSCRIBERGROUP_DATA
"

