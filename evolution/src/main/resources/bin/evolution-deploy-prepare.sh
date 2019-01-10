#################################################################################
#
#  evolution-deploy-prepare.sh
#
#################################################################################

#
#  remove and recreate nglm runtime 
#

for SWARM_HOST in $SWARM_HOSTS
do
   ssh $SWARM_HOST "
      rm -rf ${NGLM_STREAMS_RUNTIME}
      rm -rf ${NGLM_GUI_RUNTIME}
      rm -rf ${NGLM_REPORTS}
   "
done

#
#  remove and recreate nglm data
#

ssh $MASTER_SWARM_HOST "
   rm -rf ${NGLM_DATA}
   rm -rf ${NGLM_SUBSCRIBERGROUP_DATA}
"

#########################################
#
#  additional one-time work
#
#########################################

