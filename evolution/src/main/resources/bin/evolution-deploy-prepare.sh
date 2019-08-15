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
      rm -rf ${NGLM_UPLOADED}
      rm -rf ${NGLM_MYSQL_RUNTIME}
   "
done

#
#  remove and recreate nglm data
#

ssh $MASTER_SWARM_HOST "
   rm -rf ${NGLM_DATA}
   rm -rf ${NGLM_SUBSCRIBERGROUP_DATA}
   rm -rf ${NGLM_STORECONFIGURATION_DATA}
"

