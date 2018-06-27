#################################################################################
#
#  deployment-deploy-prepare.sh
#
#################################################################################

#
#  remove and recreate nglm runtime 
#

for SWARM_HOST in $SWARM_HOSTS
do
   ssh $SWARM_HOST "
      rm -rf ${NGLM_STREAMS_RUNTIME}
   "
done

#
#  remove and recreate nglm data
#

ssh $MASTER_SWARM_HOST "
   rm -rf ${NGLM_DATA}
"

#########################################
#
#  additional one-time work
#
#########################################

#
# IOM tables for testing
#

if [ "$IOM_DB_PRODUCTION" = "FALSE" ]; then
  exit | /opt/mssql-tools/bin/sqlcmd -S $IOM_DB_HOST -U $IOM_DB_USER -P "$IOM_DB_PASSWORD" -i $DEPLOY_ROOT/support/offerlogs.sql
fi  

