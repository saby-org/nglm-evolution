#################################################################################
#
#  evolution-prepare-docker.sh
#
#################################################################################

#########################################
#
#  docker pulls
#
#########################################

for SWARM_HOST in $SWARM_HOSTS
do
   echo "evolution-prepare-docker on $SWARM_HOST"
   ssh $SWARM_HOST "
      docker pull ${env.DOCKER_REGISTRY}fwk.mysqldb:${gui-fwk.version}
      docker pull ${env.DOCKER_REGISTRY}fwk.api:${gui-fwk.version}
      docker pull ${env.DOCKER_REGISTRY}fwkauth.api:${gui-fwk.version}
      docker pull ${env.DOCKER_REGISTRY}fwk.web:${gui-fwk.version}
   " &
done

#
#  wait for all pulls to complete
#

wait
echo "evolution-prepare-docker complete"

