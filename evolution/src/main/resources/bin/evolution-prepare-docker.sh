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
   echo "deployment-prepare-docker on $SWARM_HOST"
   ssh $SWARM_HOST "
      docker pull ${env.DOCKER_REGISTRY}ev-criteriaapi:${project.name}-${project.version}
      docker pull ${env.DOCKER_REGISTRY}ev-subscribergroup:${project.name}-${project.version}
   " &
done

#
#  wait for all pulls to complete
#

wait
echo "evolution-prepare-docker complete"

