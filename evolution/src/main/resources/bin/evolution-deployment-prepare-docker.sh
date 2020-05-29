#################################################################################
#
#  evolution-deployment-prepare-docker.sh
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
      docker pull ${env.DOCKER_REGISTRY}ev-guimanager:${project.name}-${project.version}
      docker pull ${env.DOCKER_REGISTRY}ev-subscribergroup:${project.name}-${project.version}
      docker pull ${env.DOCKER_REGISTRY}ev-storeconfiguration:${project.name}-${project.version}
      docker pull ${env.DOCKER_REGISTRY}ev-simpletransform:${project.name}-${project.version}
      docker pull ${env.DOCKER_REGISTRY}ev-connect:${project.name}-${project.version}
      docker pull ${env.DOCKER_REGISTRY}ev-subscribermanager:${project.name}-${project.version}
      docker pull ${env.DOCKER_REGISTRY}ev-setup:${project.name}-${project.version}
      docker pull ${env.DOCKER_REGISTRY}ev-analyse-upgrade:${project.name}-${project.version}
      docker pull ${env.DOCKER_REGISTRY}ev-grafana:${project.name}-${project.version}
      docker pull ${env.DOCKER_REGISTRY}ev-licensemanager:${project.name}-${project.version}
      docker pull ${env.DOCKER_REGISTRY}ev-upgrade:${project.name}-${project.version}
      docker pull ${env.DOCKER_REGISTRY}ev-prometheus:${evolving.prometheus.version}
   " &
done

#
#  wait for all pulls to complete
#

wait
echo "evolution-deployment-prepare-docker complete"

