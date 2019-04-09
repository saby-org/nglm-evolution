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
      docker pull ${env.DOCKER_REGISTRY}ev-thirdpartymanager:${project.name}-${project.version}
      docker pull ${env.DOCKER_REGISTRY}ev-evolutionengine:${project.name}-${project.version}
      docker pull ${env.DOCKER_REGISTRY}ev-subscribergroup:${project.name}-${project.version}
      docker pull ${env.DOCKER_REGISTRY}ev-simpletransform:${project.name}-${project.version}
      docker pull ${env.DOCKER_REGISTRY}ev-connect:${project.name}-${project.version}
      docker pull ${env.DOCKER_REGISTRY}ev-subscribermanager:${project.name}-${project.version}
      docker pull ${env.DOCKER_REGISTRY}ev-setup:${project.name}-${project.version}
      docker pull ${env.DOCKER_REGISTRY}ev-prometheus-core:${project.name}-${project.version}
      docker pull ${env.DOCKER_REGISTRY}ev-prometheus-environment:${project.name}-${project.version}
      docker pull ${env.DOCKER_REGISTRY}ev-prometheus-application:${project.name}-${project.version}
      docker pull ${env.DOCKER_REGISTRY}ev-grafana:${project.name}-${project.version}
      docker pull ${env.DOCKER_REGISTRY}ev-licensemanager:${project.name}-${project.version}
      docker pull ${env.DOCKER_REGISTRY}ev-propensityengine:${project.name}-${project.version}
      docker pull ${env.DOCKER_REGISTRY}ev-ucgengine:${project.name}-${project.version}
      docker pull ${env.DOCKER_REGISTRY}ev-infulfillmentmanager:${project.name}-${project.version}
      docker pull ${env.DOCKER_REGISTRY}ev-emptyfulfillmentmanager:${project.name}-${project.version}
      docker pull ${env.DOCKER_REGISTRY}ev-pointtypefulfillmentmanager:${project.name}-${project.version}
      docker pull ${env.DOCKER_REGISTRY}ev-commoditydeliverymanager:${project.name}-${project.version}
      docker pull ${env.DOCKER_REGISTRY}ev-purchasefulfillmentmanager:${project.name}-${project.version}
      docker pull ${env.DOCKER_REGISTRY}ev-notificationmanagersms:${project.name}-${project.version}
      docker pull ${env.DOCKER_REGISTRY}ev-notificationmanagermail:${project.name}-${project.version}
      docker pull ${env.DOCKER_REGISTRY}ev-reportmanager:${project.name}-${project.version}
      docker pull ${env.DOCKER_REGISTRY}ev-reportscheduler:${project.name}-${project.version}
      docker pull ${env.DOCKER_REGISTRY}ev-upgrade:${project.name}-${project.version}
      docker pull ${env.DOCKER_REGISTRY}ev-suspenseprocessor:${project.name}-${project.version}
   " &
done

#
#  wait for all pulls to complete
#

wait
echo "evolution-deployment-prepare-docker complete"

