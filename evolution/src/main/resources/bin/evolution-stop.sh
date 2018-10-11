#################################################################################
#
#  evolution-stop.sh
#
#################################################################################

#
#  application stacks
#

docker stack rm <_DOCKER_STACK_>-application-monitoring
docker stack rm <_DOCKER_STACK_>-guimanager

#
#  thirdpartymanager(if necessary)
#

if [ "${thirdpartymanager.enabled}" = "true" ]; then
  docker stack rm <_DOCKER_STACK_>-thirdpartymanager
fi

docker stack rm <_DOCKER_STACK_>-evolutionengine

