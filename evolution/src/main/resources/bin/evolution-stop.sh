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
docker stack rm <_DOCKER_STACK_>-gui

#
#  thirdpartymanager(if necessary)
#

if [ "${thirdpartymanager.enabled}" = "true" ]; then
  docker stack rm <_DOCKER_STACK_>-thirdpartymanager
fi

docker stack rm <_DOCKER_STACK_>-evolutionengine
docker stack rm <_DOCKER_STACK_>-gui-mysql

#
#  hack -- remove gui containers
#

sleep 5
if [ "$(docker ps -aq -f name=ev-gui_fwk-web)" ]; then
  docker kill $(docker ps -aq -f name=ev-gui_fwk-web)
  docker rm $(docker ps -aq -f name=ev-gui_fwk-web)
fi
if [ "$(docker ps -aq -f name=ev-gui_fwkauth-api)" ]; then
  docker kill $(docker ps -aq -f name=ev-gui_fwkauth-api)
  docker rm $(docker ps -aq -f name=ev-gui_fwkauth-api)
fi
if [ "$(docker ps -aq -f name=ev-gui_fwk-api)" ]; then
  docker kill $(docker ps -aq -f name=ev-gui_fwk-api)
  docker rm $(docker ps -aq -f name=ev-gui_fwk-api)
fi

