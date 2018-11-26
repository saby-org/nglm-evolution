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

#
#  hack -- remove gui containers
#

sleep 10
if [ "$(docker ps -aq -f name=ev-gui_fwk-web)" ]; then
  docker kill $(docker ps -aq -f name=ev-gui_fwk-web) > /dev/null 2>&1
  docker rm $(docker ps -aq -f name=ev-gui_fwk-web)> /dev/null 2>&1
fi
if [ "$(docker ps -aq -f name=ev-gui_fwkauth-api)" ]; then
  docker kill $(docker ps -aq -f name=ev-gui_fwkauth-api) > /dev/null 2>&1
  docker rm $(docker ps -aq -f name=ev-gui_fwkauth-api) > /dev/null 2>&1
fi
if [ "$(docker ps -aq -f name=ev-gui_fwk-api)" ]; then
  docker kill $(docker ps -aq -f name=ev-gui_fwk-api) > /dev/null 2>&1
  docker rm $(docker ps -aq -f name=ev-gui_fwk-api) > /dev/null 2>&1
fi
if [ "$(docker ps -aq -f name=ev-gui-mysql_fwk-mysqldb)" ]; then
  docker kill $(docker ps -aq -f name=ev-gui-mysql_fwk-mysqldb) > /dev/null 2>&1
  docker rm $(docker ps -aq -f name=ev-gui-mysql_fwk-mysqldb) > /dev/null 2>&1
fi
