#################################################################################
#
#  evolution-stop.sh
#
#################################################################################

#
#  application stacks
#

docker stack rm <_DOCKER_STACK_>-upgrade
docker stack rm <_DOCKER_STACK_>-application-monitoring
docker stack rm <_DOCKER_STACK_>-mysql
docker stack rm <_DOCKER_STACK_>-guimanager
docker stack rm <_DOCKER_STACK_>-gui
docker stack rm <_DOCKER_STACK_>-evolutionengine

#
#  optional stacks (from configuration)
#

if [ "<_UCGENGINE_ENABLED_>" = "true" ]; then
  docker stack rm <_DOCKER_STACK_>-ucgengine
fi

if [ "<_FAKEEMULATORS_ENABLED_>" = "true" ]; then
  docker stack rm <_DOCKER_STACK_>-fake
fi

if [ "<_PROPENSITYENGINE_ENABLED_>" = "true" ]; then
  docker stack rm <_DOCKER_STACK_>-propensityengine
fi

if [ "<_THIRDPARTYMANAGER_ENABLED_>" = "true" ]; then
  docker stack rm <_DOCKER_STACK_>-thirdpartymanager
fi

if [ "<_INFULFILLMENTMANAGER_ENABLED_>" = "true" ]; then
  docker stack rm <_DOCKER_STACK_>-infulfillmentmanager
fi

if [ "<_PURCHASEFULFILLMENTMANAGER_ENABLED_>" = "true" ]; then
  docker stack rm <_DOCKER_STACK_>-purchasefulfillmentmanager
fi

if [ "<_NOTIFICATIONMANAGER_SMS_ENABLED_>" = "true" ]; then
  docker stack rm <_DOCKER_STACK_>-notificationmanagersms
fi

if [ "<_NOTIFICATIONMANAGER_MAIL_ENABLED_>" = "true" ]; then
  docker stack rm <_DOCKER_STACK_>-notificationmanagermail
fi  

if [ "<_REPORTMANAGER_ENABLED_>" = "true" ]; then
  docker stack rm <_DOCKER_STACK_>-reportmanager
fi

if [ "<_REPORTSCHEDULER_ENABLED_>" = "true" ]; then
  docker stack rm <_DOCKER_STACK_>-reportscheduler
fi  

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
