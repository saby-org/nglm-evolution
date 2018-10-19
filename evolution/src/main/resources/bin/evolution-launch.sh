#########################################
#
#  evolution-launch.sh
#
#########################################

docker stack deploy -c $DEPLOY_ROOT/stack/stack-gui-mysql.yml --with-registry-auth <_DOCKER_STACK_>-gui-mysql
docker stack deploy -c $DEPLOY_ROOT/stack/stack-application-monitoring.yml --with-registry-auth <_DOCKER_STACK_>-application-monitoring
docker stack deploy -c $DEPLOY_ROOT/stack/stack-guimanager.yml --with-registry-auth <_DOCKER_STACK_>-guimanager

#
#  thirdpartymanager(if necessary)
#

if [ "${thirdpartymanager.enabled}" = "true" ]; then
  docker stack deploy -c $DEPLOY_ROOT/stack/stack-thirdpartymanager.yml --with-registry-auth <_DOCKER_STACK_>-thirdpartymanager
fi

docker stack deploy -c $DEPLOY_ROOT/stack/stack-evolutionengine.yml --with-registry-auth <_DOCKER_STACK_>-evolutionengine

#
#  gui -- temporary - wait an additional 30 seconds for mysql database to initialize
#

sleep 30
docker stack deploy -c $DEPLOY_ROOT/stack/stack-gui.yml --with-registry-auth <_DOCKER_STACK_>-gui

