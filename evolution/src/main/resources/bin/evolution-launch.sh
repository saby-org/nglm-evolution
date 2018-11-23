#########################################
#
#  evolution-launch.sh
#
#########################################

docker stack deploy -c $DEPLOY_ROOT/stack/stack-application-monitoring.yml <_DOCKER_STACK_>-application-monitoring
docker stack deploy -c $DEPLOY_ROOT/stack/stack-guimanager.yml <_DOCKER_STACK_>-guimanager

#
#  thirdpartymanager(if necessary)
#

if [ "${thirdpartymanager.enabled}" = "true" ]; then
  docker stack deploy -c $DEPLOY_ROOT/stack/stack-thirdpartymanager.yml <_DOCKER_STACK_>-thirdpartymanager
fi

docker stack deploy -c $DEPLOY_ROOT/stack/stack-evolutionengine.yml <_DOCKER_STACK_>-evolutionengine

#
#  gui -- temporary - wait an additional 30 seconds for mysql database to initialize
#

# Moved in carriere as the DB lives there now. Move back when MySQL is in core/storage
# No need to wait anymore as if it's past upgrade all DBs are up and running
# Search for TODO_DOCKER_STACK_GUI_DEPLOY_MOVE_BACK_IN_EVOLUTION to find it in carriere

# sleep 30
# docker stack deploy -c $DEPLOY_ROOT/stack/stack-gui.yml <_DOCKER_STACK_>-gui
