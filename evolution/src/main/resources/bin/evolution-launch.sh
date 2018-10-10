#########################################
#
#  evolution-launch.sh
#
#########################################

docker stack deploy -c $DEPLOY_ROOT/stack/stack-application-monitoring.yml <_DOCKER_STACK_>-application-monitoring
docker stack deploy -c $DEPLOY_ROOT/stack/stack-guimanager.yml <_DOCKER_STACK_>-guimanager
docker stack deploy -c $DEPLOY_ROOT/stack/stack-thirdpartymanager.yml <_DOCKER_STACK_>-thirdpartymanager
docker stack deploy -c $DEPLOY_ROOT/stack/stack-evolutionengine.yml <_DOCKER_STACK_>-evolutionengine

