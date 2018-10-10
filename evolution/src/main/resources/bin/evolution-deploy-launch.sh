#########################################
#
#  evolution-deploy-launch.sh
#
#########################################

docker stack deploy -c $DEPLOY_ROOT/stack/stack-application-monitoring.yml ${DOCKER_STACK}-application-monitoring
docker stack deploy -c $DEPLOY_ROOT/stack/stack-guimanager.yml ${DOCKER_STACK}-guimanager
docker stack deploy -c $DEPLOY_ROOT/stack/stack-thirdpartymanager.yml ${DOCKER_STACK}-thirdpartymanager
docker stack deploy -c $DEPLOY_ROOT/stack/stack-evolutionengine.yml ${DOCKER_STACK}-evolutionengine

