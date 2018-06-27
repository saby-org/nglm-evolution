
#########################################
#
#  deploy application stack(s)
#
#########################################

docker stack deploy -c $DEPLOY_ROOT/stack/stack-application-monitoring.yml ${DOCKER_STACK}-application-monitoring
docker stack deploy -c $DEPLOY_ROOT/stack/stack-guimanager.yml ${DOCKER_STACK}-guimanager
docker stack deploy -c $DEPLOY_ROOT/stack/stack-profileengine.yml ${DOCKER_STACK}-profileengine
docker stack deploy -c $DEPLOY_ROOT/stack/stack-nbo-to-redis.yml ${DOCKER_STACK}-nbo-to-redis
