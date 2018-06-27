
#########################################
#
#  deploy application stack(s)
#
#########################################

docker stack deploy -c $DEPLOY_ROOT/stack/stack-application-monitoring.yml <_DOCKER_STACK_>-application-monitoring
docker stack deploy -c $DEPLOY_ROOT/stack/stack-guimanager.yml <_DOCKER_STACK_>-guimanager
docker stack deploy -c $DEPLOY_ROOT/stack/stack-profileengine.yml <_DOCKER_STACK_>-profileengine
docker stack deploy -c $DEPLOY_ROOT/stack/stack-nbo-to-redis.yml <_DOCKER_STACK_>-nbo-to-redis
