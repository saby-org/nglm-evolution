
#################################################################################
#
#  stop application stacks
#
#################################################################################

#
#  application stacks
#

docker stack rm <_DOCKER_STACK_>-application-monitoring
docker stack rm <_DOCKER_STACK_>-guimanager
docker stack rm <_DOCKER_STACK_>-profileengine
