#########################################
#
#  evolution-deploy-launch-presetup.sh
#
#########################################

docker stack deploy -c $DEPLOY_ROOT/stack/stack-mysql.yml ${DOCKER_STACK}-mysql

