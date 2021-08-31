#########################################
#
#  evolution-deploy-launch-postamble.sh
#
#########################################

docker stack deploy -c $DEPLOY_ROOT/stack/stack-gui.yml ${DOCKER_STACK}-gui
