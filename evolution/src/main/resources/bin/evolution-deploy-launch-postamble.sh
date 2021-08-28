#########################################
#
#  evolution-deploy-launch-postamble.sh
#
#########################################

docker stack deploy -c $DEPLOY_ROOT/stack/stack-gui-ssl-monitoring.yml ${DOCKER_STACK}-gui-ssl-monitoring
echo "sleeping for 5s before starting gui"
sleep 5
docker stack deploy -c $DEPLOY_ROOT/stack/stack-gui.yml ${DOCKER_STACK}-gui
