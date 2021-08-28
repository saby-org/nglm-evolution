#########################################
#
#  evolution-launch-preamble.sh
#
#########################################

docker stack deploy -c $DEPLOY_ROOT/stack/stack-gui-ssl-monitoring.yml <_DOCKER_STACK_>-gui-ssl-monitoring
echo "sleeping for 5s before starting gui"
sleep 5
docker stack deploy -c $DEPLOY_ROOT/stack/stack-gui.yml <_DOCKER_STACK_>-gui
