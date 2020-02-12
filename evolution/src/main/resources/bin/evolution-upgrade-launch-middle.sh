#########################################
#
#  evolution-upgrade-launch-middle.sh
#
#########################################

docker stack deploy -c $DEPLOY_ROOT/stack/stack-upgrade.yml ${DOCKER_STACK}-upgrade

#
#  wait for upgrade to complete
#

echo waiting for upgrade to complete ...
UPGRADE_EXECUTING=1
while [ $UPGRADE_EXECUTING -ne 0 ]; do
  ( docker service logs `docker service ls | grep ${DOCKER_STACK}-upgrade | cut -d' ' -f1` 2>&1 ) | grep -q "Created /evolving/nglm/upgraded"
  UPGRADE_EXECUTING=$?
  if [ $UPGRADE_EXECUTING -ne 0 ]; then
    echo upgrade executing ...
    sleep 5
  fi
done
echo upgrade complete

