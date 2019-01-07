#########################################
#
#  evolution-deploy-launch.sh
#
#########################################

docker stack deploy -c $DEPLOY_ROOT/stack/stack-application-monitoring.yml ${DOCKER_STACK}-application-monitoring
docker stack deploy -c $DEPLOY_ROOT/stack/stack-guimanager.yml ${DOCKER_STACK}-guimanager
docker stack deploy -c $DEPLOY_ROOT/stack/stack-evolutionengine.yml ${DOCKER_STACK}-evolutionengine
docker stack deploy -c $DEPLOY_ROOT/stack/stack-propensityengine.yml ${DOCKER_STACK}-propensityengine

#
#  optional stacks (from configuration)
#

if [ "${THIRDPARTYMANAGER_ENABLED}" = "true" ]; then
  docker stack deploy -c $DEPLOY_ROOT/stack/stack-thirdpartymanager.yml ${DOCKER_STACK}-thirdpartymanager
fi

if [ "${INFULFILLMENTMANAGER_ENABLED}" = "true" ]; then
  docker stack deploy -c $DEPLOY_ROOT/stack/stack-infulfillmentmanager.yml ${DOCKER_STACK}-infulfillmentmanager
fi

if [ "${PURCHASEFULFILLMENTMANAGER_ENABLED}" = "true" ]; then
  docker stack deploy -c $DEPLOY_ROOT/stack/stack-purchasefulfillmentmanager.yml ${DOCKER_STACK}-purchasefulfillmentmanager
fi

if [ "${NOTIFICATIONMANAGER_SMS_ENABLED}" = "true" ]; then
  docker stack deploy -c $DEPLOY_ROOT/stack/stack-notificationmanagersms.yml ${DOCKER_STACK}-notificationmanagersms
fi

if [ "${NOTIFICATIONMANAGER_MAIL_ENABLED}" = "true" ]; then
  docker stack deploy -c $DEPLOY_ROOT/stack/stack-notificationmanagermail.yml ${DOCKER_STACK}-notificationmanagermail
fi  

#
#  gui -- temporary - wait an additional 90 seconds for mysql database to initialize
#

# Moved in carriere as the DB lives there now. Move back when MySQL is in core/storage
# No need to wait anymore as if it's past upgrade all DBs are up and running
# Search for TODO_DOCKER_STACK_GUI_DEPLOY_MOVE_BACK_IN_EVOLUTION to find it in carriere

# sleep 120
# docker exec -it ev-gui-mysql_fwk-mysqldb.1.$(docker service ps -f 'name=ev-gui-mysql_fwk-mysqldb.1' ev-gui-mysql_fwk-mysqldb -q --no-trunc | head -n1) mysql -u root -p${MYSQL_ROOT_PASSWORD} -e "use dbframework;update tbl_apps set web_link=REPLACE(web_link,'localhost','${GUI_MYSQL_SERVER_HOST_IP}');" > /dev/null 2>&1
# docker stack deploy -c $DEPLOY_ROOT/stack/stack-gui.yml ${DOCKER_STACK}-gui
