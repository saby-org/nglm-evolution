#########################################
#
#  evolution-deploy-launch.sh
#
#########################################

docker stack deploy -c $DEPLOY_ROOT/stack/stack-gui-mysql.yml --with-registry-auth ${DOCKER_STACK}-gui-mysql
docker stack deploy -c $DEPLOY_ROOT/stack/stack-application-monitoring.yml --with-registry-auth ${DOCKER_STACK}-application-monitoring
docker stack deploy -c $DEPLOY_ROOT/stack/stack-guimanager.yml --with-registry-auth ${DOCKER_STACK}-guimanager

#
#  thirdpartymanager(if necessary)
#

if [ "${thirdpartymanager.enabled}" = "true" ]; then
  docker stack deploy -c $DEPLOY_ROOT/stack/stack-thirdpartymanager.yml --with-registry-auth ${DOCKER_STACK}-thirdpartymanager
fi

docker stack deploy -c $DEPLOY_ROOT/stack/stack-evolutionengine.yml --with-registry-auth ${DOCKER_STACK}-evolutionengine

#
#  gui -- temporary - wait an additional 90 seconds for mysql database to initialize
#

sleep 120
docker exec -it ev-gui-mysql_fwk-mysqldb.1.$(docker service ps -f 'name=ev-gui-mysql_fwk-mysqldb.1' ev-gui-mysql_fwk-mysqldb -q --no-trunc | head -n1) mysql -u root -p${GUI_MYSQLDB_PASSWORD} -e "use dbframework;update tbl_apps set web_link=REPLACE(web_link,'localhost','${GUI_MYSQLDB_HOST_IP}');" > /dev/null 2>&1
docker stack deploy -c $DEPLOY_ROOT/stack/stack-gui.yml --with-registry-auth ${DOCKER_STACK}-gui

