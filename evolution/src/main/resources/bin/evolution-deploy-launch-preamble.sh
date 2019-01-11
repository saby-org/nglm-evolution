#########################################
#
#  evolution-deploy-launch-preamble.sh
#
#########################################

docker stack deploy -c $DEPLOY_ROOT/stack/stack-application-monitoring.yml ${DOCKER_STACK}-application-monitoring
docker stack deploy -c $DEPLOY_ROOT/stack/stack-mysql.yml ${DOCKER_STACK}-mysql
docker stack deploy -c $DEPLOY_ROOT/stack/stack-guimanager.yml ${DOCKER_STACK}-guimanager
docker stack deploy -c $DEPLOY_ROOT/stack/stack-evolutionengine.yml ${DOCKER_STACK}-evolutionengine

#
#  optional stacks (from configuration)
#

if [ "${PROPENSITYENGINE_ENABLED}" = "true" ]; then
  docker stack deploy -c $DEPLOY_ROOT/stack/stack-propensityengine.yml ${DOCKER_STACK}-propensityengine
fi

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

if [ "${REPORTMANAGER_ENABLED}" = "true" ]; then
  docker stack deploy -c $DEPLOY_ROOT/stack/stack-reportmanager.yml ${DOCKER_STACK}-reportmanager
fi

if [ "${REPORTSCHEDULER_ENABLED}" = "true" ]; then
  docker stack deploy -c $DEPLOY_ROOT/stack/stack-reportscheduler.yml ${DOCKER_STACK}-reportscheduler
fi

