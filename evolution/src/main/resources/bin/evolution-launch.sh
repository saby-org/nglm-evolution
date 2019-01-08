#########################################
#
#  evolution-launch.sh
#
#########################################

docker stack deploy -c $DEPLOY_ROOT/stack/stack-application-monitoring.yml <_DOCKER_STACK_>-application-monitoring
docker stack deploy -c $DEPLOY_ROOT/stack/stack-guimanager.yml <_DOCKER_STACK_>-guimanager
docker stack deploy -c $DEPLOY_ROOT/stack/stack-evolutionengine.yml <_DOCKER_STACK_>-evolutionengine

#
#  optional stacks (from configuration)
#

if [ "<_PROPENSITYENGINE_ENABLED_>" = "true" ]; then
  docker stack deploy -c $DEPLOY_ROOT/stack/stack-propensityengine.yml <_DOCKER_STACK_>-propensityengine
fi

if [ "<_THIRDPARTYMANAGER_ENABLED_>" = "true" ]; then
  docker stack deploy -c $DEPLOY_ROOT/stack/stack-thirdpartymanager.yml <_DOCKER_STACK_>-thirdpartymanager
fi

if [ "<_INFULFILLMENTMANAGER_ENABLED_>" = "true" ]; then
  docker stack deploy -c $DEPLOY_ROOT/stack/stack-infulfillmentmanager.yml <_DOCKER_STACK_>-infulfillmentmanager
fi

if [ "<_PURCHASEFULFILLMENTMANAGER_ENABLED_>" = "true" ]; then
  docker stack deploy -c $DEPLOY_ROOT/stack/stack-purchasefulfillmentmanager.yml <_DOCKER_STACK_>-purchasefulfillmentmanager
fi

if [ "<_NOTIFICATIONMANAGER_SMS_ENABLED_>" = "true" ]; then
  docker stack deploy -c $DEPLOY_ROOT/stack/stack-notificationmanagersms.yml <_DOCKER_STACK_>-notificationmanagersms
fi

if [ "<_NOTIFICATIONMANAGER_MAIL_ENABLED_>" = "true" ]; then
  docker stack deploy -c $DEPLOY_ROOT/stack/stack-notificationmanagermail.yml <_DOCKER_STACK_>-notificationmanagermail
fi  

if [ "<_REPORTMANAGER_ENABLED_>" = "true" ]; then
  docker stack deploy -c $DEPLOY_ROOT/stack/stack-reportmanager.yml <_DOCKER_STACK_>-reportmanager
fi

if [ "<_REPORTSCHEDULER_ENABLED_>" = "true" ]; then
  docker stack deploy -c $DEPLOY_ROOT/stack/stack-reportscheduler.yml <_DOCKER_STACK_>-reportscheduler
fi


#
#  gui -- temporary - wait an additional 30 seconds for mysql database to initialize
#

# Moved in carriere as the DB lives there now. Move back when MySQL is in core/storage
# No need to wait anymore as if it's past upgrade all DBs are up and running
# Search for TODO_DOCKER_STACK_GUI_DEPLOY_MOVE_BACK_IN_EVOLUTION to find it in carriere

# sleep 30
# docker stack deploy -c $DEPLOY_ROOT/stack/stack-gui.yml <_DOCKER_STACK_>-gui
