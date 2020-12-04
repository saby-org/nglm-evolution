#########################################
#
#  evolution-launch-preamble.sh
#
#########################################

docker stack deploy -c $DEPLOY_ROOT/stack/stack-application-monitoring.yml <_DOCKER_STACK_>-application-monitoring
docker stack deploy -c $DEPLOY_ROOT/stack/stack-mysql.yml <_DOCKER_STACK_>-mysql
docker stack deploy -c $DEPLOY_ROOT/stack/stack-guimanager.yml <_DOCKER_STACK_>-guimanager
docker stack deploy -c $DEPLOY_ROOT/stack/stack-evolutionengine.yml <_DOCKER_STACK_>-evolutionengine

#
#  optional stacks (from configuration)
#

if [ "<_UCGENGINE_ENABLED_>" = "true" ]; then
  docker stack deploy -c $DEPLOY_ROOT/stack/stack-ucgengine.yml <_DOCKER_STACK_>-ucgengine
fi

if [ "<_FAKEEMULATORS_ENABLED_>" = "true" ]; then
  docker stack deploy -c $DEPLOY_ROOT/stack/stack-fake.yml <_DOCKER_STACK_>-fake
fi

if [ "<_THIRDPARTYMANAGER_ENABLED_>" = "true" ]; then
  docker stack deploy -c $DEPLOY_ROOT/stack/stack-thirdpartymanager.yml <_DOCKER_STACK_>-thirdpartymanager
fi

if [ "<_DNBOPROXY_ENABLED_>" = "true" ]; then
  docker stack deploy -c $DEPLOY_ROOT/stack/stack-dnboproxy.yml <_DOCKER_STACK_>-dnboproxy
fi

if [ "<_INFULFILLMENTMANAGER_ENABLED_>" = "true" ]; then
  docker stack deploy -c $DEPLOY_ROOT/stack/stack-infulfillmentmanager.yml <_DOCKER_STACK_>-infulfillmentmanager
fi

if [ "<_COMMODITYDELIVERYMANAGER_ENABLED_>" = "true" ]; then
  docker stack deploy -c $DEPLOY_ROOT/stack/stack-commoditydeliverymanager.yml <_DOCKER_STACK_>-commoditydeliverymanager
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

if [ "<_NOTIFICATIONMANAGER_PUSH_ENABLED_>" = "true" ]; then
  docker stack deploy -c $DEPLOY_ROOT/stack/stack-notificationmanagerpush.yml <_DOCKER_STACK_>-notificationmanagerpush
fi

if [ "<_NOTIFICATIONMANAGER_ENABLED_>" = "true" ]; then
  docker stack deploy -c $DEPLOY_ROOT/stack/stack-notificationmanager.yml <_DOCKER_STACK_>-notificationmanager
fi
  

if [ "<_REPORTMANAGER_ENABLED_>" = "true" ]; then
  docker stack deploy -c $DEPLOY_ROOT/stack/stack-reportmanager.yml <_DOCKER_STACK_>-reportmanager
fi

if [ "<_REPORTSCHEDULER_ENABLED_>" = "true" ]; then
  docker stack deploy -c $DEPLOY_ROOT/stack/stack-reportscheduler.yml <_DOCKER_STACK_>-reportscheduler
fi

if [ "<_DATACUBEMANAGER_ENABLED_>" = "true" ]; then
  docker stack deploy -c $DEPLOY_ROOT/stack/stack-datacubemanager.yml <_DOCKER_STACK_>-datacubemanager
fi

if [ "<_EXTRACTMANAGER_ENABLED_>" = "true" ]; then
  docker stack deploy -c $DEPLOY_ROOT/stack/stack-extractmanager.yml <_DOCKER_STACK_>-extractmanager
fi

