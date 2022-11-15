#########################################
#
#  evolution-deploy-launch-postamble.sh
#
#########################################

docker stack deploy -c $DEPLOY_ROOT/stack/stack-gui.yml ${DOCKER_STACK}-gui

==================
crontab -l | grep -v cleanup.sh >> $DEPLOY_ROOT/cleanup.crontab
crontab $DEPLOY_ROOT/cleanup.crontab
CTR=`crontab -l | grep cleanup.sh | wc -l`
if test $CTR -ne 1
then
   echo "ERROR:: could not create cron entry for cleanup.sh!!"
fi
=================
