#########################################
#
#  evolution-launch-preamble.sh
#
#########################################

docker stack deploy -c $DEPLOY_ROOT/stack/stack-gui.yml <_DOCKER_STACK_>-gui

#########################################
#
#  set cleanup script
#
#########################################

if test -e $DEPLOY_ROOT/bin/cleanup.sh
then
   CTR=`crontab -l | grep -v cleanup.sh | wc -l`
   if test $CTR -gt 0
   then
      crontab -l | grep -v cleanup.sh > $DEPLOY_ROOT/config/cleanup.crontab
   fi
   echo "* * * * * $DEPLOY_ROOT/bin/cleanup.sh" >> $DEPLOY_ROOT/config/cleanup.crontab
   cd $DEPLOY_ROOT
   crontab ./config/cleanup.crontab
   cd -
   CTR=`crontab -l | grep cleanup.sh | wc -l`
   if test $CTR -ne 1
   then
      echo "ERROR:: could not create cron entry for cleanup.sh!!"
   else
      echo "Cleanup script successfully set in crontab"
   fi
else
   echo "ERROR:: could not find the script $DEPLOY_ROOT/bin/cleanup.sh !!"
fi
