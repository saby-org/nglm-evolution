  #################################################################################
  #
  #  evolution-setup-connect
  #
  #################################################################################

  #
  #  sink connector -- subscriberProfile (redis)
  #

  curl -XPOST $CONNECT_URL/connectors -H "Content-Type: application/json" -d '
    {
      "name" : "subscriberprofile_redis_sink_connector",
      "config" :
        {
        "connector.class" : "com.evolving.nglm.evolution.SubscriberProfileRedisSinkConnector",
        "tasks.max" : '$CONNECT_REDIS_SUBSCRIBERPROFILE_SINK_TASKS',
        "topics" : "${changelog.evolutionengine.subscriberstate.topic}",
        "redisSentinels" : "'$REDIS_SENTINELS'",
        "redisInstance" : "subscriberprofile",
        "defaultDBIndex"   : "0",
        "pipelined" : "true"
        }
    }' &

  #
  #  sink connector -- journeyStatistic (elasticsearch)
  #

  curl -XPOST $CONNECT_URL/connectors -H "Content-Type: application/json" -d '
    {
      "name" : "journeystatistic_es_sink_connector",
      "config" :
        {
        "connector.class" : "com.evolving.nglm.evolution.JourneyStatisticESSinkConnector",
        "tasks.max" : 1,
        "topics" : "${topic.journeystatistic}",
        "connectionHost" : "'$MASTER_ESROUTER_HOST'",
        "connectionPort" : "'$MASTER_ESROUTER_PORT'",
        "indexName" : "journeystatistic"
        }
    }' &

  #
  #  sink connector -- journeyMetric (elasticsearch)
  #

  curl -XPOST $CONNECT_URL/connectors -H "Content-Type: application/json" -d '
    {
      "name" : "journeymetric_es_sink_connector",
      "config" :
        {
        "connector.class" : "com.evolving.nglm.evolution.JourneyMetricESSinkConnector",
        "tasks.max" : 1,
        "topics" : "${topic.journeymetric}",
        "connectionHost" : "'$MASTER_ESROUTER_HOST'",
        "connectionPort" : "'$MASTER_ESROUTER_PORT'",
        "batchRecordCount" : "'$ES_BATCH_RECORD_COUNT'",
        "indexName" : "journeymetric"
        }
    }' &

  #
  #  source connector -- externalDeliveryRequest
  #

  curl -XPOST $CONNECT_URL/connectors -H "Content-Type: application/json" -d '
    {
      "name" : "externaldeliveryrequest_file_connector",
      "config" :
        {
          "connector.class" : "com.evolving.nglm.evolution.ExternalDeliveryRequestFileSourceConnector",
          "tasks.max" : 1,
          "directory" : "/app/data/externaldeliveryrequests",
          "filenamePattern" : "^.*(\\.gz)?(?<!\\.tmp)$",
          "pollMaxRecords" : 5,
          "pollingInterval" : 10,
          "verifySizeInterval" : 0,
          "topic" : "(automatically populated)",
          "bootstrapServers" : "'$BROKER_SERVERS'",
          "internalTopic" : "${topic.externaldeliveryrequest_fileconnector}",
          "archiveDirectory" : "/app/data/externaldeliveryrequestsarchive"
        }
    }' &

  #
  #  source connector -- presentationLog
  #

  curl -XPOST $CONNECT_URL/connectors -H "Content-Type: application/json" -d '
    {
      "name" : "presentationlog_file_connector",
      "config" :
        {
        "connector.class" : "com.evolving.nglm.evolution.PresentationLogFileSourceConnector",
        "tasks.max" : 1,
        "directory" : "/app/data/presentationlog",
        "filenamePattern" : "^.*(\\.gz)?(?<!\\.tmp)$",
        "pollMaxRecords" : 5,
        "pollingInterval" : 10,
        "verifySizeInterval" : 0,
        "topic" : "${topic.presentationlog}",
        "bootstrapServers" : "'$BROKER_SERVERS'",
        "internalTopic" : "${topic.presentationlog_fileconnector}",
        "archiveDirectory" : "/app/data/presentationlogarchive"
        }
    }' &

  #
  #  source connector -- acceptanceLog
  #

  curl -XPOST $CONNECT_URL/connectors -H "Content-Type: application/json" -d '
    {
      "name" : "acceptancelog_file_connector",
      "config" :
        {
        "connector.class" : "com.evolving.nglm.evolution.AcceptanceLogFileSourceConnector",
        "tasks.max" : 1,
        "directory" : "/app/data/acceptancelog",
        "filenamePattern" : "^.*(\\.gz)?(?<!\\.tmp)$",
        "pollMaxRecords" : 5,
        "pollingInterval" : 10,
        "verifySizeInterval" : 0,
        "topic" : "${topic.acceptancelog}",
        "bootstrapServers" : "'$BROKER_SERVERS'",
        "internalTopic" : "${topic.acceptancelog_fileconnector}",
        "archiveDirectory" : "/app/data/acceptancelogarchive"
        }
    }' &

  #
  #  source connector -- SubscriberProfileForceUpdateFileSourceConnector
  #

  curl -XPOST $CONNECT_URL/connectors -H "Content-Type: application/json" -d '
    {
      "name" : "SubscriberProfileForceUpdateFileSourceConnector",
      "config" :
        {
        "connector.class" : "com.evolving.nglm.evolution.SubscriberProfileForceUpdateFileSourceConnector",
        "tasks.max" : 1,
        "directory" : "/app/data/subscriberprofileforceupdate",
        "filenamePattern" : "^.*(\\.gz)?(?<!\\.tmp)$",
        "pollMaxRecords" : 5,
        "pollingInterval" : 10,
        "verifySizeInterval" : 0,
        "topic" : "${topic.subscriberprofileforceupdate}",
        "bootstrapServers" : "'$BROKER_SERVERS'",
        "internalTopic" : "${topic.subscriberprofileforceupdate_fileconnector}",
        "archiveDirectory" : "/app/data/subscriberprofileforceupdatearchive"
        }
    }' &

  #
  #  sink connector -- propensity (elasticsearch)
  #

  curl -XPOST $CONNECT_URL/connectors -H "Content-Type: application/json" -d '
    {
      "name" : "propensity_es_sink_connector",
      "config" :
        {
        "connector.class" : "com.evolving.nglm.evolution.PropensityESSinkConnector",
        "tasks.max" : 1,
        "topics" : "${topic.propensitylog}",
        "connectionHost" : "'$MASTER_ESROUTER_HOST'",
        "connectionPort" : "'$MASTER_ESROUTER_PORT'",
        "indexName" : "propensity"
        }
    }' &

  #
  #  sink connector -- ODR (elasticsearch)
  #

  curl -XPOST $CONNECT_URL/connectors -H "Content-Type: application/json" -d '
     {
       "name" : "odr_es_sink_connector",
       "config" :
         {
         "connector.class" : "com.evolving.nglm.evolution.ODRSinkConnector",
         "tasks.max" : 1,
         "topics" : "${topic.fulfillment.purchasefulfillment.response}",
         "connectionHost" : "'$MASTER_ESROUTER_HOST'",
         "connectionPort" : "'$MASTER_ESROUTER_PORT'",
         "indexName" : "odr"
         }
     }' &
   
  #
  #  sink connector -- BDR (elasticsearch)
  #

  curl -XPOST $CONNECT_URL/connectors -H "Content-Type: application/json" -d '
     {
       "name" : "bdr_es_sink_connector",
       "config" :
         {
         "connector.class" : "com.evolving.nglm.evolution.BDRSinkConnector",
         "tasks.max" : 1,
         "topics" : "${topic.commoditydelivery.response}",
         "connectionHost" : "'$MASTER_ESROUTER_HOST'",
         "connectionPort" : "'$MASTER_ESROUTER_PORT'",
         "indexName" : "bdr"
         }
     }' &
  
  #
  #  sink connector -- Notification (elasticsearch)
  #

  curl -XPOST $CONNECT_URL/connectors -H "Content-Type: application/json" -d '
    {
      "name" : "notification_es_sink_connector",
      "config" :
        {
        "connector.class" : "com.evolving.nglm.evolution.NotificationSinkConnector",
        "tasks.max" : 1,
        "topics" : "${topic.notificationmanagermail.response},${topic.notificationmanagersms.response}",
        "connectionHost" : "'$MASTER_ESROUTER_HOST'",
        "connectionPort" : "'$MASTER_ESROUTER_PORT'",
        "indexName" : "notification"
        }
    }' &

  #
  #  source connector -- periodicEvaluation
  #

  if [ "${env.USE_REGRESSION}" = "1" ]
  then
  curl -XPOST $CONNECT_URL/connectors -H "Content-Type: application/json" -d '
    {
      "name" : "periodicevaluation_file_connector",
      "config" :
        {
        "connector.class" : "com.evolving.nglm.evolution.PeriodicEvaluationFileSourceConnector",
        "tasks.max" : 1,
        "directory" : "/app/data/periodicevaluation",
        "filenamePattern" : "^.*(\\.gz)?(?<!\\.tmp)$",
        "pollMaxRecords" : 5,
        "pollingInterval" : 10,
        "verifySizeInterval" : 0,
        "topic" : "${topic.timedevaluation}",
        "bootstrapServers" : "'$BROKER_SERVERS'",
        "internalTopic" : "${topic.periodicevaluation_fileconnector}",
        "archiveDirectory" : "/app/data/periodicevaluationarchive"
        }
    }' &
  fi

wait
