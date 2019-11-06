  #################################################################################
  #
  #  evolution-setup-connect
  #
  #################################################################################

  #
  #  sink connector -- subscriberProfile (redis)
  #

  export CONNECT_URL_SUBSCRIBERPROFILE_REDIS=${CONNECT_URL_SUBSCRIBERPROFILE_REDIS:-$DEFAULT_CONNECT_URL}
  curl -XPOST $CONNECT_URL_SUBSCRIBERPROFILE_REDIS/connectors -H "Content-Type: application/json" -d '
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
  #  sink connector -- subscriberProfile (elasticsearch)
  #

  export CONNECT_URL_SUBSCRIBERPROFILE_ES=${CONNECT_URL_SUBSCRIBERPROFILE_ES:-$DEFAULT_CONNECT_URL}
  curl -XPOST $CONNECT_URL_SUBSCRIBERPROFILE_ES/connectors -H "Content-Type: application/json" -d '
    {
      "name" : "subscriberprofile_es_sink_connector",
      "config" :
        {
        "connector.class" : "${connector.sink.elasticsearch.subscriberprofile}",
        "tasks.max" : '$CONNECT_ES_SUBSCRIBERPROFILE_SINK_TASKS',
        "topics" : "${changelog.evolutionengine.subscriberstate.topic}",
        "connectionHost" : "'$MASTER_ESROUTER_HOST'",
        "connectionPort" : "'$MASTER_ESROUTER_PORT'",
        "indexName" : "subscriberprofile",
	"batchRecordCount" : "'$CONNECT_ES_SUBSCRIBERPROFILE_BATCHRECORDCOUNT'",
	"batchSize" : "'$CONNECT_ES_SUBSCRIBERPROFILE_BATCHSIZEMB'"
        }
    }' &
  
  #
  #  sink connector -- extendedSubscriberProfile (elasticsearch)
  #

  export CONNECT_URL_EXTENDEDSUBSCRIBERPROFILE_ES=${CONNECT_URL_EXTENDEDSUBSCRIBERPROFILE_ES:-$DEFAULT_CONNECT_URL}
  curl -XPOST $CONNECT_URL_EXTENDEDSUBSCRIBERPROFILE_ES/connectors -H "Content-Type: application/json" -d '
    {
      "name" : "extendedsubscriberprofile_es_sink_connector",
      "config" :
        {
        "connector.class" : "${connector.sink.elasticsearch.extendedsubscriberprofile}",
        "tasks.max" : '$CONNECT_ES_EXTENDEDSUBSCRIBERPROFILE_SINK_TASKS',
        "topics" : "${changelog.evolutionengine.extendedsubscriberprofile.topic}",
        "connectionHost" : "'$MASTER_ESROUTER_HOST'",
        "connectionPort" : "'$MASTER_ESROUTER_PORT'",
        "indexName" : "subscriberprofile",
	"batchRecordCount" : "'$CONNECT_ES_EXTENDEDSUBSCRIBERPROFILE_BATCHRECORDCOUNT'",
	"batchSize" : "'$CONNECT_ES_EXTENDEDSUBSCRIBERPROFILE_BATCHSIZEMB'"
        }
    }' &
    
  #
  #  sink connector -- journeyStatistic (elasticsearch)
  #

  export CONNECT_URL_JOURNEYSTATISTIC_ES=${CONNECT_URL_JOURNEYSTATISTIC_ES:-$DEFAULT_CONNECT_URL}
  curl -XPOST $CONNECT_URL_JOURNEYSTATISTIC_ES/connectors -H "Content-Type: application/json" -d '
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
  #  sink connector -- journeyTraffic (elasticsearch)
  #

  export CONNECT_URL_JOURNEYTRAFFIC_ES=${CONNECT_URL_JOURNEYTRAFFIC_ES:-$DEFAULT_CONNECT_URL}
  curl -XPOST $CONNECT_URL_JOURNEYTRAFFIC_ES/connectors -H "Content-Type: application/json" -d '
    {
      "name" : "journeytraffic_es_sink_connector",
      "config" :
        {
        "connector.class" : "com.evolving.nglm.evolution.JourneyTrafficESSinkConnector",
        "tasks.max" : 1,
        "topics" : "${changelog.evolutionengine.journeytraffic.topic}",
        "connectionHost" : "'$MASTER_ESROUTER_HOST'",
        "connectionPort" : "'$MASTER_ESROUTER_PORT'",
        "indexName" : "datacube_journeytraffic"
        }
    }' &
    

  #
  #  sink connector -- journeyMetric (elasticsearch)
  #

  export CONNECT_URL_JOURNEYMETRIC_ES=${CONNECT_URL_JOURNEYMETRIC_ES:-$DEFAULT_CONNECT_URL}
  curl -XPOST $CONNECT_URL_JOURNEYMETRIC_ES/connectors -H "Content-Type: application/json" -d '
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

  export CONNECT_URL_EXTERNALDELIVERYREQUEST=${CONNECT_URL_EXTERNALDELIVERYREQUEST:-$DEFAULT_CONNECT_URL}
  curl -XPOST $CONNECT_URL_EXTERNALDELIVERYREQUEST/connectors -H "Content-Type: application/json" -d '
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

  export CONNECT_URL_PRESENTATIONLOG=${CONNECT_URL_PRESENTATIONLOG:-$DEFAULT_CONNECT_URL}
  curl -XPOST $CONNECT_URL_PRESENTATIONLOG/connectors -H "Content-Type: application/json" -d '
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

  export CONNECT_URL_ACCEPTANCELOG=${CONNECT_URL_ACCEPTANCELOG:-$DEFAULT_CONNECT_URL}
  curl -XPOST $CONNECT_URL_ACCEPTANCELOG/connectors -H "Content-Type: application/json" -d '
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

  export CONNECT_URL_SUBSCRIBERPROFILE_FORCEUPDATE=${CONNECT_URL_SUBSCRIBERPROFILE_FORCEUPDATE:-$DEFAULT_CONNECT_URL}
  curl -XPOST $CONNECT_URL_SUBSCRIBERPROFILE_FORCEUPDATE/connectors -H "Content-Type: application/json" -d '
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
  #  source connector -- TokenRedeemedFileSourceConnector
  #

  export CONNECT_URL_TOKEN_REDEEMED=${CONNECT_URL_TOKEN_REDEEMED:-$DEFAULT_CONNECT_URL}
  curl -XPOST $CONNECT_URL_TOKEN_REDEEMED/connectors -H "Content-Type: application/json" -d '
    {
      "name" : "TokenRedeemedFileSourceConnector",
      "config" :
        {
        "connector.class" : "com.evolving.nglm.evolution.TokenRedeemedFileSourceConnector",
        "tasks.max" : 1,
        "directory" : "/app/data/tokenredeemed",
        "filenamePattern" : "^.*(\\.gz)?(?<!\\.tmp)$",
        "pollMaxRecords" : 5,
        "pollingInterval" : 10,
        "verifySizeInterval" : 0,
        "topic" : "${topic.tokenredeemed}",
        "bootstrapServers" : "'$BROKER_SERVERS'",
        "internalTopic" : "${topic.tokenredeemed_fileconnector}",
        "archiveDirectory" : "/app/data/tokenredeemedarchive"
        }
    }' &

  #
  #  source connector -- BonusDeliveryFileSourceConnector
  #

  export CONNECT_URL_BONUS_DELIVERY=${CONNECT_URL_BONUS_DELIVERY:-$DEFAULT_CONNECT_URL}
  curl -XPOST $CONNECT_URL_BONUS_DELIVERY/connectors -H "Content-Type: application/json" -d '
    {
      "name" : "BonusDeliveryFileSourceConnector",
      "config" :
        {
        "connector.class" : "com.evolving.nglm.evolution.BonusDeliveryFileSourceConnector",
        "tasks.max" : 1,
        "directory" : "/app/data/bonusdelivery",
        "filenamePattern" : "^.*(\\.gz)?(?<!\\.tmp)$",
        "pollMaxRecords" : 5,
        "pollingInterval" : 10,
        "verifySizeInterval" : 0,
        "topic" : "${topic.bonusdelivery}",
        "bootstrapServers" : "'$BROKER_SERVERS'",
        "internalTopic" : "${topic.bonusdelivery_fileconnector}",
        "archiveDirectory" : "/app/data/bonusdeliveryarchive"
        }
    }' &

  #
  #  source connector -- OfferDeliveryFileSourceConnector
  #

  export CONNECT_URL_OFFER_DELIVERY=${CONNECT_URL_OFFER_DELIVERY:-$DEFAULT_CONNECT_URL}
  curl -XPOST $CONNECT_URL_OFFER_DELIVERY/connectors -H "Content-Type: application/json" -d '
    {
      "name" : "OfferDeliveryFileSourceConnector",
      "config" :
        {
        "connector.class" : "com.evolving.nglm.evolution.OfferDeliveryFileSourceConnector",
        "tasks.max" : 1,
        "directory" : "/app/data/offerdelivery",
        "filenamePattern" : "^.*(\\.gz)?(?<!\\.tmp)$",
        "pollMaxRecords" : 5,
        "pollingInterval" : 10,
        "verifySizeInterval" : 0,
        "topic" : "${topic.offerdelivery}",
        "bootstrapServers" : "'$BROKER_SERVERS'",
        "internalTopic" : "${topic.offerdelivery_fileconnector}",
        "archiveDirectory" : "/app/data/offerdeliveryarchive"
        }
    }' &

  #
  #  source connector -- MessageDeliveryFileSourceConnector
  #

  export CONNECT_URL_MESSAGE_DELIVERY=${CONNECT_URL_MESSAGE_DELIVERY:-$DEFAULT_CONNECT_URL}
  curl -XPOST $CONNECT_URL_MESSAGE_DELIVERY/connectors -H "Content-Type: application/json" -d '
    {
      "name" : "MessageDeliveryFileSourceConnector",
      "config" :
        {
        "connector.class" : "com.evolving.nglm.evolution.MessageDeliveryFileSourceConnector",
        "tasks.max" : 1,
        "directory" : "/app/data/messagedelivery",
        "filenamePattern" : "^.*(\\.gz)?(?<!\\.tmp)$",
        "pollMaxRecords" : 5,
        "pollingInterval" : 10,
        "verifySizeInterval" : 0,
        "topic" : "${topic.messagedelivery}",
        "bootstrapServers" : "'$BROKER_SERVERS'",
        "internalTopic" : "${topic.messagedelivery_fileconnector}",
        "archiveDirectory" : "/app/data/messagedeliveryarchive"
        }
    }' &

  #
  #  sink connector -- propensity (elasticsearch)
  #

  export CONNECT_URL_PROPENSITY_ES=${CONNECT_URL_PROPENSITY_ES:-$DEFAULT_CONNECT_URL}
  curl -XPOST $CONNECT_URL_PROPENSITY_ES/connectors -H "Content-Type: application/json" -d '
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

  export CONNECT_URL_ODR_ES=${CONNECT_URL_ODR_ES:-$DEFAULT_CONNECT_URL}
  curl -XPOST $CONNECT_URL_ODR_ES/connectors -H "Content-Type: application/json" -d '
     {
       "name" : "odr_es_sink_connector",
       "config" :
         {
         "connector.class" : "com.evolving.nglm.evolution.ODRSinkConnector",
         "tasks.max" : 1,
         "topics" : "${topic.fulfillment.purchasefulfillment.response}",
         "connectionHost" : "'$MASTER_ESROUTER_HOST'",
         "connectionPort" : "'$MASTER_ESROUTER_PORT'",
         "indexName" : "detailedrecords_offers",
         "pipelineName" : "odr-daily"
         }
     }' &
   
  #
  #  sink connector -- BDR (elasticsearch)
  #

  export CONNECT_URL_BDR_ES=${CONNECT_URL_BDR_ES:-$DEFAULT_CONNECT_URL}
  curl -XPOST $CONNECT_URL_BDR_ES/connectors -H "Content-Type: application/json" -d '
     {
       "name" : "bdr_es_sink_connector",
       "config" :
         {
         "connector.class" : "com.evolving.nglm.evolution.BDRSinkConnector",
         "tasks.max" : 1,
         "topics" : "${topic.commoditydelivery.response}",
         "connectionHost" : "'$MASTER_ESROUTER_HOST'",
         "connectionPort" : "'$MASTER_ESROUTER_PORT'",
         "indexName" : "detailedrecords_bonuses",
         "pipelineName" : "bdr-daily"
         }
     }' &
  
  #
  #  sink connector -- Notification (elasticsearch)
  #

  export CONNECT_URL_NOTIFICATION_ES=${CONNECT_URL_NOTIFICATION_ES:-$DEFAULT_CONNECT_URL}
  curl -XPOST $CONNECT_URL_NOTIFICATION_ES/connectors -H "Content-Type: application/json" -d '
    {
      "name" : "notification_es_sink_connector",
      "config" :
        {
        "connector.class" : "com.evolving.nglm.evolution.NotificationSinkConnector",
        "tasks.max" : 1,
        "topics" : "${topic.notificationmanagerpush.response},"${topic.notificationmanagermail.response},${topic.notificationmanagersms.response}",
        "connectionHost" : "'$MASTER_ESROUTER_HOST'",
        "connectionPort" : "'$MASTER_ESROUTER_PORT'",
        "indexName" : "detailedrecords_messages",
        "pipelineName" : "mdr-daily"
        }
    }' &

  #
  #  source connector -- periodicEvaluation
  #

  if [ "${env.USE_REGRESSION}" = "1" ]
  then
  export CONNECT_URL_PERIODIC_EVALUATION=${CONNECT_URL_PERIODIC_EVALUATION:-$DEFAULT_CONNECT_URL}
  curl -XPOST $CONNECT_URL_PERIODIC_EVALUATION/connectors -H "Content-Type: application/json" -d '
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
