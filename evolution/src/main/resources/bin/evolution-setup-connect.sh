  #################################################################################
  #
  #  evolution-setup-connect
  #
  #################################################################################

  #
  #  sink connector -- subscriberProfile (elasticsearch)
  #

  export CONNECT_URL_SUBSCRIBERPROFILE_ES=${CONNECT_URL_SUBSCRIBERPROFILE_ES:-$DEFAULT_CONNECT_URL}
  export CONNECT_ES_SUBSCRIBERPROFILE_SINK_TASKS=${CONNECT_ES_SUBSCRIBERPROFILE_SINK_TASKS:-'1'}
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
  export CONNECT_ES_EXTENDEDSUBSCRIBERPROFILE_SINK_TASKS=${CONNECT_ES_EXTENDEDSUBSCRIBERPROFILE_SINK_TASKS:-'1'}
  export CONNECT_ES_EXTENDEDSUBSCRIBERPROFILE_BATCHRECORDCOUNT=${CONNECT_ES_EXTENDEDSUBSCRIBERPROFILE_BATCHRECORDCOUNT:-'1'}
  export CONNECT_ES_EXTENDEDSUBSCRIBERPROFILE_BATCHSIZEMB=${CONNECT_ES_EXTENDEDSUBSCRIBERPROFILE_BATCHSIZEMB:-'5'}
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
  export CONNECT_ES_JOURNEYSTATISTIC_SINK_TASKS=${CONNECT_ES_JOURNEYSTATISTIC_SINK_TASKS:-'1'}
  export CONNECT_ES_JOURNEYSTATISTIC_BATCHRECORDCOUNT=${CONNECT_ES_JOURNEYSTATISTIC_BATCHRECORDCOUNT:-'1'}
  export CONNECT_ES_JOURNEYSTATISTIC_BATCHSIZEMB=${CONNECT_ES_JOURNEYSTATISTIC_BATCHSIZEMB:-'5'}
  curl -XPOST $CONNECT_URL_JOURNEYSTATISTIC_ES/connectors -H "Content-Type: application/json" -d '
    {
      "name" : "journeystatistic_es_sink_connector",
      "config" :
        {
        "connector.class" : "com.evolving.nglm.evolution.JourneyStatisticESSinkConnector",
        "tasks.max" : '$CONNECT_ES_JOURNEYSTATISTIC_SINK_TASKS',
        "topics" : "${topic.journeystatistic}",
        "connectionHost" : "'$MASTER_ESROUTER_HOST'",
        "connectionPort" : "'$MASTER_ESROUTER_PORT'",
        "indexName" : "journeystatistic",
	"batchRecordCount" : "'$CONNECT_ES_JOURNEYSTATISTIC_BATCHRECORDCOUNT'",
	"batchSize" : "'$CONNECT_ES_JOURNEYSTATISTIC_BATCHSIZEMB'"
        }
    }' &

  #
  #  sink connector -- journeyTraffic (elasticsearch)
  #

  export CONNECT_URL_JOURNEYTRAFFIC_ES=${CONNECT_URL_JOURNEYTRAFFIC_ES:-$DEFAULT_CONNECT_URL}
  export CONNECT_ES_JOURNEYTRAFFIC_SINK_TASKS=${CONNECT_ES_JOURNEYTRAFFIC_SINK_TASKS:-'1'}
  export CONNECT_ES_JOURNEYTRAFFIC_BATCHRECORDCOUNT=${CONNECT_ES_JOURNEYTRAFFIC_BATCHRECORDCOUNT:-'1'}
  export CONNECT_ES_JOURNEYTRAFFIC_BATCHSIZEMB=${CONNECT_ES_JOURNEYTRAFFIC_BATCHSIZEMB:-'5'}
  curl -XPOST $CONNECT_URL_JOURNEYTRAFFIC_ES/connectors -H "Content-Type: application/json" -d '
    {
      "name" : "journeytraffic_es_sink_connector",
      "config" :
        {
        "connector.class" : "com.evolving.nglm.evolution.JourneyTrafficESSinkConnector",
        "tasks.max" : '$CONNECT_ES_JOURNEYTRAFFIC_SINK_TASKS',
        "topics" : "${changelog.journeytrafficengine.journeytraffic.topic}",
        "connectionHost" : "'$MASTER_ESROUTER_HOST'",
        "connectionPort" : "'$MASTER_ESROUTER_PORT'",
        "indexName" : "datacube_journeytraffic",
	"batchRecordCount" : "'$CONNECT_ES_JOURNEYTRAFFIC_BATCHRECORDCOUNT'",
	"batchSize" : "'$CONNECT_ES_JOURNEYTRAFFIC_BATCHSIZEMB'"
        }
    }' &
    

  #
  #  sink connector -- journeyMetric (elasticsearch)
  #

  export CONNECT_URL_JOURNEYMETRIC_ES=${CONNECT_URL_JOURNEYMETRIC_ES:-$DEFAULT_CONNECT_URL}
  export CONNECT_ES_JOURNEYMETRIC_SINK_TASKS=${CONNECT_ES_JOURNEYMETRIC_SINK_TASKS:-'1'}
  export CONNECT_ES_JOURNEYMETRIC_BATCHRECORDCOUNT=${CONNECT_ES_JOURNEYMETRIC_BATCHRECORDCOUNT:-'1000'}
  export CONNECT_ES_JOURNEYMETRIC_BATCHSIZEMB=${CONNECT_ES_JOURNEYMETRIC_BATCHSIZEMB:-'5'}
  curl -XPOST $CONNECT_URL_JOURNEYMETRIC_ES/connectors -H "Content-Type: application/json" -d '
    {
      "name" : "journeymetric_es_sink_connector",
      "config" :
        {
        "connector.class" : "com.evolving.nglm.evolution.JourneyMetricESSinkConnector",
        "tasks.max" : '$CONNECT_ES_JOURNEYMETRIC_SINK_TASKS',
        "topics" : "${topic.journeymetric}",
        "connectionHost" : "'$MASTER_ESROUTER_HOST'",
        "connectionPort" : "'$MASTER_ESROUTER_PORT'",
        "batchRecordCount" : "'$ES_BATCH_RECORD_COUNT'",
        "indexName" : "journeymetric",
	"batchRecordCount" : "'$CONNECT_ES_JOURNEYMETRIC_BATCHRECORDCOUNT'",
	"batchSize" : "'$CONNECT_ES_JOURNEYMETRIC_BATCHSIZEMB'"
        }
    }' &

  #
  #  sink connector -- tokenchange (elasticsearch)
  #

  export CONNECT_URL_TOKENCHANGE_ES=${CONNECT_URL_TOKENCHANGE_ES:-$DEFAULT_CONNECT_URL}
  export CONNECT_ES_TOKENCHANGE_SINK_TASKS=${CONNECT_ES_TOKENCHANGE_SINK_TASKS:-'1'}
  export CONNECT_ES_TOKENCHANGE_BATCHRECORDCOUNT=${CONNECT_ES_TOKENCHANGE_BATCHRECORDCOUNT:-'1'}
  export CONNECT_ES_TOKENCHANGE_BATCHSIZEMB=${CONNECT_ES_TOKENCHANGE_BATCHSIZEMB:-'5'}
  curl -XPOST $CONNECT_URL_TOKENCHANGE_ES/connectors -H "Content-Type: application/json" -d '
    {
      "name" : "tokenchange_es_sink_connector",
      "config" :
        {
          "connector.class"  : "com.evolving.nglm.evolution.TokenChangeESSinkConnector",
          "tasks.max"        : '$CONNECT_ES_TOKENCHANGE_SINK_TASKS',
          "topics"           : "${topic.tokenchange}",
          "connectionHost"   : "'$MASTER_ESROUTER_HOST'",
          "connectionPort"   : "'$MASTER_ESROUTER_PORT'",
          "indexName"        : "token",
	      "batchRecordCount" : "'$CONNECT_ES_TOKENCHANGE_BATCHRECORDCOUNT'",
	      "batchSize"        : "'$CONNECT_ES_TOKENCHANGE_BATCHSIZEMB'"
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
  #  sink connector -- propensity (elasticsearch)
  #

  export CONNECT_URL_PROPENSITY_ES=${CONNECT_URL_PROPENSITY_ES:-$DEFAULT_CONNECT_URL}
  export CONNECT_ES_PROPENSITY_SINK_TASKS=${CONNECT_ES_PROPENSITY_SINK_TASKS:-'1'}
  export CONNECT_ES_PROPENSITY_BATCHRECORDCOUNT=${CONNECT_ES_PROPENSITY_BATCHRECORDCOUNT:-'1000'}
  export CONNECT_ES_PROPENSITY_BATCHSIZEMB=${CONNECT_ES_PROPENSITY_BATCHSIZEMB:-'5'}
  curl -XPOST $CONNECT_URL_PROPENSITY_ES/connectors -H "Content-Type: application/json" -d '
    {
      "name" : "propensity_es_sink_connector",
      "config" :
        {
        "connector.class" : "com.evolving.nglm.evolution.PropensityESSinkConnector",
        "tasks.max" : '$CONNECT_ES_PROPENSITY_SINK_TASKS',
        "topics" : "${topic.propensitylog}",
        "connectionHost" : "'$MASTER_ESROUTER_HOST'",
        "connectionPort" : "'$MASTER_ESROUTER_PORT'",
        "indexName" : "propensity",
	"batchRecordCount" : "'$CONNECT_ES_PROPENSITY_BATCHRECORDCOUNT'",
	"batchSize" : "'$CONNECT_ES_PROPENSITY_BATCHSIZEMB'"
        }
    }' &

  #
  #  sink connector -- ODR (elasticsearch)
  #

  export CONNECT_URL_ODR_ES=${CONNECT_URL_ODR_ES:-$DEFAULT_CONNECT_URL}
  export CONNECT_ES_ODR_SINK_TASKS=${CONNECT_ES_ODR_SINK_TASKS:-'1'}
  export CONNECT_ES_ODR_BATCHRECORDCOUNT=${CONNECT_ES_ODR_BATCHRECORDCOUNT:-'1000'}
  export CONNECT_ES_ODR_BATCHSIZEMB=${CONNECT_ES_ODR_BATCHSIZEMB:-'5'}
  curl -XPOST $CONNECT_URL_ODR_ES/connectors -H "Content-Type: application/json" -d '
     {
       "name" : "odr_es_sink_connector",
       "config" :
         {
         "connector.class" : "com.evolving.nglm.evolution.ODRSinkConnector",
         "tasks.max" : '$CONNECT_ES_ODR_SINK_TASKS',
         "topics" : "${topic.fulfillment.purchasefulfillment.response}",
         "connectionHost" : "'$MASTER_ESROUTER_HOST'",
         "connectionPort" : "'$MASTER_ESROUTER_PORT'",
         "indexName" : "detailedrecords_offers",
         "pipelineName" : "odr-daily",
         "batchRecordCount" : "'$CONNECT_ES_ODR_BATCHRECORDCOUNT'",
         "batchSize" : "'$CONNECT_ES_ODR_BATCHSIZEMB'"
         }
     }' &
   
  #
  #  sink connector -- BDR (elasticsearch)
  #

  export CONNECT_URL_BDR_ES=${CONNECT_URL_BDR_ES:-$DEFAULT_CONNECT_URL}
  export CONNECT_ES_BDR_SINK_TASKS=${CONNECT_ES_BDR_SINK_TASKS:-'1'}
  export CONNECT_ES_BDR_BATCHRECORDCOUNT=${CONNECT_ES_BDR_BATCHRECORDCOUNT:-'1000'}
  export CONNECT_ES_BDR_BATCHSIZEMB=${CONNECT_ES_BDR_BATCHSIZEMB:-'5'}
  curl -XPOST $CONNECT_URL_BDR_ES/connectors -H "Content-Type: application/json" -d '
     {
       "name" : "bdr_es_sink_connector",
       "config" :
         {
         "connector.class" : "com.evolving.nglm.evolution.BDRSinkConnector",
         "tasks.max" : '$CONNECT_ES_BDR_SINK_TASKS',
         "topics" : "${topic.commoditydelivery.response}",
         "connectionHost" : "'$MASTER_ESROUTER_HOST'",
         "connectionPort" : "'$MASTER_ESROUTER_PORT'",
         "indexName" : "detailedrecords_bonuses",
         "pipelineName" : "bdr-daily",
         "batchRecordCount" : "'$CONNECT_ES_BDR_BATCHRECORDCOUNT'",
         "batchSize" : "'$CONNECT_ES_BDR_BATCHSIZEMB'"
         }
     }' &
  
  #
  #  sink connector -- Notification (elasticsearch)
  #

  export CONNECT_URL_NOTIFICATION_ES=${CONNECT_URL_NOTIFICATION_ES:-$DEFAULT_CONNECT_URL}
  export CONNECT_ES_NOTIFICATION_SINK_TASKS=${CONNECT_ES_NOTIFICATION_SINK_TASKS:-'1'}
  export CONNECT_ES_NOTIFICATION_BATCHRECORDCOUNT=${CONNECT_ES_NOTIFICATION_BATCHRECORDCOUNT:-'1000'}
  export CONNECT_ES_NOTIFICATION_BATCHSIZEMB=${CONNECT_ES_NOTIFICATION_BATCHSIZEMB:-'5'}
  curl -XPOST $CONNECT_URL_NOTIFICATION_ES/connectors -H "Content-Type: application/json" -d '
    {
      "name" : "notification_es_sink_connector",
      "config" :
        {
        "connector.class" : "com.evolving.nglm.evolution.NotificationSinkConnector",
        "tasks.max" : '$CONNECT_ES_NOTIFICATION_SINK_TASKS',
        "topics" : "${topic.notificationmanagerpush.response},${topic.notificationmanagermail.response},${topic.notificationmanagersms.response}",
        "connectionHost" : "'$MASTER_ESROUTER_HOST'",
        "connectionPort" : "'$MASTER_ESROUTER_PORT'",
        "indexName" : "detailedrecords_messages",
        "pipelineName" : "mdr-daily",
         "batchRecordCount" : "'$CONNECT_ES_NOTIFICATION_BATCHRECORDCOUNT'",
         "batchSize" : "'$CONNECT_ES_NOTIFICATION_BATCHSIZEMB'"
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

  #
  #  sink connector -- Deliverable (elasticsearch)
  #

  export CONNECT_URL_DELIVERABLE_ES=${CONNECT_URL_DELIVERABLE_ES:-$DEFAULT_CONNECT_URL}
  export CONNECT_ES_DELIVERABLE_SINK_TASKS=${CONNECT_ES_DELIVERABLE_SINK_TASKS:-'1'}
  export CONNECT_ES_DELIVERABLE_BATCHRECORDCOUNT=${CONNECT_ES_DELIVERABLE_BATCHRECORDCOUNT:-'1000'}
  export CONNECT_ES_DELIVERABLE_BATCHSIZEMB=${CONNECT_ES_DELIVERABLE_BATCHSIZEMB:-'5'}
  curl -XPOST $CONNECT_URL_DELIVERABLE_ES/connectors -H "Content-Type: application/json" -d '
    {
      "name" : "deliverable_es_sink_connector",
      "config" :
        {
        "connector.class" : "com.evolving.nglm.evolution.DeliverableESSinkConnector",
        "tasks.max" : '$CONNECT_ES_DELIVERABLE_SINK_TASKS',
        "topics" : "${topic.deliverable}",
        "connectionHost" : "'$MASTER_ESROUTER_HOST'",
        "connectionPort" : "'$MASTER_ESROUTER_PORT'",
        "batchRecordCount" : "'$ES_BATCH_RECORD_COUNT'",
        "indexName" : "mapping_deliverables",
	    "batchRecordCount" : "'$CONNECT_ES_DELIVERABLE_BATCHRECORDCOUNT'",
	    "batchSize" : "'$CONNECT_ES_DELIVERABLE_BATCHSIZEMB'"
        }
    }' &

  #
  #  sink connector -- PaymentMean (elasticsearch)
  #

  export CONNECT_URL_PAYMENTMEAN_ES=${CONNECT_URL_PAYMENTMEAN_ES:-$DEFAULT_CONNECT_URL}
  export CONNECT_ES_PAYMENTMEAN_SINK_TASKS=${CONNECT_ES_PAYMENTMEAN_SINK_TASKS:-'1'}
  export CONNECT_ES_PAYMENTMEAN_BATCHRECORDCOUNT=${CONNECT_ES_PAYMENTMEAN_BATCHRECORDCOUNT:-'1000'}
  export CONNECT_ES_PAYMENTMEAN_BATCHSIZEMB=${CONNECT_ES_PAYMENTMEAN_BATCHSIZEMB:-'5'}
  curl -XPOST $CONNECT_URL_PAYMENTMEAN_ES/connectors -H "Content-Type: application/json" -d '
    {
      "name" : "paymentmean_es_sink_connector",
      "config" :
        {
        "connector.class" : "com.evolving.nglm.evolution.PaymentMeanESSinkConnector",
        "tasks.max" : '$CONNECT_ES_PAYMENTMEAN_SINK_TASKS',
        "topics" : "${topic.paymentMean}",
        "connectionHost" : "'$MASTER_ESROUTER_HOST'",
        "connectionPort" : "'$MASTER_ESROUTER_PORT'",
        "batchRecordCount" : "'$ES_BATCH_RECORD_COUNT'",
        "indexName" : "mapping_paymentmeans",
	    "batchRecordCount" : "'$CONNECT_ES_PAYMENTMEAN_BATCHRECORDCOUNT'",
	    "batchSize" : "'$CONNECT_ES_PAYMENTMEAN_BATCHSIZEMB'"
        }
    }' &

  #
  #  sink connector -- Offer (elasticsearch)
  #

  export CONNECT_URL_OFFER_ES=${CONNECT_URL_OFFER_ES:-$DEFAULT_CONNECT_URL}
  export CONNECT_ES_OFFER_SINK_TASKS=${CONNECT_ES_OFFER_SINK_TASKS:-'1'}
  export CONNECT_ES_OFFER_BATCHRECORDCOUNT=${CONNECT_ES_OFFER_BATCHRECORDCOUNT:-'1000'}
  export CONNECT_ES_OFFER_BATCHSIZEMB=${CONNECT_ES_OFFER_BATCHSIZEMB:-'5'}
  curl -XPOST $CONNECT_URL_OFFER_ES/connectors -H "Content-Type: application/json" -d '
    {
      "name" : "offer_es_sink_connector",
      "config" :
        {
        "connector.class" : "com.evolving.nglm.evolution.OfferESSinkConnector",
        "tasks.max" : '$CONNECT_ES_OFFER_SINK_TASKS',
        "topics" : "${topic.offer}",
        "connectionHost" : "'$MASTER_ESROUTER_HOST'",
        "connectionPort" : "'$MASTER_ESROUTER_PORT'",
        "batchRecordCount" : "'$ES_BATCH_RECORD_COUNT'",
        "indexName" : "mapping_offers",
	    "batchRecordCount" : "'$CONNECT_ES_OFFER_BATCHRECORDCOUNT'",
	    "batchSize" : "'$CONNECT_ES_OFFER_BATCHSIZEMB'"
        }
    }' &

  #
  #  sink connector -- Product (elasticsearch)
  #

  export CONNECT_URL_PRODUCT_ES=${CONNECT_URL_PRODUCT_ES:-$DEFAULT_CONNECT_URL}
  export CONNECT_ES_PRODUCT_SINK_TASKS=${CONNECT_ES_PRODUCT_SINK_TASKS:-'1'}
  export CONNECT_ES_PRODUCT_BATCHRECORDCOUNT=${CONNECT_ES_PRODUCT_BATCHRECORDCOUNT:-'1000'}
  export CONNECT_ES_PRODUCT_BATCHSIZEMB=${CONNECT_ES_PRODUCT_BATCHSIZEMB:-'5'}
  curl -XPOST $CONNECT_URL_PRODUCT_ES/connectors -H "Content-Type: application/json" -d '
    {
      "name" : "product_es_sink_connector",
      "config" :
        {
        "connector.class" : "com.evolving.nglm.evolution.ProductESSinkConnector",
        "tasks.max" : '$CONNECT_ES_PRODUCT_SINK_TASKS',
        "topics" : "${topic.product}",
        "connectionHost" : "'$MASTER_ESROUTER_HOST'",
        "connectionPort" : "'$MASTER_ESROUTER_PORT'",
        "batchRecordCount" : "'$ES_BATCH_RECORD_COUNT'",
        "indexName" : "mapping_products",
	    "batchRecordCount" : "'$CONNECT_ES_PRODUCT_BATCHRECORDCOUNT'",
	    "batchSize" : "'$CONNECT_ES_PRODUCT_BATCHSIZEMB'"
        }
    }' &

  #
  #  sink connector -- Loyalty Program (elasticsearch)
  #

  export CONNECT_URL_LOYALTYPROGRAM_ES=${CONNECT_URL_LOYALTYPROGRAM_ES:-$DEFAULT_CONNECT_URL}
  export CONNECT_ES_LOYALTYPROGRAM_SINK_TASKS=${CONNECT_ES_LOYALTYPROGRAM_SINK_TASKS:-'1'}
  export CONNECT_ES_LOYALTYPROGRAM_BATCHRECORDCOUNT=${CONNECT_ES_LOYALTYPROGRAM_BATCHRECORDCOUNT:-'1000'}
  export CONNECT_ES_LOYALTYPROGRAM_BATCHSIZEMB=${CONNECT_ES_LOYALTYPROGRAM_BATCHSIZEMB:-'5'}
  curl -XPOST $CONNECT_URL_LOYALTYPROGRAM_ES/connectors -H "Content-Type: application/json" -d '
    {
      "name" : "loyaltyprogram_es_sink_connector",
      "config" :
        {
        "connector.class" : "com.evolving.nglm.evolution.LoyaltyProgramPointsESSinkConnector",
        "tasks.max" : '$CONNECT_ES_LOYALTYPROGRAM_SINK_TASKS',
        "topics" : "${topic.loyaltyprogram}",
        "connectionHost" : "'$MASTER_ESROUTER_HOST'",
        "connectionPort" : "'$MASTER_ESROUTER_PORT'",
        "batchRecordCount" : "'$ES_BATCH_RECORD_COUNT'",
        "indexName" : "mapping_loyaltyprograms",
	    "batchRecordCount" : "'$CONNECT_ES_LOYALTYPROGRAM_BATCHRECORDCOUNT'",
        "batchSize" : "'$CONNECT_ES_LOYALTYPROGRAM_BATCHSIZEMB'"
        }
    }' &

  #
  #  sink connector -- Journey (elasticsearch)
  #

  export CONNECT_URL_JOURNEY_ES=${CONNECT_URL_JOURNEY_ES:-$DEFAULT_CONNECT_URL}
  export CONNECT_ES_JOURNEY_SINK_TASKS=${CONNECT_ES_JOURNEY_SINK_TASKS:-'1'}
  export CONNECT_ES_JOURNEY_BATCHRECORDCOUNT=${CONNECT_ES_JOURNEY_BATCHRECORDCOUNT:-'1000'}
  export CONNECT_ES_JOURNEY_BATCHSIZEMB=${CONNECT_ES_JOURNEY_BATCHSIZEMB:-'5'}
  curl -XPOST $CONNECT_URL_JOURNEY_ES/connectors -H "Content-Type: application/json" -d '
    {
      "name" : "journey_es_sink_connector",
      "config" :
        {
        "connector.class" : "com.evolving.nglm.evolution.JourneyESSinkConnector",
        "tasks.max" : '$CONNECT_ES_JOURNEY_SINK_TASKS',
        "topics" : "${topic.journey}",
        "connectionHost" : "'$MASTER_ESROUTER_HOST'",
        "connectionPort" : "'$MASTER_ESROUTER_PORT'",
        "batchRecordCount" : "'$ES_BATCH_RECORD_COUNT'",
        "indexName" : "mapping_journeys",
	    "batchRecordCount" : "'$CONNECT_ES_JOURNEY_BATCHRECORDCOUNT'",
	    "batchSize" : "'$CONNECT_ES_JOURNEY_BATCHSIZEMB'"
        }
    }' &

  #
  #  sink connector -- Sales Channel (elasticsearch)
  #

  export CONNECT_URL_SALESCHANNEL_ES=${CONNECT_URL_SALESCHANNEL_ES:-$DEFAULT_CONNECT_URL}
  export CONNECT_ES_SALESCHANNEL_SINK_TASKS=${CONNECT_ES_SALESCHANNEL_SINK_TASKS:-'1'}
  export CONNECT_ES_SALESCHANNEL_BATCHRECORDCOUNT=${CONNECT_ES_SALESCHANNEL_BATCHRECORDCOUNT:-'1000'}
  export CONNECT_ES_SALESCHANNEL_BATCHSIZEMB=${CONNECT_ES_SALESCHANNEL_BATCHSIZEMB:-'5'}
  curl -XPOST $CONNECT_URL_SALESCHANNEL_ES/connectors -H "Content-Type: application/json" -d '
    {
      "name" : "saleschannel_es_sink_connector",
      "config" :
        {
        "connector.class" : "com.evolving.nglm.evolution.SalesChannelESSinkConnector",
        "tasks.max" : '$CONNECT_ES_SALESCHANNEL_SINK_TASKS',
        "topics" : "${topic.saleschannel}",
        "connectionHost" : "'$MASTER_ESROUTER_HOST'",
        "connectionPort" : "'$MASTER_ESROUTER_PORT'",
        "batchRecordCount" : "'$ES_BATCH_RECORD_COUNT'",
        "indexName" : "mapping_saleschannels",
	    "batchRecordCount" : "'$CONNECT_ES_SALESCHANNEL_BATCHRECORDCOUNT'",
	    "batchSize" : "'$CONNECT_ES_SALESCHANNEL_BATCHSIZEMB'"
        }
    }' &


wait
