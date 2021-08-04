#################################################################################
#
#  evolution-setup-connect
#
#################################################################################

#
#  sink connector -- subscriberProfile (elasticsearch)
#

export CONNECT_URL_SUBSCRIBERPROFILE_ES=${CONNECT_URL_SUBSCRIBERPROFILE_ES:-$DEFAULT_CONNECT_URL}
export CONNECT_ES_SUBSCRIBERPROFILE_SINK_TASKS=${CONNECT_ES_SUBSCRIBERPROFILE_SINK_TASKS:-$CONNECT_ES_DEFAULT_SINK_TASKS}
export CONNECT_ES_SUBSCRIBERPROFILE_BATCHRECORDCOUNT=${CONNECT_ES_SUBSCRIBERPROFILE_BATCHRECORDCOUNT:-$CONNECT_ES_DEFAULT_BATCHRECORDCOUNT}
export CONNECT_ES_SUBSCRIBERPROFILE_BATCHSIZEMB=${CONNECT_ES_SUBSCRIBERPROFILE_BATCHSIZEMB:-$CONNECT_ES_DEFAULT_BATCHSIZEMB}
prepare-curl -XPOST $CONNECT_URL_SUBSCRIBERPROFILE_ES/connectors -H "Content-Type: application/json" -d '
  {
    "name" : "subscriberprofile_es_sink_connector",
    "config" :
      {
      "connector.class" : "${connector.sink.elasticsearch.subscriberprofile}",
      "tasks.max" : '$CONNECT_ES_SUBSCRIBERPROFILE_SINK_TASKS',
      "topics" : "${changelog.evolutionengine.subscriberstate.topic}",
      "indexName" : "subscriberprofile",
      "batchRecordCount" : "'$CONNECT_ES_SUBSCRIBERPROFILE_BATCHRECORDCOUNT'",
      "batchSize" : "'$CONNECT_ES_SUBSCRIBERPROFILE_BATCHSIZEMB'"
      }
  }' 

#
#  sink connector -- extendedSubscriberProfile (elasticsearch)
#

export CONNECT_URL_EXTENDEDSUBSCRIBERPROFILE_ES=${CONNECT_URL_EXTENDEDSUBSCRIBERPROFILE_ES:-$DEFAULT_CONNECT_URL}
export CONNECT_ES_EXTENDEDSUBSCRIBERPROFILE_SINK_TASKS=${CONNECT_ES_EXTENDEDSUBSCRIBERPROFILE_SINK_TASKS:-$CONNECT_ES_DEFAULT_SINK_TASKS}
export CONNECT_ES_EXTENDEDSUBSCRIBERPROFILE_BATCHRECORDCOUNT=${CONNECT_ES_EXTENDEDSUBSCRIBERPROFILE_BATCHRECORDCOUNT:-$CONNECT_ES_DEFAULT_BATCHRECORDCOUNT}
export CONNECT_ES_EXTENDEDSUBSCRIBERPROFILE_BATCHSIZEMB=${CONNECT_ES_EXTENDEDSUBSCRIBERPROFILE_BATCHSIZEMB:-$CONNECT_ES_DEFAULT_BATCHSIZEMB}
prepare-curl -XPOST $CONNECT_URL_EXTENDEDSUBSCRIBERPROFILE_ES/connectors -H "Content-Type: application/json" -d '
  {
    "name" : "extendedsubscriberprofile_es_sink_connector",
    "config" :
      {
      "connector.class" : "${connector.sink.elasticsearch.extendedsubscriberprofile}",
      "tasks.max" : '$CONNECT_ES_EXTENDEDSUBSCRIBERPROFILE_SINK_TASKS',
      "topics" : "${changelog.evolutionengine.extendedsubscriberprofile.topic}",
      "indexName" : "subscriberprofile",
      "batchRecordCount" : "'$CONNECT_ES_EXTENDEDSUBSCRIBERPROFILE_BATCHRECORDCOUNT'",
      "batchSize" : "'$CONNECT_ES_EXTENDEDSUBSCRIBERPROFILE_BATCHSIZEMB'"
      }
  }' 
  
#
#  sink connector -- journeyStatistic (elasticsearch)
#

export CONNECT_URL_JOURNEYSTATISTIC_ES=${CONNECT_URL_JOURNEYSTATISTIC_ES:-$DEFAULT_CONNECT_URL}
export CONNECT_ES_JOURNEYSTATISTIC_SINK_TASKS=${CONNECT_ES_JOURNEYSTATISTIC_SINK_TASKS:-$CONNECT_ES_DEFAULT_SINK_TASKS}
export CONNECT_ES_JOURNEYSTATISTIC_BATCHRECORDCOUNT=${CONNECT_ES_JOURNEYSTATISTIC_BATCHRECORDCOUNT:-$CONNECT_ES_DEFAULT_BATCHRECORDCOUNT}
export CONNECT_ES_JOURNEYSTATISTIC_BATCHSIZEMB=${CONNECT_ES_JOURNEYSTATISTIC_BATCHSIZEMB:-$CONNECT_ES_DEFAULT_BATCHSIZEMB}
prepare-curl -XPOST $CONNECT_URL_JOURNEYSTATISTIC_ES/connectors -H "Content-Type: application/json" -d '
  {
    "name" : "journeystatistic_es_sink_connector",
    "config" :
      {
      "connector.class" : "com.evolving.nglm.evolution.JourneyStatisticESSinkConnector",
      "tasks.max" : '$CONNECT_ES_JOURNEYSTATISTIC_SINK_TASKS',
      "topics" : "${topic.journeystatistic}",
      "indexName" : "journeystatistic",
      "batchRecordCount" : "'$CONNECT_ES_JOURNEYSTATISTIC_BATCHRECORDCOUNT'",
      "batchSize" : "'$CONNECT_ES_JOURNEYSTATISTIC_BATCHSIZEMB'"
      }
  }' 
  
#
#  sink connector -- journeyMetric (elasticsearch)
#

export CONNECT_URL_JOURNEYMETRIC_ES=${CONNECT_URL_JOURNEYMETRIC_ES:-$DEFAULT_CONNECT_URL}
export CONNECT_ES_JOURNEYMETRIC_SINK_TASKS=${CONNECT_ES_JOURNEYMETRIC_SINK_TASKS:-$CONNECT_ES_DEFAULT_SINK_TASKS}
export CONNECT_ES_JOURNEYMETRIC_BATCHRECORDCOUNT=${CONNECT_ES_JOURNEYMETRIC_BATCHRECORDCOUNT:-$CONNECT_ES_DEFAULT_BATCHRECORDCOUNT}
export CONNECT_ES_JOURNEYMETRIC_BATCHSIZEMB=${CONNECT_ES_JOURNEYMETRIC_BATCHSIZEMB:-$CONNECT_ES_DEFAULT_BATCHSIZEMB}
prepare-curl -XPOST $CONNECT_URL_JOURNEYMETRIC_ES/connectors -H "Content-Type: application/json" -d '
  {
    "name" : "journeymetric_es_sink_connector",
    "config" :
      {
      "connector.class" : "com.evolving.nglm.evolution.JourneyMetricESSinkConnector",
      "tasks.max" : '$CONNECT_ES_JOURNEYMETRIC_SINK_TASKS',
      "topics" : "${topic.journeymetric}",
      "indexName" : "journeystatistic",
      "batchRecordCount" : "'$CONNECT_ES_JOURNEYMETRIC_BATCHRECORDCOUNT'",
      "batchSize" : "'$CONNECT_ES_JOURNEYMETRIC_BATCHSIZEMB'"
      }
  }' 

#
#  source connector -- presentationLog
#

export CONNECT_URL_PRESENTATIONLOG=${CONNECT_URL_PRESENTATIONLOG:-$DEFAULT_CONNECT_URL}
prepare-curl -XPOST $CONNECT_URL_PRESENTATIONLOG/connectors -H "Content-Type: application/json" -d '
  {
    "name" : "presentationlog_file_connector",
    "config" :
      {
      "connector.class" : "com.evolving.nglm.evolution.PresentationLogFileSourceConnector",
      "tasks.max" : 1,
      "directory" : "/app/data/presentationlog",
      "filenamePattern" : "^.*(\\.gz)?(?<!\\.tmp)$",
      "pollMaxRecords" : 5,
      "pollingInterval" : 2,
      "verifySizeInterval" : 0,
      "topic" : "${topic.presentationlog}",
      "bootstrapServers" : "'$BROKER_SERVERS'",
      "internalTopic" : "${topic.presentationlog_fileconnector}",
      "archiveDirectory" : "/app/data/presentationlogarchive"
      }
  }' 

#
#  source connector -- acceptanceLog
#

export CONNECT_URL_ACCEPTANCELOG=${CONNECT_URL_ACCEPTANCELOG:-$DEFAULT_CONNECT_URL}
prepare-curl -XPOST $CONNECT_URL_ACCEPTANCELOG/connectors -H "Content-Type: application/json" -d '
  {
    "name" : "acceptancelog_file_connector",
    "config" :
      {
      "connector.class" : "com.evolving.nglm.evolution.AcceptanceLogFileSourceConnector",
      "tasks.max" : 1,
      "directory" : "/app/data/acceptancelog",
      "filenamePattern" : "^.*(\\.gz)?(?<!\\.tmp)$",
      "pollMaxRecords" : 5,
      "pollingInterval" : 2,
      "verifySizeInterval" : 0,
      "topic" : "${topic.acceptancelog}",
      "bootstrapServers" : "'$BROKER_SERVERS'",
      "internalTopic" : "${topic.acceptancelog_fileconnector}",
      "archiveDirectory" : "/app/data/acceptancelogarchive"
      }
  }' 

#
#  source connector -- SubscriberProfileForceUpdateFileSourceConnector
#

export CONNECT_URL_SUBSCRIBERPROFILE_FORCEUPDATE=${CONNECT_URL_SUBSCRIBERPROFILE_FORCEUPDATE:-$DEFAULT_CONNECT_URL}
prepare-curl -XPOST $CONNECT_URL_SUBSCRIBERPROFILE_FORCEUPDATE/connectors -H "Content-Type: application/json" -d '
  {
    "name" : "SubscriberProfileForceUpdateFileSourceConnector",
    "config" :
      {
      "connector.class" : "com.evolving.nglm.evolution.SubscriberProfileForceUpdateFileSourceConnector",
      "tasks.max" : 1,
      "directory" : "/app/data/subscriberprofileforceupdate",
      "filenamePattern" : "^.*(\\.gz)?(?<!\\.tmp)$",
      "pollMaxRecords" : 5,
      "pollingInterval" : 2,
      "verifySizeInterval" : 0,
      "topic" : "${topic.subscriberprofileforceupdate}",
      "bootstrapServers" : "'$BROKER_SERVERS'",
      "internalTopic" : "${topic.subscriberprofileforceupdate_fileconnector}",
      "archiveDirectory" : "/app/data/subscriberprofileforceupdatearchive"
      }
  }' 

#
#  source connector -- TokenRedeemedFileSourceConnector
#

export CONNECT_URL_TOKEN_REDEEMED=${CONNECT_URL_TOKEN_REDEEMED:-$DEFAULT_CONNECT_URL}
prepare-curl -XPOST $CONNECT_URL_TOKEN_REDEEMED/connectors -H "Content-Type: application/json" -d '
  {
    "name" : "TokenRedeemedFileSourceConnector",
    "config" :
      {
      "connector.class" : "com.evolving.nglm.evolution.TokenRedeemedFileSourceConnector",
      "tasks.max" : 1,
      "directory" : "/app/data/tokenredeemed",
      "filenamePattern" : "^.*(\\.gz)?(?<!\\.tmp)$",
      "pollMaxRecords" : 5,
      "pollingInterval" : 2,
      "verifySizeInterval" : 0,
      "topic" : "${topic.tokenredeemed}",
      "bootstrapServers" : "'$BROKER_SERVERS'",
      "internalTopic" : "${topic.tokenredeemed_fileconnector}",
      "archiveDirectory" : "/app/data/tokenredeemedarchive"
      }
  }' 

#
#  sink connector -- ODR (elasticsearch)
#

export CONNECT_URL_ODR_ES=${CONNECT_URL_ODR_ES:-$DEFAULT_CONNECT_URL}
export CONNECT_ES_ODR_SINK_TASKS=${CONNECT_ES_ODR_SINK_TASKS:-$CONNECT_ES_DEFAULT_SINK_TASKS}
export CONNECT_ES_ODR_BATCHRECORDCOUNT=${CONNECT_ES_ODR_BATCHRECORDCOUNT:-$CONNECT_ES_DEFAULT_BATCHRECORDCOUNT}
export CONNECT_ES_ODR_BATCHSIZEMB=${CONNECT_ES_ODR_BATCHSIZEMB:-$CONNECT_ES_DEFAULT_BATCHSIZEMB}
prepare-curl -XPOST $CONNECT_URL_ODR_ES/connectors -H "Content-Type: application/json" -d '
   {
     "name" : "odr_es_sink_connector",
     "config" :
       {
       "connector.class" : "com.evolving.nglm.evolution.ODRSinkConnector",
       "tasks.max" : '$CONNECT_ES_ODR_SINK_TASKS',
       "connectionHost" : "'$MASTER_ESROUTER_HOST'",
       "connectionPort" : "'$MASTER_ESROUTER_PORT'",
       "connectionUserName" : "'$ELASTICSEARCH_USERNAME'",
       "connectionUserPassword" : "'$ELASTICSEARCH_USERPASSWORD'",
       "indexName" : "detailedrecords_offers-",
       "batchRecordCount" : "'$CONNECT_ES_ODR_BATCHRECORDCOUNT'",
       "batchSize" : "'$CONNECT_ES_ODR_BATCHSIZEMB'"
       }
   }' 
     
#
#  sink connector -- tokenchange (elasticsearch)
#

export CONNECT_URL_TOKENCHANGE_ES=${CONNECT_URL_TOKENCHANGE_ES:-$DEFAULT_CONNECT_URL}
export CONNECT_ES_TOKENCHANGE_SINK_TASKS=${CONNECT_ES_TOKENCHANGE_SINK_TASKS:-$CONNECT_ES_DEFAULT_SINK_TASKS}
export CONNECT_ES_TOKENCHANGE_BATCHRECORDCOUNT=${CONNECT_ES_TOKENCHANGE_BATCHRECORDCOUNT:-$CONNECT_ES_DEFAULT_BATCHRECORDCOUNT}
export CONNECT_ES_TOKENCHANGE_BATCHSIZEMB=${CONNECT_ES_TOKENCHANGE_BATCHSIZEMB:-$CONNECT_ES_DEFAULT_BATCHSIZEMB}
prepare-curl -XPOST $CONNECT_URL_TOKENCHANGE_ES/connectors -H "Content-Type: application/json" -d '
  {
    "name" : "tokenchange_es_sink_connector",
    "config" :
      {
        "connector.class"  : "com.evolving.nglm.evolution.TokenChangeESSinkConnector",
        "tasks.max"        : '$CONNECT_ES_TOKENCHANGE_SINK_TASKS',
        "topics"           : "${topic.tokenchange}",
        "connectionHost"   : "'$MASTER_ESROUTER_HOST'",
        "connectionPort"   : "'$MASTER_ESROUTER_PORT'",
        "connectionUserName" : "'$ELASTICSEARCH_USERNAME'",
        "connectionUserPassword" : "'$ELASTICSEARCH_USERPASSWORD'",
        "indexName"        : "detailedrecords_tokens-",
        "batchRecordCount" : "'$CONNECT_ES_TOKENCHANGE_BATCHRECORDCOUNT'",
        "batchSize"        : "'$CONNECT_ES_TOKENCHANGE_BATCHSIZEMB'"
      }
  }'
#
#  sink connector -- BDR (elasticsearch)
#

export CONNECT_URL_BDR_ES=${CONNECT_URL_BDR_ES:-$DEFAULT_CONNECT_URL}
export CONNECT_ES_BDR_SINK_TASKS=${CONNECT_ES_BDR_SINK_TASKS:-$CONNECT_ES_DEFAULT_SINK_TASKS}
export CONNECT_ES_BDR_BATCHRECORDCOUNT=${CONNECT_ES_BDR_BATCHRECORDCOUNT:-$CONNECT_ES_DEFAULT_BATCHRECORDCOUNT}
export CONNECT_ES_BDR_BATCHSIZEMB=${CONNECT_ES_BDR_BATCHSIZEMB:-$CONNECT_ES_DEFAULT_BATCHSIZEMB}
prepare-curl -XPOST $CONNECT_URL_BDR_ES/connectors -H "Content-Type: application/json" -d '
   {
     "name" : "bdr_es_sink_connector",
     "config" :
       {
       "connector.class" : "com.evolving.nglm.evolution.BDRSinkConnector",
       "tasks.max" : '$CONNECT_ES_BDR_SINK_TASKS',
       "connectionHost" : "'$MASTER_ESROUTER_HOST'",
       "connectionPort" : "'$MASTER_ESROUTER_PORT'",
       "connectionUserName" : "'$ELASTICSEARCH_USERNAME'",
       "connectionUserPassword" : "'$ELASTICSEARCH_USERPASSWORD'",
       "indexName" : "detailedrecords_bonuses-",
       "batchRecordCount" : "'$CONNECT_ES_BDR_BATCHRECORDCOUNT'",
       "batchSize" : "'$CONNECT_ES_BDR_BATCHSIZEMB'"
       }
   }' 

#
#  sink connector -- Notification (elasticsearch)
#

export CONNECT_URL_NOTIFICATION_ES=${CONNECT_URL_NOTIFICATION_ES:-$DEFAULT_CONNECT_URL}
export CONNECT_ES_NOTIFICATION_SINK_TASKS=${CONNECT_ES_NOTIFICATION_SINK_TASKS:-$CONNECT_ES_DEFAULT_SINK_TASKS}
export CONNECT_ES_NOTIFICATION_BATCHRECORDCOUNT=${CONNECT_ES_NOTIFICATION_BATCHRECORDCOUNT:-$CONNECT_ES_DEFAULT_BATCHRECORDCOUNT}
export CONNECT_ES_NOTIFICATION_BATCHSIZEMB=${CONNECT_ES_NOTIFICATION_BATCHSIZEMB:-$CONNECT_ES_DEFAULT_BATCHSIZEMB}
prepare-curl -XPOST $CONNECT_URL_NOTIFICATION_ES/connectors -H "Content-Type: application/json" -d '
  {
    "name" : "notification_es_sink_connector",
    "config" :
      {
      "connector.class" : "com.evolving.nglm.evolution.NotificationSinkConnector",
      "tasks.max" : '$CONNECT_ES_NOTIFICATION_SINK_TASKS',
      "connectionHost" : "'$MASTER_ESROUTER_HOST'",
      "connectionPort" : "'$MASTER_ESROUTER_PORT'",
      "connectionUserName" : "'$ELASTICSEARCH_USERNAME'",
      "connectionUserPassword" : "'$ELASTICSEARCH_USERPASSWORD'",
      "indexName" : "detailedrecords_messages-",
      "batchRecordCount" : "'$CONNECT_ES_NOTIFICATION_BATCHRECORDCOUNT'",
      "batchSize" : "'$CONNECT_ES_NOTIFICATION_BATCHSIZEMB'"
      }
  }' 

#
#  source connector -- periodicEvaluation
#

if [ "${env.USE_REGRESSION}" = "1" ]
then
export CONNECT_URL_PERIODIC_EVALUATION=${CONNECT_URL_PERIODIC_EVALUATION:-$DEFAULT_CONNECT_URL}
prepare-curl -XPOST $CONNECT_URL_PERIODIC_EVALUATION/connectors -H "Content-Type: application/json" -d '
  {
    "name" : "periodicevaluation_file_connector",
    "config" :
      {
      "connector.class" : "com.evolving.nglm.evolution.PeriodicEvaluationFileSourceConnector",
      "tasks.max" : 1,
      "directory" : "/app/data/periodicevaluation",
      "filenamePattern" : "^.*(\\.gz)?(?<!\\.tmp)$",
      "pollMaxRecords" : 5,
      "pollingInterval" : 2,
      "verifySizeInterval" : 0,
      "topic" : "${topic.timedevaluation}",
      "bootstrapServers" : "'$BROKER_SERVERS'",
      "internalTopic" : "${topic.periodicevaluation_fileconnector}",
      "archiveDirectory" : "/app/data/periodicevaluationarchive"
      }
  }' 
fi

#
#  sink connector -- Deliverable (elasticsearch)
#

export CONNECT_URL_DELIVERABLE_ES=${CONNECT_URL_DELIVERABLE_ES:-$DEFAULT_CONNECT_URL}
export CONNECT_ES_DELIVERABLE_SINK_TASKS=${CONNECT_ES_DELIVERABLE_SINK_TASKS:-$CONNECT_ES_DEFAULT_SINK_TASKS}
export CONNECT_ES_DELIVERABLE_BATCHRECORDCOUNT=${CONNECT_ES_DELIVERABLE_BATCHRECORDCOUNT:-$CONNECT_ES_DEFAULT_BATCHRECORDCOUNT}
export CONNECT_ES_DELIVERABLE_BATCHSIZEMB=${CONNECT_ES_DELIVERABLE_BATCHSIZEMB:-$CONNECT_ES_DEFAULT_BATCHSIZEMB}
prepare-curl -XPOST $CONNECT_URL_DELIVERABLE_ES/connectors -H "Content-Type: application/json" -d '
  {
    "name" : "deliverable_es_sink_connector",
    "config" :
      {
      "connector.class" : "com.evolving.nglm.evolution.DeliverableESSinkConnector",
      "tasks.max" : '$CONNECT_ES_DELIVERABLE_SINK_TASKS',
      "topics" : "${topic.deliverable}",
      "indexName" : "mapping_deliverables",
      "batchRecordCount" : "'$CONNECT_ES_DELIVERABLE_BATCHRECORDCOUNT'",
      "batchSize" : "'$CONNECT_ES_DELIVERABLE_BATCHSIZEMB'"
      }
  }' 
  
#
#  sink connector -- Partners (elasticsearch)
#

export CONNECT_URL_PARTNER_ES=${CONNECT_URL_PARTNER_ES:-$DEFAULT_CONNECT_URL}
export CONNECT_ES_PARTNER_SINK_TASKS=${CONNECT_ES_PARTNER_SINK_TASKS:-$CONNECT_ES_DEFAULT_SINK_TASKS}
export CONNECT_ES_PARTNER_BATCHRECORDCOUNT=${CONNECT_ES_PARTNER_BATCHRECORDCOUNT:-$CONNECT_ES_DEFAULT_BATCHRECORDCOUNT}
export CONNECT_ES_PARTNER_BATCHSIZEMB=${CONNECT_ES_PARTNER_BATCHSIZEMB:-$CONNECT_ES_DEFAULT_BATCHSIZEMB}
prepare-curl -XPOST $CONNECT_URL_PARTNER_ES/connectors -H "Content-Type: application/json" -d '
  {
    "name" : "partners_es_sink_connector",
    "config" :
      {
      "connector.class" : "com.evolving.nglm.evolution.PartnersESSinkConnector",
      "tasks.max" : '$CONNECT_ES_PARTNER_SINK_TASKS',
      "topics" : "${topic.supplier},${topic.reseller}",
      "indexName" : "mapping_partners",
      "batchRecordCount" : "'$CONNECT_ES_PARTNER_BATCHRECORDCOUNT'",
      "batchSize" : "'$CONNECT_ES_PARTNER_BATCHSIZEMB'"
      }
  }'

#
#  sink connector -- VDR (elasticsearch)
#

export CONNECT_URL_VDR_ES=${CONNECT_URL_VDR_ES:-$DEFAULT_CONNECT_URL}
export CONNECT_ES_VDR_SINK_TASKS=${CONNECT_ES_VDR_SINK_TASKS:-$CONNECT_ES_DEFAULT_SINK_TASKS}
export CONNECT_ES_VDR_BATCHRECORDCOUNT=${CONNECT_ES_VDR_BATCHRECORDCOUNT:-$CONNECT_ES_DEFAULT_BATCHRECORDCOUNT}
export CONNECT_ES_VDR_BATCHSIZEMB=${CONNECT_ES_VDR_BATCHSIZEMB:-$CONNECT_ES_DEFAULT_BATCHSIZEMB}
prepare-curl -XPOST $CONNECT_URL_VDR_ES/connectors -H "Content-Type: application/json" -d '
   {
     "name" : "vdr_es_sink_connector",
     "config" :
       {
       "connector.class" : "com.evolving.nglm.evolution.VDRSinkConnector",
       "tasks.max" : '$CONNECT_ES_VDR_SINK_TASKS',
       "topics" : "${topic.voucherchange.response}",
       "connectionHost" : "'$MASTER_ESROUTER_HOST'",
       "connectionPort" : "'$MASTER_ESROUTER_PORT'",
       "connectionUserName" : "'$ELASTICSEARCH_USERNAME'",
       "connectionUserPassword" : "'$ELASTICSEARCH_USERPASSWORD'",
       "indexName" : "detailedrecords_vouchers-",
       "batchRecordCount" : "'$CONNECT_ES_VDR_BATCHRECORDCOUNT'",
       "batchSize" : "'$CONNECT_ES_VDR_BATCHSIZEMB'"
       }
   }'

#
#  sink connector -- BGDR (elasticsearch)
#

export CONNECT_URL_BGDR_ES=${CONNECT_URL_BGDR_ES:-$DEFAULT_CONNECT_URL}
export CONNECT_ES_BGDR_SINK_TASKS=${CONNECT_ES_BGDR_SINK_TASKS:-$CONNECT_ES_DEFAULT_SINK_TASKS}
export CONNECT_ES_BGDR_BATCHRECORDCOUNT=${CONNECT_ES_BGDR_BATCHRECORDCOUNT:-$CONNECT_ES_DEFAULT_BATCHRECORDCOUNT}
export CONNECT_ES_BGDR_BATCHSIZEMB=${CONNECT_ES_BGDR_BATCHSIZEMB:-$CONNECT_ES_DEFAULT_BATCHSIZEMB}
prepare-curl -XPOST $CONNECT_URL_BGDR_ES/connectors -H "Content-Type: application/json" -d '
   {
     "name" : "bgdr_es_sink_connector",
     "config" :
       {
       "connector.class"		: "com.evolving.nglm.evolution.BGDRSinkConnector",
       "tasks.max" 				: '$CONNECT_ES_BGDR_SINK_TASKS',
       "topics" 				: "${topic.badgechange.response}",
       "connectionHost" 		: "'$MASTER_ESROUTER_HOST'",
       "connectionPort" 		: "'$MASTER_ESROUTER_PORT'",
       "connectionUserName" 	: "'$ELASTICSEARCH_USERNAME'",
       "connectionUserPassword"	: "'$ELASTICSEARCH_USERPASSWORD'",
       "indexName" 				: "detailedrecords_badges-",
       "batchRecordCount" 		: "'$CONNECT_ES_BGDR_BATCHRECORDCOUNT'",
       "batchSize" 				: "'$CONNECT_ES_BGDR_BATCHSIZEMB'"
       }
   }'   

#
#  sink connector -- EDR (elasticsearch)
#

export CONNECT_URL_EDR_ES=${CONNECT_URL_EDR_ES:-$DEFAULT_CONNECT_URL}
export CONNECT_ES_EDR_SINK_TASKS=${CONNECT_ES_EDR_SINK_TASKS:-$CONNECT_ES_DEFAULT_SINK_TASKS}
export CONNECT_ES_EDR_BATCHRECORDCOUNT=${CONNECT_ES_EDR_BATCHRECORDCOUNT:-$CONNECT_ES_DEFAULT_BATCHRECORDCOUNT}
export CONNECT_ES_EDR_BATCHSIZEMB=${CONNECT_ES_EDR_BATCHSIZEMB:-$CONNECT_ES_DEFAULT_BATCHSIZEMB}
prepare-curl -XPOST $CONNECT_URL_EDR_ES/connectors -H "Content-Type: application/json" -d '
   {
     "name" : "edr_es_sink_connector",
     "config" :
       {
       "connector.class" : "com.evolving.nglm.evolution.EDRSinkConnector",
       "tasks.max" : '$CONNECT_ES_EDR_SINK_TASKS',
       "topics" : "${topic.edrdetails}",
       "connectionHost" : "'$MASTER_ESROUTER_HOST'",
       "connectionPort" : "'$MASTER_ESROUTER_PORT'",
       "connectionUserName" : "'$ELASTICSEARCH_USERNAME'",
       "connectionUserPassword" : "'$ELASTICSEARCH_USERPASSWORD'",
       "indexName" : "detailedrecords_events-",
       "batchRecordCount" : "'$CONNECT_ES_EDR_BATCHRECORDCOUNT'",
       "batchSize" : "'$CONNECT_ES_EDR_BATCHSIZEMB'"
       }
   }'

wait
