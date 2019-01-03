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
    }'
  echo

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
    }'
  echo

  #
  #  source connector -- externalDeliveryRequest
  #

  create_topic ${topic.externaldeliveryrequest_fileconnector} $KAFKA_REPLICATION_FACTOR $FILECONNECTOR_PARTITIONS_LARGE "$TOPIC_DATA_TWO_DAYS"
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
      }'
    echo

  #
  #  source connector -- presentationLog
  #

  create_topic ${topic.presentationlog_fileconnector} $KAFKA_REPLICATION_FACTOR $FILECONNECTOR_PARTITIONS_LARGE "$TOPIC_DATA_TWO_DAYS"
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
    }'
  echo

  #
  #  source connector -- acceptanceLog
  #

  create_topic ${topic.acceptancelog_fileconnector} $KAFKA_REPLICATION_FACTOR $FILECONNECTOR_PARTITIONS_LARGE "$TOPIC_DATA_TWO_DAYS"
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
    }'
  echo

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
    }'
  echo

    