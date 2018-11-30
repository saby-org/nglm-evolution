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
  