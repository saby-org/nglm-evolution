  #################################################################################
  #
  #  deployment-setup-connect
  #
  #################################################################################

  #
  #  source connector -- externalAggregates
  #

  create_topic ${topic.externalaggregates_fileconnector} $KAFKA_REPLICATION_FACTOR $FILECONNECTOR_PARTITIONS_LARGE "$TOPIC_DATA_TWO_DAYS"
  curl -XPOST $CONNECT_URL/connectors -H "Content-Type: application/json" -d '
    {
      "name" : "externalaggregates_file_connector",
      "config" :
        {
        "connector.class" : "com.evolving.nglm.evolution.ExternalAggregatesFileSourceConnector",
        "tasks.max" : 1,
        "directory" : "/app/data/externalaggregates",
        "filenamePattern" : "^.*(\\.gz)?(?<!\\.tmp)$",
        "pollMaxRecords" : 5,
        "pollingInterval" : 10,
        "verifySizeInterval" : 0,
        "topic" : "externalaggregates:${topic.externalaggregates},externalaggregates-assignsubscriberid:${topic.externalaggregates.assignsubscriberid}",
        "bootstrapServers" : "'$BROKER_SERVERS'",
        "internalTopic" : "${topic.externalaggregates_fileconnector}",
        "archiveDirectory" : "/app/data/externalaggregatesarchive"
        }
    }'
  echo

  #
  #  sink connector -- subscriberProfile (redis)
  #

  curl -XPOST $CONNECT_URL/connectors -H "Content-Type: application/json" -d '
    {
      "name" : "subscriberprofile_redis_sink_connector",
      "config" :
        {
        "connector.class" : "com.evolving.nglm.evolution.SubscriberProfileRedisSinkConnector",
        "tasks.max" : 1,
        "topics" : "${changelog.evolutionengine.subscriberstate.topic}",
        "redisSentinels" : "'$REDIS_SENTINELS'",
        "redisInstance" : "subscriberprofile",
        "defaultDBIndex"   : "0",
        "pipelined" : "true"
        }
    }'
  echo

  #
  #  sink connector -- subscriberProfile (elasticsearch)
  #

  curl -XPOST $CONNECT_URL/connectors -H "Content-Type: application/json" -d '
    {
      "name" : "subscriberprofile_es_sink_connector",
      "config" :
        {
        "connector.class" : "com.evolving.nglm.evolution.SubscriberProfileESSinkConnector",
        "tasks.max" : 1,
        "topics" : "${changelog.evolutionengine.subscriberstate.topic}",
        "connectionHost" : "'$MASTER_ESROUTER_HOST'",
        "connectionPort" : "'$MASTER_ESROUTER_PORT'",
        "indexName" : "subscriberprofile"
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
  #  sink connector -- offer criterion
  #

  if [ "${env.USE_REGRESSION}" = "1" ]
  then
    curl -XPOST $CONNECT_URL/connectors -H "Content-Type: application/json" -d '
      {
        "name" : "regression_criteria_es_sink_connector",
        "config" :
          {
          "connector.class" : "com.evolving.nglm.evolution.RegressionCriteriaSinkConnector",
          "tasks.max" : 1,
          "topics" : "${topic.subscriberupdate}",
          "connectionHost" : "'$MASTER_ESROUTER_HOST'",
          "connectionPort" : "'$MASTER_ESROUTER_PORT'",
          "indexName" : "regr_criteria",
          "batchRecordCount" : "1"
          }
      }'
    echo
  fi
