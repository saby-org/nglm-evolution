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
        "topics" : "${changelog.profileengine.subscriberprofile.topic}",
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
        "topics" : "${changelog.profileengine.subscriberprofile.topic}",
        "connectionHost" : "'$MASTER_ESROUTER_HOST'",
        "connectionPort" : "'$MASTER_ESROUTER_PORT'",
        "indexName" : "subscriberprofile"
        }
    }'
  echo

  #
  #  sink connector -- subscriberUpdate (elasticsearch)
  #

  curl -XPOST $CONNECT_URL/connectors -H "Content-Type: application/json" -d '
    {
      "name" : "subscriberupdate_es_sink_connector",
      "config" :
        {
        "connector.class" : "com.evolving.nglm.evolution.SubscriberUpdateESSinkConnector",
        "tasks.max" : 1,
        "topics" : "${topic.subscriberupdate}",
        "connectionHost" : "'$MASTER_ESROUTER_HOST'",
        "connectionPort" : "'$MASTER_ESROUTER_PORT'",
        "indexName" : "subscriberupdate"
        }
    }'
  echo

