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
  #  source connector -- presentationDetailsLog
  #

  create_topic ${topic.presentationdetailslog_fileconnector} $KAFKA_REPLICATION_FACTOR $FILECONNECTOR_PARTITIONS_LARGE "$TOPIC_DATA_TWO_DAYS"
  curl -XPOST $CONNECT_URL/connectors -H "Content-Type: application/json" -d '
    {
      "name" : "presentationdetailslog_file_connector",
      "config" :
        {
        "connector.class" : "com.evolving.nglm.evolution.PresentationDetailsLogFileSourceConnector",
        "tasks.max" : 1,
        "directory" : "/app/data/presentationdetailslog",
        "filenamePattern" : "^.*(\\.gz)?(?<!\\.tmp)$",
        "pollMaxRecords" : 5,
        "pollingInterval" : 10,
        "verifySizeInterval" : 0,
        "topic" : "${topic.presentationdetailslog}",
        "bootstrapServers" : "'$BROKER_SERVERS'",
        "internalTopic" : "${topic.presentationdetailslog_fileconnector}",
        "archiveDirectory" : "/app/data/presentationdetailslogarchive"
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
  #  source connector -- interactionLog
  #

  create_topic ${topic.interactionlog_fileconnector} $KAFKA_REPLICATION_FACTOR $FILECONNECTOR_PARTITIONS_LARGE "$TOPIC_DATA_TWO_DAYS"
  curl -XPOST $CONNECT_URL/connectors -H "Content-Type: application/json" -d '
    {
      "name" : "interactionlog_file_connector",
      "config" :
        {
        "connector.class" : "com.evolving.nglm.evolution.InteractionLogFileSourceConnector",
        "tasks.max" : 1,
        "directory" : "/app/data/interactionlog",
        "filenamePattern" : "^.*(\\.gz)?(?<!\\.tmp)$",
        "pollMaxRecords" : 5,
        "pollingInterval" : 10,
        "verifySizeInterval" : 0,
        "topic" : "${topic.interactionlog}",
        "bootstrapServers" : "'$BROKER_SERVERS'",
        "internalTopic" : "${topic.interactionlog_fileconnector}",
        "archiveDirectory" : "/app/data/interactionlogarchive"
        }
    }'
  echo

  #
  #  sink connector -- presentationLog
  #

  curl -XPOST $CONNECT_URL/connectors -H "Content-Type: application/json" -d '
    {
      "name" : "presentationlog_sink_connector",
      "config" :
        {
        "connector.class" : "com.evolving.nglm.evolution.PresentationLogSinkConnector",
        "tasks.max" : 1,
        "topics" : "${topic.presentationlog}",
        "jdbcDriver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "databaseConnectionURL" : "jdbc:sqlserver://'$IOM_DB_HOST':1433;user='$IOM_DB_USER';password='$IOM_DB_PASSWORD';databaseName='$IOM_DB_DATABASE'",
        "useStoredProcedure" : "true",
        "insertSQL" : "{call dbo.stp_save_presented_offers(?, ?, ?, ?, ?, ?)}",
        "batchSize" : 1000
        }
    }'
  echo

  #
  #  sink connector -- presentationDetailsLog
  #

  curl -XPOST $CONNECT_URL/connectors -H "Content-Type: application/json" -d '
    {
      "name" : "presentationdetailslog_sink_connector",
      "config" :
        {
        "connector.class" : "com.evolving.nglm.evolution.PresentationDetailsLogSinkConnector",
        "tasks.max" : 1,
        "topics" : "${topic.presentationdetailslog}",
        "jdbcDriver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "databaseConnectionURL" : "jdbc:sqlserver://'$IOM_DB_HOST':1433;user='$IOM_DB_USER';password='$IOM_DB_PASSWORD';databaseName='$IOM_DB_DATABASE'",
        "useStoredProcedure" : "true",
        "insertSQL" : "{call dbo.stp_save_presented_offer_details(?, ?, ?, ?, ?, ?)}",
        "batchSize" : 1000
        }
    }'
  echo

  #
  #  sink connector -- acceptanceLog
  #

  curl -XPOST $CONNECT_URL/connectors -H "Content-Type: application/json" -d '
    {
      "name" : "acceptancelog_sink_connector",
      "config" :
        {
        "connector.class" : "com.evolving.nglm.evolution.AcceptanceLogSinkConnector",
        "tasks.max" : 1,
        "topics" : "${topic.acceptancelog}",
        "jdbcDriver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "databaseConnectionURL" : "jdbc:sqlserver://'$IOM_DB_HOST':1433;user='$IOM_DB_USER';password='$IOM_DB_PASSWORD';databaseName='$IOM_DB_DATABASE'",
        "useStoredProcedure" : "true",
        "insertSQL" : "{call dbo.stp_save_accepted_offers(?, ?, ?, ?, ?, ?, ?, ?)}",
        "batchSize" : 1000
        }
    }'
  echo

  #
  #  sink connector -- interactionLog
  #

  curl -XPOST $CONNECT_URL/connectors -H "Content-Type: application/json" -d '
    {
      "name" : "interactionlog_sink_connector",
      "config" :
        {
        "connector.class" : "com.evolving.nglm.evolution.InteractionLogSinkConnector",
        "tasks.max" : 1,
        "topics" : "${topic.interactionlog}",
        "jdbcDriver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "databaseConnectionURL" : "jdbc:sqlserver://'$IOM_DB_HOST':1433;user='$IOM_DB_USER';password='$IOM_DB_PASSWORD';databaseName='$IOM_DB_DATABASE'",
        "useStoredProcedure" : "true",
        "insertSQL" : "{call dbo.stp_save_interactions(?, ?, ?, ?, ?)}",
        "batchSize" : 1000
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

