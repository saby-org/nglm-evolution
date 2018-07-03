  #################################################################################
  #
  #  deployment-setup-kafka
  #
  #################################################################################

  create_topic ${topic.journey}                                                  $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"
  create_topic ${topic.offer}                                                    $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"
  create_topic ${topic.subscriberupdate}                                         $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"
  create_topic ${topic.externalaggregates}                                       $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"
  create_topic ${topic.externalaggregates.assignsubscriberid}                    $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"
  create_topic ${topic.subscribergroup}                                          $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"
  create_topic ${topic.subscribergroup.assignsubscriberid}                       $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"
  create_topic ${topic.subscribergroupepoch}                                     $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"
  create_topic ${changelog.evolutionengine.subscriberstate.topic}                $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_CHANGELOG_50MB"
  create_topic ${changelog.subscribermanager.msisdn.subscriberid.topic}          $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_CHANGELOG_50MB"
  create_topic ${changelog.subscribermanager.msisdn.alternateid.topic}           $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_CHANGELOG_50MB"
  create_topic ${rekeyed.subscribermanager.msisdn.topic}                         $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"
  create_topic ${backchannel.subscribermanager.msisdn.subscriberid.topic}        $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"
  create_topic ${backchannel.subscribermanager.msisdn.alternateid.topic}         $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"
  create_topic ${changelog.subscribermanager.contractid.subscriberid.topic}      $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_CHANGELOG_50MB"
  create_topic ${changelog.subscribermanager.contractid.alternateid.topic}       $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_CHANGELOG_50MB"
  create_topic ${rekeyed.subscribermanager.contractid.topic}                     $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"
  create_topic ${backchannel.subscribermanager.contractid.subscriberid.topic}    $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"
  create_topic ${backchannel.subscribermanager.contractid.alternateid.topic}     $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"
  create_topic ${topic.journeystatistic}                                         $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"
  create_topic ${topic.fulfillment.infulfillment.request}                        $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"
  create_topic ${topic.fulfillment.infulfillment.response}                       $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"
  create_topic ${topic.fulfillment.infulfillment.internal}                       $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"
