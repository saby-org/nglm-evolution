  #################################################################################
  #
  #  evolution-setup-kafka
  #
  #################################################################################

  create_topic ${topic.journey}                                                  $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"
  create_topic ${topic.segmentationrule}                                         $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"
  create_topic ${topic.offer}                                                    $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"
  create_topic ${topic.presentationstrategy}                                     $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"
  create_topic ${topic.scoringstrategy}                                          $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"
  create_topic ${topic.callingchannel}                                           $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"
  create_topic ${topic.supplier}                                                 $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"
  create_topic ${topic.product}                                                  $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"
  create_topic ${topic.catalogcharacteristic}                                    $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"
  create_topic ${topic.offerobjective}                                           $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"
  create_topic ${topic.producttype}                                              $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"
  create_topic ${topic.deliverable}                                              $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"
  create_topic ${topic.guiaudit}                                                 $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_DATA_TWO_WEEKS"
  create_topic ${topic.subscriberupdate}                                         $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"
  create_topic ${topic.subscribergroup}                                          $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"
  create_topic ${topic.subscribergroup.assignsubscriberid}                       $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"
  create_topic ${topic.subscribergroupepoch}                                     $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"
  create_topic ${topic.timedevaluation}                                          $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"
  create_topic ${topic.journeystatistic}                                         $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"
  create_topic ${topic.deliverable.source}                                       $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_DATA_TWO_DAYS"
  create_topic ${changelog.evolutionengine.subscriberstate.topic}                $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_CHANGELOG_50MB"
  create_topic ${changelog.evolutionengine.subscriberhistory.topic}              $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_CHANGELOG_50MB"

