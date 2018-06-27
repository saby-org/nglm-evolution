  #################################################################################
  #
  #  deployment-setup-kafka
  #
  #################################################################################

  create_topic ${topic.journey}                                                  $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"
  create_topic ${topic.offer}                                                    $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"
  create_topic ${topic.presentationstrategy}                                     $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"
  create_topic ${topic.scoringstrategy}                                          $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"
  create_topic ${topic.criteriastory}                                            $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"
  create_topic ${topic.subscriberupdate}                                         $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"
  create_topic ${topic.externalaggregates}                                       $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"
  create_topic ${topic.externalaggregates.assignsubscriberid}                    $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"
  create_topic ${topic.presentationlog_json}                                     $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"
  create_topic ${topic.presentationlog}                                          $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"
  create_topic ${topic.presentationdetailslog_json}                              $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"
  create_topic ${topic.presentationdetailslog}                                   $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"
  create_topic ${topic.acceptancelog_json}                                       $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"
  create_topic ${topic.acceptancelog}                                            $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"
  create_topic ${topic.interactionlog_json}                                      $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"
  create_topic ${topic.interactionlog}                                           $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"
  create_topic ${topic.subscribergroup}                                          $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"
  create_topic ${topic.subscribergroup.assignsubscriberid}                       $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"
  create_topic ${topic.subscribergroupepoch}                                     $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"
  create_topic ${changelog.profileengine.subscriberprofile.topic}                $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_CHANGELOG_50MB"
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
  