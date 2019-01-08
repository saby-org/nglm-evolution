  #################################################################################
  #
  #  evolution-setup-kafka
  #
  #################################################################################

  #
  #  create topics -- evolution
  #
  
  echo "Creating topics (evolution)"
  create_topic ${topic.empty}                                                    $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   
  create_topic ${topic.journey}                                                  $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   
  create_topic ${topic.segmentationrule}                                         $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   
  create_topic ${topic.offer}                                                    $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   
  create_topic ${topic.report}                                                   $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   
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
  create_topic ${topic.presentationlog}                                          $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS" 
  create_topic ${topic.acceptancelog}                                            $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS" 
  create_topic ${topic.propensitylog}                                            $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS" 
  create_topic ${topic.fulfillment.purchasefulfillment.request}                  $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"
  create_topic ${topic.fulfillment.purchasefulfillment.response}                 $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"
  create_topic ${topic.fulfillment.purchasefulfillment.internal}                 $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_CHANGELOG_50MB"
  create_topic ${topic.fulfillment.purchasefulfillment.routing}                  $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_HOURS"
  create_topic ${topic.notificationmanagersms.request}                           $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"
  create_topic ${topic.notificationmanagersms.response}                          $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"
  create_topic ${topic.notificationmanagersms.internal}                          $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_CHANGELOG_50MB"
  create_topic ${topic.notificationmanagersms.routing}                           $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_HOURS"
  create_topic ${topic.notificationmanagermail.request}                          $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"
  create_topic ${topic.notificationmanagermail.response}                         $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"
  create_topic ${topic.notificationmanagermail.internal}                         $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_CHANGELOG_50MB"
  create_topic ${topic.notificationmanagermail.routing}                          $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_HOURS"
  create_topic ${changelog.evolutionengine.subscriberstate.topic}                $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_CHANGELOG_50MB"   
  create_topic ${changelog.evolutionengine.subscriberhistory.topic}              $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_CHANGELOG_50MB"   
  create_topic ${changelog.propensityengine.propensitystate.topic}               $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_CHANGELOG_50MB" 
  wait
  echo "Created topics (evolution)"

  #
  #  register schemas - evolution
  #
  
  echo "Registering schemas (evolution)"
  curl -X POST -H "Content-Type: application/json" -d '{"schema":"{\"type\":\"record\",\"name\":\"deliverablesource\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"display\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"valid\",\"type\":\"boolean\"},{\"name\":\"active\",\"type\":\"boolean\"},{\"name\":\"effectiveStartDate\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"effectiveEndDate\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"fulfillmentProviderID\",\"type\":\"string\"},{\"name\":\"unitaryCost\",\"type\":\"int\"}],\"connect.version\":1,\"connect.name\":\"deliverablesource\"}"}' $MASTER_REGISTRY_URL/subjects/${topic.deliverable.source}-value/versions; echo
  curl -X POST -H "Content-Type: application/json" -d '{"schema":"{\"type\":\"record\",\"name\":\"presentation_log\",\"fields\":[{\"name\":\"subscriberID\",\"type\":\"string\"},{\"name\":\"offerIDs\",\"type\":{\"type\":\"array\",\"items\":\"string\"}},{\"name\":\"positions\",\"type\":{\"type\":\"array\",\"items\":\"int\"}},{\"name\":\"presentationStrategyID\",\"type\":\"string\"},{\"name\":\"channelID\",\"type\":\"string\"},{\"name\":\"salesChannelID\",\"type\":\"string\"},{\"name\":\"userID\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"eventDate\",\"type\":{\"type\":\"long\",\"connect.version\":1,\"connect.name\":\"org.apache.kafka.connect.data.Timestamp\",\"logicalType\":\"timestamp-millis\"}},{\"name\":\"presentationToken\",\"type\":[\"null\",\"string\"],\"default\":null}],\"connect.version\":1,\"connect.name\":\"presentation_log\"}"}' $MASTER_REGISTRY_URL/subjects/${topic.presentationlog}-value/versions; echo
  curl -X POST -H "Content-Type: application/json" -d '{"schema":"{\"type\":\"record\",\"name\":\"acceptance_log\",\"fields\":[{\"name\":\"subscriberID\",\"type\":\"string\"},{\"name\":\"msisdn\",\"type\":\"string\"},{\"name\":\"offerID\",\"type\":\"string\"},{\"name\":\"presentationStrategyID\",\"type\":\"string\"},{\"name\":\"presentationToken\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"channelID\",\"type\":\"string\"},{\"name\":\"salesChannelID\",\"type\":\"string\"},{\"name\":\"userID\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"eventDate\",\"type\":{\"type\":\"long\",\"connect.version\":1,\"connect.name\":\"org.apache.kafka.connect.data.Timestamp\",\"logicalType\":\"timestamp-millis\"}},{\"name\":\"fulfilledDate\",\"type\":[\"null\",{\"type\":\"long\",\"connect.version\":1,\"connect.name\":\"org.apache.kafka.connect.data.Timestamp\",\"logicalType\":\"timestamp-millis\"}],\"default\":null},{\"name\":\"position\",\"type\":[\"null\",\"int\"],\"default\":null}],\"connect.version\":1,\"connect.name\":\"acceptance_log\"}"}' $MASTER_REGISTRY_URL/subjects/${topic.acceptancelog}-value/versions; echo
  echo "Registered schemas (evolution)"

  