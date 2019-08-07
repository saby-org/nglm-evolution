  #################################################################################
  #
  #  evolution-setup-kafka
  #
  #################################################################################

  #
  #  setup topics script
  #

  cat > /app/setup/topics-core

  #
  #  create topics -- log
  #
  
  echo "Creating topics (evolution)"
  echo create_topic ${topic.empty}                                                    $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
  echo create_topic ${topic.journey}                                                  $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
  echo create_topic ${topic.journeytemplate}                                          $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
  echo create_topic ${topic.segmentationdimension}                                    $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
  echo create_topic ${topic.point}                                                    $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
  echo create_topic ${topic.offer}                                                    $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
  echo create_topic ${topic.report}                                                   $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
  echo create_topic ${topic.paymentMean}                                              $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
  echo create_topic ${topic.presentationstrategy}                                     $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
  echo create_topic ${topic.scoringstrategy}                                          $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
  echo create_topic ${topic.callingchannel}                                           $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
  echo create_topic ${topic.saleschannel}                                             $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
  echo create_topic ${topic.supplier}                                                 $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
  echo create_topic ${topic.product}                                                  $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
  echo create_topic ${topic.catalogcharacteristic}                                    $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
  echo create_topic ${topic.contactpolicy}                                            $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
  echo create_topic ${topic.journeyobjective}                                         $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
  echo create_topic ${topic.offerobjective}                                           $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
  echo create_topic ${topic.producttype}                                              $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
  echo create_topic ${topic.ucgrule}                                                  $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
  echo create_topic ${topic.deliverable}                                              $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
  echo create_topic ${topic.tokentype}                                                $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
  echo create_topic ${topic.template.subscribermessage}                               $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
  echo create_topic ${topic.guiaudit}                                                 $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_DATA_TWO_WEEKS"  >> /app/setup/topics-evolution
  echo create_topic ${topic.subscribergroup}                                          $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
  echo create_topic ${topic.subscribergroup.assignsubscriberid}                       $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
  echo create_topic ${topic.subscribergroupepoch}                                     $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
  echo create_topic ${topic.ucgstate}                                                 $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
  echo create_topic ${topic.timedevaluation}                                          $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
  echo create_topic ${topic.subscriberprofileforceupdate}                             $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
  echo create_topic ${topic.subscriberprofileforceupdate_fileconnector}               $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
  echo create_topic ${topic.journeyrequest}                                           $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
  echo create_topic ${topic.journeyresponse}                                          $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
  echo create_topic ${topic.journeystatistic}                                         $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
  echo create_topic ${topic.journeymetric}                                            $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
  echo create_topic ${topic.deliverable.source}                                       $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
  echo create_topic ${topic.presentationlog}                                          $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
  echo create_topic ${topic.acceptancelog}                                            $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
  echo create_topic ${topic.propensitylog}                                            $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
  echo create_topic ${topic.pointfulfillment.request}                                 $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
  echo create_topic ${topic.pointfulfillment.response}                                $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
  echo create_topic ${topic.commoditydelivery.request}                                $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
  echo create_topic ${topic.commoditydelivery.response}                               $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
  echo create_topic ${topic.commoditydelivery.internal}                               $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_CHANGELOG_50MB"  >> /app/setup/topics-evolution
  echo create_topic ${topic.commoditydelivery.routing}                                $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_HOURS"  >> /app/setup/topics-evolution
  echo create_topic ${topic.uploadedfile}                                             $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
  echo create_topic ${topic.target} 		                                          $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
  echo create_topic ${topic.communicationchannel} 		                              $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
  echo create_topic ${topic.blackoutperiod} 		                                  $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
  echo create_topic ${topic.loyaltyprogram} 		                                  $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
  echo create_topic ${topic.salespartner} 		                                      $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
  echo create_topic ${topic.exclusioninclusiontarget} 		                          $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
  echo create_topic ${topic.fulfillment.purchasefulfillment.request}                  $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
  echo create_topic ${topic.fulfillment.purchasefulfillment.response}                 $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
  echo create_topic ${topic.fulfillment.purchasefulfillment.internal}                 $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_CHANGELOG_50MB"  >> /app/setup/topics-evolution
  echo create_topic ${topic.fulfillment.purchasefulfillment.routing}                  $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_HOURS"  >> /app/setup/topics-evolution
  echo create_topic ${topic.notificationmanagersms.request}                           $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
  echo create_topic ${topic.notificationmanagersms.response}                          $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
  echo create_topic ${topic.notificationmanagersms.internal}                          $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_CHANGELOG_50MB"  >> /app/setup/topics-evolution
  echo create_topic ${topic.notificationmanagersms.routing}                           $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_HOURS"  >> /app/setup/topics-evolution
  echo create_topic ${topic.notificationmanagermail.request}                          $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
  echo create_topic ${topic.notificationmanagermail.response}                         $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
  echo create_topic ${topic.notificationmanagermail.internal}                         $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_CHANGELOG_50MB"  >> /app/setup/topics-evolution
  echo create_topic ${topic.notificationmanagermail.routing}                          $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_HOURS"  >> /app/setup/topics-evolution
  echo create_topic ${changelog.evolutionengine.subscriberstate.topic}                $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_CHANGELOG_50MB"  >> /app/setup/topics-evolution
  echo create_topic ${changelog.evolutionengine.extendedsubscriberprofile.topic}      $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_CHANGELOG_50MB"  >> /app/setup/topics-evolution
  echo create_topic ${changelog.evolutionengine.subscriberhistory.topic}              $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_CHANGELOG_50MB"  >> /app/setup/topics-evolution
  echo create_topic ${changelog.evolutionengine.propensitystate.topic}                $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_CHANGELOG_50MB"  >> /app/setup/topics-evolution
  echo create_topic ${topic.evolutionengine.pointfulfillment.repartition}             $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
  echo create_topic ${topic.evolutionengine.propensityoutput.repartition}             $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
  echo create_topic ${topic.externaldeliveryrequest_fileconnector}                    $KAFKA_REPLICATION_FACTOR               $FILECONNECTOR_PARTITIONS_LARGE         "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
  echo create_topic ${topic.presentationlog_fileconnector}                            $KAFKA_REPLICATION_FACTOR               $FILECONNECTOR_PARTITIONS_LARGE         "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
  echo create_topic ${topic.acceptancelog_fileconnector}                              $KAFKA_REPLICATION_FACTOR               $FILECONNECTOR_PARTITIONS_LARGE         "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
  
  #
  #  regression topics	
  #
  
  if [ "${env.USE_REGRESSION}" = "1" ]
  then
    echo create_topic ${topic.periodicevaluation_fileconnector}                         $KAFKA_REPLICATION_FACTOR               $FILECONNECTOR_PARTITIONS_LARGE         "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
  fi

  #
  #  create topics
  #

  create_multiple_topics /app/setup/topics-evolution

  #
  #  create topics -- finish
  #

  wait
  echo "Created topics (evolution)"

  #
  #  wait for schema registry
  #

  echo waiting for schema registry ...
  cub sr-ready $MASTER_REGISTRY_HOST $MASTER_REGISTRY_PORT $SETUP_CUB_REGISTRY_TIMEOUT
  echo registry $MASTER_REGISTRY_HOST $MASTER_REGISTRY_PORT ready

  #
  #  register schemas - evolution
  #
  
  echo "Registering schemas (evolution)"

  curl -X POST -H "Content-Type: application/json" -d '{"schema":"{\"type\":\"record\",\"name\":\"deliverablesource\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"display\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"active\",\"type\":\"boolean\"},{\"name\":\"effectiveStartDate\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"effectiveEndDate\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"fulfillmentProviderID\",\"type\":\"string\"},{\"name\":\"commodityID\",\"type\":\"string\"},{\"name\":\"unitaryCost\",\"type\":\"int\"}],\"connect.version\":130,\"connect.name\":\"deliverablesource\"}"}' $MASTER_REGISTRY_URL/subjects/${topic.deliverable.source}-value/versions; echo
  curl -X POST -H "Content-Type: application/json" -d '{"schema":"{\"type\":\"record\",\"name\":\"presentation_log\",\"fields\":[{\"name\":\"msisdn\",\"type\":\"string\"},{\"name\":\"subscriberID\",\"type\":\"string\"},{\"name\":\"eventDate\",\"type\":\"long\"},{\"name\":\"callUniqueIdentifier\",\"type\":\"string\"},{\"name\":\"channelID\",\"type\":\"string\"},{\"name\":\"salesChannelID\",\"type\":\"string\"},{\"name\":\"userID\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"presentationToken\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"presentationStrategyID\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"transactionDurationMs\",\"type\":\"int\"},{\"name\":\"offerIDs\",\"type\":{\"type\":\"array\",\"items\":\"string\"}},{\"name\":\"positions\",\"type\":{\"type\":\"array\",\"items\":\"int\"}},{\"name\":\"controlGroupState\",\"type\":\"string\"},{\"name\":\"scoringStrategyID\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"scoringGroup\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"scoringGroupID\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"algorithmID\",\"type\":[\"null\",\"string\"],\"default\":null}],\"connect.version\":1,\"connect.name\":\"presentation_log\"}"}' $MASTER_REGISTRY_URL/subjects/${topic.presentationlog}-value/versions; echo
  curl -X POST -H "Content-Type: application/json" -d '{"schema":"{\"type\":\"record\",\"name\":\"acceptance_log\",\"fields\":[{\"name\":\"msisdn\",\"type\":\"string\"},{\"name\":\"subscriberID\",\"type\":\"string\"},{\"name\":\"eventDate\",\"type\":\"long\"},{\"name\":\"callUniqueIdentifier\",\"type\":\"string\"},{\"name\":\"channelID\",\"type\":\"string\"},{\"name\":\"salesChannelID\",\"type\":\"string\"},{\"name\":\"userID\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"presentationToken\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"presentationStrategyID\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"transactionDurationMs\",\"type\":\"int\"},{\"name\":\"controlGroupState\",\"type\":\"string\"},{\"name\":\"offerID\",\"type\":\"string\"},{\"name\":\"fulfilledDate\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"position\",\"type\":[\"null\",\"int\"],\"default\":null}],\"connect.version\":1,\"connect.name\":\"acceptance_log\"}"}' $MASTER_REGISTRY_URL/subjects/${topic.acceptancelog}-value/versions; echo

  echo "Registered schemas (evolution)"

  
