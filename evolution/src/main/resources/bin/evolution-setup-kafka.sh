#################################################################################
#
#  evolution-setup-kafka
#
#################################################################################

#
#  setup topics script
#

cat > /app/setup/topics-evolution


#
#  create topics -- log
#

echo "Creating topics (evolution)"
echo create_topic ${topic.journey}                                                  $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
echo create_topic ${topic.journeytemplate}                                          $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
echo create_topic ${topic.segmentationdimension}                                    $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
echo create_topic ${topic.point}                                                    $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
echo create_topic ${topic.complexobjecttype}                                        $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
echo create_topic ${topic.offer}                                                    $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
echo create_topic ${topic.report}                                                   $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
echo create_topic ${topic.paymentMean}                                              $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
echo create_topic ${topic.presentationstrategy}                                     $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
echo create_topic ${topic.dnbomatrix}                                               $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
echo create_topic ${topic.dynamiceventdeclarations}                                 $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
echo create_topic ${topic.dynamiccriterionfield}                                    $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
echo create_topic ${topic.scoringstrategy}                                          $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
echo create_topic ${topic.callingchannel}                                           $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
echo create_topic ${topic.saleschannel}                                             $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
echo create_topic ${topic.supplier}                                                 $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
echo create_topic ${topic.reseller}                                                 $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
echo create_topic ${topic.product}                                                  $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
echo create_topic ${topic.catalogcharacteristic}                                    $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
echo create_topic ${topic.contactpolicy}                                            $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
echo create_topic ${topic.journeyobjective}                                         $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
echo create_topic ${topic.offerobjective}                                           $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
echo create_topic ${topic.producttype}                                              $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
echo create_topic ${topic.ucgrule}                                                  $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
echo create_topic ${topic.deliverable}                                              $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
echo create_topic ${topic.tokentype}                                                $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
echo create_topic ${topic.vouchertype}                                              $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
echo create_topic ${topic.voucher}                                                  $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
echo create_topic ${topic.template.subscribermessage}                               $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
echo create_topic ${topic.guiaudit}                                                 $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_DATA_TWO_WEEKS"  >> /app/setup/topics-evolution
echo create_topic ${topic.subscribergroup}                                          $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
echo create_topic ${topic.subscribergroup.assignsubscriberid}                       $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
echo create_topic ${topic.subscribergroupepoch}                                     $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
echo create_topic ${topic.ucgstate}                                                 $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
echo create_topic ${topic.renamedprofilecriterionfield}                             $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
echo create_topic ${topic.timedevaluation}                                          $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
echo create_topic ${topic.evaluatetargets}                                          $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
echo create_topic ${topic.subscriberprofileforceupdate}                             $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
echo create_topic ${topic.subscriberprofileforceupdate_fileconnector}               $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
echo create_topic ${topic.executeactionothersubscriber}                             $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
echo create_topic ${topic.voucheraction}                                            $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
echo create_topic ${topic.filewithvariableevent}                                    $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
echo create_topic ${topic.tokenredeemed}                                            $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
echo create_topic ${topic.tokenredeemed_fileconnector}                              $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
echo create_topic ${topic.journeystatistic}                                         $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
echo create_topic ${topic.journeymetric}                                            $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
echo create_topic ${topic.presentationlog}                                          $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
echo create_topic ${topic.acceptancelog}                                            $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
echo create_topic ${topic.tokenchange}                                              $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
echo create_topic ${topic.profilechangeevent}                                       $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
echo create_topic ${topic.profilesegmentchangeevent}                                $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
echo create_topic ${topic.profileloyaltyprogramchangeevent}                         $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
echo create_topic ${topic.uploadedfile}                                             $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
echo create_topic ${topic.target}                                                   $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
echo create_topic ${topic.communicationchannel}                                     $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
echo create_topic ${topic.blackoutperiod}                                           $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
echo create_topic ${topic.timewindowperiod}                                         $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
echo create_topic ${topic.loyaltyprogram}                                           $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
echo create_topic ${topic.exclusioninclusiontarget}                                 $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
echo create_topic ${topic.segmentcontactpolicy}                                     $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
echo create_topic ${topic.criterionfieldavailablevalues}                            $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
echo create_topic ${topic.sourceaddress}                                            $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution
echo create_topic ${changelog.evolutionengine.subscriberstate.topic}                $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_CHANGELOG_50MB"  >> /app/setup/topics-evolution
echo create_topic ${changelog.evolutionengine.extendedsubscriberprofile.topic}      $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_CHANGELOG_50MB"  >> /app/setup/topics-evolution
echo create_topic ${topic.externaldeliveryrequest_fileconnector}                    $KAFKA_REPLICATION_FACTOR               $FILECONNECTOR_PARTITIONS_LARGE         "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
echo create_topic ${topic.presentationlog_fileconnector}                            $KAFKA_REPLICATION_FACTOR               $FILECONNECTOR_PARTITIONS_LARGE         "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
echo create_topic ${topic.acceptancelog_fileconnector}                              $KAFKA_REPLICATION_FACTOR               $FILECONNECTOR_PARTITIONS_LARGE         "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
echo create_topic ${topic.voucherchange.request}                                    $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
echo create_topic ${topic.voucherchange.response}                                   $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
echo create_topic ${topic.edrdetails}                                   			$KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
echo create_topic ${topic.workflowEvent}                                            $KAFKA_REPLICATION_FACTOR               $SUBSCRIBER_PARTITIONS                  "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
echo create_topic ${topic.otptype}                                                  $KAFKA_REPLICATION_FACTOR               1                                       "$TOPIC_CONFIGURATION"   >> /app/setup/topics-evolution

#
#  regression topics  
#

if [ "${env.USE_REGRESSION}" = "1" ]
then
  echo create_topic ${topic.periodicevaluation_fileconnector}                         $KAFKA_REPLICATION_FACTOR               $FILECONNECTOR_PARTITIONS_LARGE         "$TOPIC_DATA_TWO_DAYS"   >> /app/setup/topics-evolution
fi

  #
  #  wait for schema registry
  #

  /app/bin/ev-cub sr-ready $REGISTRY_SERVERS $CUB_REGISTRY_MIN_NODES $SETUP_CUB_REGISTRY_TIMEOUT


