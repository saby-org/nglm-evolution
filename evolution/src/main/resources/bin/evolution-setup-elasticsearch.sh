  #################################################################################
  #
  #  evolution-setup-elasticsearch
  #
  #################################################################################

  #
  #  manually create subscriberprofile index
  #   - these settings are for index heavy load
  #

  curl -XPUT http://$MASTER_ESROUTER_SERVER/subscriberprofile -H'Content-Type: application/json' -d'
    {
      "settings" :
        {
          "index" :
            {
              "number_of_shards" : "'$ELASTICSEARCH_SHARDS_SMALL'",
              "number_of_replicas" : "'$ELASTICSEARCH_REPLICAS'",
              "refresh_interval" : "30s",
              "translog" : 
                { 
                  "durability" : "async", 
                  "sync_interval" : "10s" 
                },
              "routing" : 
                {
                  "allocation" : { "total_shards_per_node" : '$ELASTICSEARCH_SHARDS_SMALL' }
                },
              "merge" : 
                {
                  "scheduler" : { "max_thread_count" : 4, "max_merge_count" : 100 }
                }
            }
        },
      "mappings" :
        {
              "properties" :
                {
                  "subscriberID" : { "type" : "keyword" },
                  "evaluationDate" : { "type" : "date" },
                  "evolutionSubscriberStatus" : { "type" : "keyword" },
                  "previousEvolutionSubscriberStatus" : { "type" : "keyword" },
                  "evolutionSubscriberStatusChangeDate" : { "type" : "date" },
                  "universalControlGroup" : { "type" : "boolean" },
                  "language" : { "type" : "keyword" },
                  "segments" : { "type" : "keyword" }
                }
        }
    }'
  echo

  #
  #  manually create propensity index
  #   - these settings are for index heavy load
  #

  curl -XPUT http://$MASTER_ESROUTER_SERVER/propensity -H'Content-Type: application/json' -d'
    {
      "settings" :
        {
          "index" :
            {
              "number_of_shards" : "'$ELASTICSEARCH_SHARDS_SMALL'",
              "number_of_replicas" : "'$ELASTICSEARCH_REPLICAS'",
              "refresh_interval" : "30s",
              "translog" :
                {
                  "durability" : "async",
                  "sync_interval" : "10s"
                },
              "routing" :
                {
                  "allocation" : { "total_shards_per_node" : '$ELASTICSEARCH_SHARDS_SMALL' }
                },
              "merge" :
                {
                  "scheduler" : { "max_thread_count" : 4, "max_merge_count" : 100 }
                }
            }
        },
      "mappings" :
        {
              "properties" :
                {
                  "offerID" : { "type" : "keyword" },
                  "segment" : { "type" : "keyword" },
                  "propensity" : { "type" : "double" },
                  "evaluationDate" : { "type" : "date" }
                }
        }
    }'
  echo

  #
  #  manually create bdr index
  #   - these settings are for index heavy load
  #

  curl -XPUT http://$MASTER_ESROUTER_SERVER/bdr -H'Content-Type: application/json' -d'
    {
      "settings" :
        {
          "index" :
            {
              "number_of_shards" : "'$ELASTICSEARCH_SHARDS_SMALL'",
              "number_of_replicas" : "'$ELASTICSEARCH_REPLICAS'",
              "refresh_interval" : "30s",
              "translog" : 
                { 
                  "durability" : "async", 
                  "sync_interval" : "10s" 
                },
              "routing" : 
                {
                  "allocation" : { "total_shards_per_node" : '$ELASTICSEARCH_SHARDS_SMALL' }
                },
              "merge" : 
                {
                  "scheduler" : { "max_thread_count" : 4, "max_merge_count" : 100 }
                }
            }
        },
      "mappings" :
        {
              "properties" :
                {
	          "subscriberID" : { "type" : "keyword" },
	          "eventID" : { "type" : "keyword" },
	          "eventDatetime" : { "type" : "date" },
	          "providerID" : { "type" : "keyword" },
	          "deliverableID" : { "type" : "keyword" },
	          "deliverableQty" : { "type" : "integer", "index" : "false" },
	          "operation" : { "type" : "keyword" },
	          "moduleID" : { "type" : "keyword" },
	          "featureID" : { "type" : "keyword" },
	          "origin" : { "type" : "text", "index" : "false" },
	          "returnCode" : { "type" : "keyword" },
	          "deliveryStatus" : { "type" : "keyword" },
	          "returnCodeDetails" : { "type" : "text", "index" : "false" }
                }
        }
    }'
  echo

  #
  #  manually create odr index
  #   - these settings are for index heavy load
  #

  curl -XPUT http://$MASTER_ESROUTER_SERVER/odr -H'Content-Type: application/json' -d'
    {
      "settings" :
        {
          "index" :
            {
              "number_of_shards" : "'$ELASTICSEARCH_SHARDS_SMALL'",
              "number_of_replicas" : "'$ELASTICSEARCH_REPLICAS'",
              "refresh_interval" : "30s",
              "translog" : 
                { 
                  "durability" : "async", 
                  "sync_interval" : "10s" 
                },
              "routing" : 
                {
                  "allocation" : { "total_shards_per_node" : '$ELASTICSEARCH_SHARDS_SMALL' }
                },
              "merge" : 
                {
                  "scheduler" : { "max_thread_count" : 4, "max_merge_count" : 100 }
                }
            }
        },
      "mappings" :
        {
              "properties" :
                {
	          "subscriberID" : { "type" : "keyword" },
	          "eventID" : { "type" : "keyword" },
	          "eventDatetime" : { "type" : "date" },
	          "purchaseID" : { "type" : "keyword" },
	          "offerID" : { "type" : "keyword" },
	          "offerQty" : { "type" : "integer", "index" : "false" },
	          "salesChannelID" : { "type" : "keyword" },
	          "offerPrice" : { "type" : "integer", "index" : "false" },
	          "offerStock" : { "type" : "integer", "index" : "false" },
	          "offerContent" : { "type" : "text", "index" : "false" },
	          "moduleID" : { "type" : "keyword" },
	          "featureID" : { "type" : "keyword" },
	          "origin" : { "type" : "text", "index" : "false" },
	          "returnCode" : { "type" : "keyword" },
	          "deliveryStatus" : { "type" : "keyword" },
	          "returnCodeDetails" : { "type" : "text", "index" : "false" },
	          "voucherCode" : { "type" : "keyword" },
	          "voucherPartnerID" : { "type" : "keyword" }
                }
        }
    }'
  echo
  
  #
  #  manually create notification index
  #   - these settings are for index heavy load
  #

  curl -XPUT http://$MASTER_ESROUTER_SERVER/notification -H'Content-Type: application/json' -d'
    {
      "settings" :
        {
          "index" :
            {
              "number_of_shards" : "'$ELASTICSEARCH_SHARDS_SMALL'",
              "number_of_replicas" : "'$ELASTICSEARCH_REPLICAS'",
              "refresh_interval" : "30s",
              "translog" : 
                { 
                  "durability" : "async", 
                  "sync_interval" : "10s" 
                },
              "routing" : 
                {
                  "allocation" : { "total_shards_per_node" : '$ELASTICSEARCH_SHARDS_SMALL' }
                },
              "merge" : 
                {
                  "scheduler" : { "max_thread_count" : 4, "max_merge_count" : 100 }
                }
            }
        },
      "mappings" :
        {
              "properties" :
                {
	          "subscriberID" : { "type" : "keyword" },
	          "eventID" : { "type" : "keyword" },
	          "eventDatetime" : { "type" : "date" },
	          "messageID" : { "type" : "keyword" },
	          "moduleID" : { "type" : "keyword" },
	          "featureID" : { "type" : "keyword" },
	          "origin" : { "type" : "text", "index" : "false" },
	          "returnCode" : { "type" : "keyword" },
	          "deliveryStatus" : { "type" : "keyword" },
	          "returnCodeDetails" : { "type" : "text", "index" : "false" }
                }
        }
    }'
  echo
  
  #
  #  manually create journeystatistic index
  #   - these settings are for index heavy load
  #

  curl -XPUT http://$MASTER_ESROUTER_SERVER/journeystatistic -H'Content-Type: application/json' -d'
    {
      "settings" :
        {
          "index" :
            {
              "number_of_shards" : "'$ELASTICSEARCH_SHARDS_SMALL'",
              "number_of_replicas" : "'$ELASTICSEARCH_REPLICAS'",
              "refresh_interval" : "30s",
              "translog" : 
                { 
                  "durability" : "async", 
                  "sync_interval" : "10s" 
                },
              "routing" : 
                {
                  "allocation" : { "total_shards_per_node" : '$ELASTICSEARCH_SHARDS_SMALL' }
                },
              "merge" : 
                {
                  "scheduler" : { "max_thread_count" : 4, "max_merge_count" : 100 }
                }
            }
        },
      "mappings" :
        {
              "properties" :
                {
                  "journeyInstanceID" : { "type" : "keyword" },
                  "journeyID" : { "type" : "keyword" },
                  "subscriberID" : { "type" : "keyword" },
                  "transitionDate" : { "type" : "date" },
                  "statsHistory" : { "type" : "keyword" },
                  "statusHistory" : { "type" : "keyword" },
                  "deliveryRequestID" : { "type" : "keyword" },
                  "markNotified" : { "type" : "boolean" },
                  "markConverted" : { "type" : "boolean" },
                  "statusNotified" : { "type" : "boolean" },
                  "statusConverted" : { "type" : "boolean" },
                  "statusControlGroup" : { "type" : "boolean" },
                  "statusUniversalControlGroup" : { "type" : "boolean" },
                  "journeyComplete" : { "type" : "boolean" }
                }
        }
    }'
  echo

  #
  #  manually create journeymetric index
  #   - these settings are for index heavy load
  #

  curl -XPUT http://$MASTER_ESROUTER_SERVER/journeymetric -H'Content-Type: application/json' -d'
    {
      "settings" :
        {
          "index" :
            {
              "number_of_shards" : "'$ELASTICSEARCH_SHARDS_SMALL'",
              "number_of_replicas" : "'$ELASTICSEARCH_REPLICAS'",
              "refresh_interval" : "30s",
              "translog" : 
                { 
                  "durability" : "async", 
                  "sync_interval" : "10s" 
                },
              "routing" : 
                {
                  "allocation" : { "total_shards_per_node" : '$ELASTICSEARCH_SHARDS_SMALL' }
                },
              "merge" : 
                {
                  "scheduler" : { "max_thread_count" : 4, "max_merge_count" : 100 }
                }
            }
        },
      "mappings" :
        {
              "properties" :
                {
                  "journeyInstanceID" : { "type" : "keyword" },
                  "journeyID" : { "type" : "keyword" },
                  "subscriberID" : { "type" : "keyword" },
                  "journeyExitDate" : { "type" : "date" }
                }
        }
    }'
  echo

  #
  #  manually create regr_criteria index
  #
  
  if [ "${env.USE_REGRESSION}" = "1" ]
  then
    curl -XPUT http://$MASTER_ESROUTER_SERVER/regr_criteria -H'Content-Type: application/json' -d'
      {
        "settings" :
          {
            "index" :
              {
                "number_of_shards" : "'$ELASTICSEARCH_SHARDS_LARGE'",
                "number_of_replicas" : "'$ELASTICSEARCH_REPLICAS'",
	        "refresh_interval" : "5s"
              }
          },
        "mappings" :
          {
                "properties" :
                  {
                    "subscriberID" : { "type" : "keyword" },
                    "offerID" : { "type" : "keyword" },
                    "eligible" : { "type" : "keyword" },
                    "evaluationDate" : { "type" : "date" }
                  }
          }
      }'
    echo
  fi

  #
  #  manually create regr_counter index
  #

  if [ "${env.USE_REGRESSION}" = "1" ]
  then
    curl -XPUT http://$MASTER_ESROUTER_SERVER/regr_counter -H'Content-Type: application/json' -d'
      {
        "settings" :
          {
            "index" :
              {
                "number_of_shards" : "'$ELASTICSEARCH_SHARDS_LARGE'",
                "number_of_replicas" : "'$ELASTICSEARCH_REPLICAS'",
                "refresh_interval" : "5s"
              }
          },
        "mappings" :
          {
                "properties" :
                  {
                    "count" : { "type" : "long" }
                  }
          }
      }'

    curl -XPUT http://$MASTER_ESROUTER_SERVER/regr_counter/_create/1 -H'Content-Type: application/json' -d'
      {
        "count" : 100
      }'

    echo
  fi


