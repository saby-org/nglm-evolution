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
              "number_of_shards" : "6",
              "number_of_replicas" : "1",
              "refresh_interval" : "30s",
              "translog" : 
                { 
                  "durability" : "async", 
                  "sync_interval" : "10s" 
                },
              "routing" : 
                {
                  "allocation" : { "total_shards_per_node" : 4 }
                },
              "merge" : 
                {
                  "scheduler" : { "max_thread_count" : 4, "max_merge_count" : 100 }
                }
            }
        },
      "mappings" :
        {
          "doc" :
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
                  "subscriberGroups" : { "type" : "keyword" }
                }
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
              "number_of_shards" : "6",
              "number_of_replicas" : "1",
              "refresh_interval" : "30s",
              "translog" : 
                { 
                  "durability" : "async", 
                  "sync_interval" : "10s" 
                },
              "routing" : 
                {
                  "allocation" : { "total_shards_per_node" : 4 }
                },
              "merge" : 
                {
                  "scheduler" : { "max_thread_count" : 4, "max_merge_count" : 100 }
                }
            }
        },
      "mappings" :
        {
          "doc" :
            {
              "properties" :
                {
                  "journeyInstanceID" : { "type" : "keyword" },
                  "journeyID" : { "type" : "keyword" },
                  "subscriberID" : { "type" : "keyword" },
                  "transitionDate" : { "type" : "date" },
                  "linkID" : { "type" : "keyword" },
                  "fromNodeID" : { "type" : "keyword" },
                  "toNodeID" : { "type" : "keyword" },
                  "deliveryRequestID" : { "type" : "keyword" },
                  "statusNotified" : { "type" : "boolean" },
                  "statusConverted" : { "type" : "boolean" },
                  "statusControlGroup" : { "type" : "boolean" },
                  "statusUniversalControlGroup" : { "type" : "boolean" },
                  "journeyComplete" : { "type" : "boolean" }
                }
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
                "number_of_shards" : "12",
                "number_of_replicas" : "1",
                "refresh_interval" : "5s"
              }
          },
        "mappings" :
          {
            "doc" :
              {
                "properties" :
                  {
                    "subscriberID" : { "type" : "keyword" },
                    "offerID" : { "type" : "keyword" },
                    "eligible" : { "type" : "keyword" },
                    "evaluationDate" : { "type" : "date" }
                  }
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
                "number_of_shards" : "12",
                "number_of_replicas" : "1",
                "refresh_interval" : "5s"
              }
          },
        "mappings" :
          {
            "doc" :
              {
                "properties" :
                  {
                    "count" : { "type" : "long" }
                  }
              }
          }
      }'

    curl -XPUT http://$MASTER_ESROUTER_SERVER/regr_counter/doc/1 -H'Content-Type: application/json' -d'
      {
        "count" : 100
      }'
      
    echo
  fi

  