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
                  "segments" : { "type" : "keyword" },
                  "loyaltyPrograms" : { "type" : "nested"},
                  "pointFluctuations" : { "type" : "object"},
                  "pointBalances" : { "type" : "keyword", "index" : "false"}
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
  #  create a cleaning policy for bdr
  #
  
  curl -XPUT http://$MASTER_ESROUTER_SERVER/_ilm/policy/bdr_policy -H'Content-Type: application/json' -d'
    {
      "policy": 
        {
          "phases": 
            {
              "delete": 
                {
                  "min_age": "'$ELASTICSEARCH_BDR_CLEANING'",
                  "actions": 
                    {
                      "delete": {}
                    }
                }
            }
        }
    }'
  echo

  #
  #  manually create bdr template
  #   - these settings are for index heavy load
  #

  curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/bdr -H'Content-Type: application/json' -d'
    {
      "index_patterns": ["detailedrecords_bonuses-*"],
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
                },
              "lifecycle.name": "bdr_policy"
            }
        },
      "mappings" :
        {
              "properties" :
                {
	          "subscriberID" : { "type" : "keyword" },
	          "providerID" : { "type" : "keyword" },
	          "eventID" : { "type" : "keyword" },
	          "deliveryRequestID" : { "type" : "keyword" },
	          "deliverableID" : { "type" : "keyword" },
	          "eventDatetime" : { "type" : "date", "format":"yyyy-MM-dd HH:mm:ss.SSSZZ"},
	          "deliverableExpiration" : { "type" : "date" },
	          "deliverableQty" : { "type" : "integer", "index" : "false" },
	          "operation" : { "type" : "keyword" },
	          "moduleID" : { "type" : "keyword" },
	          "featureID" : { "type" : "keyword" },
	          "origin" : { "type" : "keyword", "index" : "false" },
	          "returnCode" : { "type" : "keyword" },
	          "deliveryStatus" : { "type" : "keyword" },
	          "returnCodeDetails" : { "type" : "keyword", "index" : "false" }
                }
        }
    }'
  echo
  
  #
  #  manually create bdr pipeline
  #

  curl -XPUT http://$MASTER_ESROUTER_SERVER/_ingest/pipeline/bdr-daily -H 'Content-Type: application/json' -d'
    {
      "description": "daily bdr index naming",
      "processors" : [
        {
          "date_index_name" : {
            "field" : "eventDatetime",
            "index_name_prefix" : "detailedrecords_bonuses-",
            "index_name_format" : "yyyy-MM-dd",
            "date_formats" : ["yyyy-MM-dd HH:mm:ss.SSSZZ"],
            "date_rounding" : "d"
          }
        }
      ]
    }'
  echo
  
  #
  #  create a cleaning policy for odr
  #
  
  curl -XPUT http://$MASTER_ESROUTER_SERVER/_ilm/policy/odr_policy -H'Content-Type: application/json' -d'
    {
      "policy": 
        {
          "phases": 
            {
              "delete": 
                {
                  "min_age": "'$ELASTICSEARCH_ODR_CLEANING'",
                  "actions": 
                    {
                      "delete": {}
                    }
                }
            }
        }
    }'
  echo

  #
  #  manually create odr template
  #   - these settings are for index heavy load
  #

  curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/odr -H'Content-Type: application/json' -d'
    {
      "index_patterns": ["detailedrecords_offers-*"],
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
                },
              "lifecycle.name": "odr_policy"
            }
        },
      "mappings" :
        {
              "properties" :
                {
	          "subscriberID" : { "type" : "keyword" },
	          "eventDatetime" : { "type" : "date", "format":"yyyy-MM-dd HH:mm:ss.SSSZZ"},
	          "deliveryRequestID" : { "type" : "keyword" },
	          "eventID" : { "type" : "keyword" },
	          "offerID" : { "type" : "keyword" },
	          "offerQty" : { "type" : "integer", "index" : "false" },
	          "salesChannelID" : { "type" : "keyword" },
	          "offerPrice" : { "type" : "integer", "index" : "false" },
	          "meanOfPayment" : { "type" : "keyword", "index" : "false" },
	          "offerStock" : { "type" : "integer", "index" : "false" },
	          "offerContent" : { "type" : "keyword", "index" : "false" },
	          "moduleID" : { "type" : "keyword" },
	          "featureID" : { "type" : "keyword" },
	          "origin" : { "type" : "keyword", "index" : "false" },
	          "returnCode" : { "type" : "keyword" },
	          "deliveryStatus" : { "type" : "keyword" },
	          "returnCodeDetails" : { "type" : "keyword", "index" : "false" },
	          "voucherCode" : { "type" : "keyword" },
	          "voucherPartnerID" : { "type" : "keyword" }
                }
        }
    }'
  echo
  
  #
  #  manually create odr pipeline
  #

  curl -XPUT http://$MASTER_ESROUTER_SERVER/_ingest/pipeline/odr-daily -H 'Content-Type: application/json' -d'
    {
      "description": "daily odr index naming",
      "processors" : [
        {
          "date_index_name" : {
            "field" : "eventDatetime",
            "index_name_prefix" : "detailedrecords_offers-",
            "index_name_format" : "yyyy-MM-dd",
            "date_formats" : ["yyyy-MM-dd HH:mm:ss.SSSZZ"],
            "date_rounding" : "d"
          }
        }
      ]
    }'
  echo
  
  #
  #  create a cleaning policy for mdr
  #
  
  curl -XPUT http://$MASTER_ESROUTER_SERVER/_ilm/policy/mdr_policy -H'Content-Type: application/json' -d'
    {
      "policy": 
        {
          "phases": 
            {
              "delete": 
                {
                  "min_age": "'$ELASTICSEARCH_MDR_CLEANING'",
                  "actions": 
                    {
                      "delete": {}
                    }
                }
            }
        }
    }'
  echo

  #
  #  manually create mdr template
  #   - these settings are for index heavy load
  #

  curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/mdr -H'Content-Type: application/json' -d'
    {
      "index_patterns": ["detailedrecords_messages-*"],
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
                },
              "lifecycle.name": "mdr_policy"
            }
        },
      "mappings" :
        {
              "properties" :
                {
	          "subscriberID" : { "type" : "keyword" },
	          "eventID" : { "type" : "keyword" },
	          "deliveryRequestID" : { "type" : "keyword" },
	          "creationDate" : { "type" : "date", "format":"yyyy-MM-dd HH:mm:ss.SSSZZ"},
	          "deliveryDate" : { "type" : "date", "format":"yyyy-MM-dd HH:mm:ss.SSSZZ"},
	          "messageID" : { "type" : "keyword" },
	          "moduleID" : { "type" : "keyword" },
	          "featureID" : { "type" : "keyword" },
	          "origin" : { "type" : "keyword", "index" : "false" },
	          "returnCode" : { "type" : "keyword" },
	          "deliveryStatus" : { "type" : "keyword" },
	          "returnCodeDetails" : { "type" : "keyword", "index" : "false" }
                }
        }
    }'
  echo
  
  #
  #  manually create mdr pipeline
  #

  curl -XPUT http://$MASTER_ESROUTER_SERVER/_ingest/pipeline/mdr-daily -H 'Content-Type: application/json' -d'
    {
      "description": "daily mdr index naming",
      "processors" : [
        {
          "date_index_name" : {
            "field" : "creationDate",
            "index_name_prefix" : "detailedrecords_messages-",
            "index_name_format" : "yyyy-MM-dd",
            "date_formats" : ["yyyy-MM-dd HH:mm:ss.SSSZZ"],
            "date_rounding" : "d"
          }
        }
      ]
    }'
  echo
  
  #
  #  manually create journeystatistic index
  #   - these settings are for index heavy load
  #

  curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/journeystatistic -H'Content-Type: application/json' -d'
    {
      "index_patterns": ["journeystatistic*"],
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
                  "nodeHistory" : { "type" : "keyword" },
                  "statusHistory" : { "type" : "keyword" },
                  "rewardHistory" : { "type" : "keyword" },
                  "fromNodeID" : { "type" : "keyword" },
                  "toNodeID" : { "type" : "keyword" },
                  "deliveryRequestID" : { "type" : "keyword" },
                  "sample" : { "type" : "keyword" },
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
  #  manually create datacube_journeytraffic index
  #   - these settings are for index heavy load
  #

  curl -XPUT http://$MASTER_ESROUTER_SERVER/datacube_journeytraffic -H'Content-Type: application/json' -d'
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
                  "journeyID" : { "type" : "keyword" },
                  "lastUpdateDate" : { "type" : "date" },
                  "lastArchivedDataDate" : { "type" : "date" },
                  "archivePeriodInSeconds" : { "type" : "integer" },
                  "maxNumberOfPeriods" : { "type" : "integer" },
                  "currentData" : { "type" : "object" },
                  "archivedData" : { "type" : "object" }
                }
        }
    }'
  echo
  
  #
  #  manually create datacube_odr index
  #   - these settings are for index heavy load
  #

  curl -XPUT http://$MASTER_ESROUTER_SERVER/datacube_odr -H'Content-Type: application/json' -d'
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
                  "computationDate" : { "type" : "long" },
                  "filter.dataDate" : { "type" : "keyword" },
                  "filter.offer.id" : { "type" : "keyword" },
                  "filter.offer.display" : { "type" : "keyword" },
                  "filter.module.id" : { "type" : "keyword" },
                  "filter.module.display" : { "type" : "keyword" },
                  "filter.feature.id" : { "type" : "keyword" },
                  "filter.feature.display" : { "type" : "keyword" },
                  "filter.salesChannel.id" : { "type" : "keyword" },
                  "filter.salesChannel.display" : { "type" : "keyword" },
                  "filter.meanOfPayment.id" : { "type" : "keyword" },
                  "filter.meanOfPayment.display" : { "type" : "keyword" },
                  "filter.meanOfPayment.paymentProviderID" : { "type" : "keyword" },
                  "count" : { "type" : "integer" },
                  "data.totalAmount" : { "type" : "integer" }
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

  #
  #  manually create mapping_deliverables index
  #   - these settings are for index heavy load
  #

  curl -XPUT http://$MASTER_ESROUTER_SERVER/mapping_modules -H'Content-Type: application/json' -d'
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
                  "moduleID" : { "type" : "keyword" },
                  "moduleName" : { "type" : "keyword" },
                  "moduleDisplay" : { "type" : "keyword" },
                  "moduleFeature" : { "type" : "keyword" }
                }
        }
    }'
  echo

  #
  #  manually insert data mapping_modules
  #   - these settings are for index heavy load
  #

  curl -XPUT http://$MASTER_ESROUTER_SERVER/mapping_modules/_doc/1 -H'Content-Type: application/json' -d'
    {
      "moduleID" : "1", "moduleName": "Journey_Manager", "moduleDisplay" : "Journey Manager", "moduleFeature" : ""
    }'
  echo

  curl -XPUT http://$MASTER_ESROUTER_SERVER/mapping_modules/_doc/2 -H'Content-Type: application/json' -d'
    {
      "moduleID" : "2", "moduleName": "Loyalty_Program", "moduleDisplay" : "Loyalty Program", "moduleFeature" : ""
    }'
  echo

  curl -XPUT http://$MASTER_ESROUTER_SERVER/mapping_modules/_doc/3 -H'Content-Type: application/json' -d'
    {
      "moduleID" : "3", "moduleName": "Offer_Catalog", "moduleDisplay" : "Offer Catalog", "moduleFeature" : ""
    }'
  echo

  curl -XPUT http://$MASTER_ESROUTER_SERVER/mapping_modules/_doc/4 -H'Content-Type: application/json' -d'
    {
      "moduleID" : "4", "moduleName": "Delivery_Manager", "moduleDisplay" : "Delivery Manager", "moduleFeature" : ""
    }'
  echo

  curl -XPUT http://$MASTER_ESROUTER_SERVER/mapping_modules/_doc/5 -H'Content-Type: application/json' -d'
    {
      "moduleID" : "5", "moduleName": "Customer_Care", "moduleDisplay" : "Customer Care", "moduleFeature" : ""
    }'
  echo

  curl -XPUT http://$MASTER_ESROUTER_SERVER/mapping_modules/_doc/6 -H'Content-Type: application/json' -d'
    {
      "moduleID" : "6", "moduleName": "REST_API", "moduleDisplay" : "REST API", "moduleFeature" : ""
    }'
  echo

  curl -XPUT http://$MASTER_ESROUTER_SERVER/mapping_modules/_doc/999 -H'Content-Type: application/json' -d'
    {
      "moduleID" : "999", "moduleName": "Unknown", "moduleDisplay" : "Unknown", "moduleFeature" : ""
    }'
  echo

  #
  #  manually create mapping_deliverables index
  #   - these settings are for index heavy load
  #

  curl -XPUT http://$MASTER_ESROUTER_SERVER/mapping_deliverables -H'Content-Type: application/json' -d'
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
                  "deliverableID" : { "type" : "keyword" },
                  "deliverableName" : { "type" : "keyword" },
                  "deliverableActive" : { "type" : "boolean" },
                  "deliverableProviderID" : { "type" : "keyword" }
                }
        }
    }'
  echo

  #
  #  manually create mapping_deliverables index
  #   - these settings are for index heavy load
  #

  curl -XPUT http://$MASTER_ESROUTER_SERVER/mapping_paymentmeans -H'Content-Type: application/json' -d'
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
                  "paymentMeanID" : { "type" : "keyword" },
                  "paymentMeanName" : { "type" : "keyword" },
                  "paymentMeanActive" : { "type" : "boolean" },
                  "paymentMeanProviderID" : { "type" : "keyword" }
                }
        }
    }'
  echo

  #
  #  manually create mapping_offers index
  #   - these settings are for index heavy load
  #

  curl -XPUT http://$MASTER_ESROUTER_SERVER/mapping_offers -H'Content-Type: application/json' -d'
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
                  "offerName" : { "type" : "keyword" },
                  "offerActive" : { "type" : "boolean" }
                }
        }
    }'
  echo

  #
  #  manually create mapping_products index
  #   - these settings are for index heavy load
  #

  curl -XPUT http://$MASTER_ESROUTER_SERVER/mapping_products -H'Content-Type: application/json' -d'
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
                  "productID" : { "type" : "keyword" },
                  "productName" : { "type" : "keyword" },
                  "productActive" : { "type" : "boolean" }
                }
        }
    }'
  echo

  #
  #  manually create mapping_loyaltyprograms index
  #   - these settings are for index heavy load
  #

  curl -XPUT http://$MASTER_ESROUTER_SERVER/mapping_loyaltyprograms -H'Content-Type: application/json' -d'
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
                  "loyaltyProgramID" : { "type" : "keyword" },
                  "loyaltyProgramName" : { "type" : "keyword" },
                  "loyaltyProgramType" : { "type" : "keyword" },
                  "rewardPointsID" : { "type" : "keyword" },
                  "statusPointsID" : { "type" : "keyword" },
                  "tiers" : { "type" : "nested" }
                }
        }
    }'
  echo
  
  