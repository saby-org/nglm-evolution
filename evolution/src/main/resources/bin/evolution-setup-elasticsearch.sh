#################################################################################
#
#  evolution-setup-elasticsearch
#
#################################################################################

# -------------------------------------------------------------------------------
#
# default
#
# -------------------------------------------------------------------------------
#
#  Root template.
# Those settings will be applied by default to all indexes
# Order is intentionally set to -10 (to be taken into account before every other templates).
#
curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/root -H'Content-Type: application/json' -d'
{
  "index_patterns": ["*"],
  "order": -10,
  "settings" : {
    "index" : {
      "number_of_shards" : "'$ELASTICSEARCH_SHARDS_DEFAULT'",
      "number_of_replicas" : "'$ELASTICSEARCH_REPLICAS_DEFAULT'",
      "refresh_interval" : "30s",
      "translog" : { 
        "durability" : "async", 
        "sync_interval" : "10s" 
      }
    }
  },
  "mappings" : {
   "dynamic_templates": [ {
       "strings_as_keywords": {
         "match_mapping_type": "string",
         "mapping": { "type": "keyword" }
       }
     }, {
       "numerics_as_integers": {
         "match_mapping_type": "long",
         "mapping": { "type": "integer" }
       }
     }
   ]
  }
}'
echo

# -------------------------------------------------------------------------------
#
# subscriberprofile & snapshots
#
# -------------------------------------------------------------------------------
#
#  manually create subscriberprofile template
#   - this template will be used by the subscriberprofile index, and also subscriberprofile snapshots
#  Order is set to -1 to be taken into account before subscriberprofile_snapshot
#
curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/subscriberprofile -H'Content-Type: application/json' -d'
{
  "index_patterns": ["subscriberprofile*"],
  "order": -1,
  "settings" : {
    "index" : {
      "number_of_shards" : "'$ELASTICSEARCH_SHARDS_SUBSCRIBERPROFILE'",
      "number_of_replicas" : "'$ELASTICSEARCH_REPLICAS_SUBSCRIBERPROFILE'"
    }
  },
  "mappings" : {
    "properties" : {
      "subscriberID"                        : { "type" : "keyword" },
      "evaluationDate"                      : { "type" : "date"    },
      "evolutionSubscriberStatus"           : { "type" : "keyword" },
      "previousEvolutionSubscriberStatus"   : { "type" : "keyword" },
      "evolutionSubscriberStatusChangeDate" : { "type" : "date"    },
      "universalControlGroup"               : { "type" : "boolean" },
      "language"                            : { "type" : "keyword" },
      "segments"                            : { "type" : "keyword" },
      "targets"                             : { "type" : "keyword" },
      "lastUpdateDate"                      : { "type" : "date", "format":"yyyy-MM-dd HH:mm:ss.SSSZZ" },
      "pointFluctuations"                   : { "type" : "object"  },
      "subscriberJourneys"                  : { "type" : "nested"  },
      "tokens"                              : { "type" : "nested",
      	   "properties" : {
              "creationDate"       : { "type" : "long" },
              "expirationDate"     : { "type" : "long" },
              "redeemedDate"       : { "type" : "long" },
              "lastAllocationDate" : { "type" : "long" }
           }
      },
      "vouchers"                            : { "type" : "nested",
      	   "properties" : {
              "vouchers"        : { "type": "nested", "properties": { "voucherExpiryDate" : { "type" : "date" } } }
           }
      },
      "loyaltyPrograms"                     : { "type" : "nested",
      	   "properties" : {
              "loyaltyProgramEnrollmentDate" : { "type" : "date" },
              "loyaltyProgramExitDate"       : { "type" : "date" },
              "tierUpdateDate"               : { "type" : "date" },
              "loyaltyProgramEpoch"          : { "type" : "long" }
           }
      },
      "pointBalances"                       : { "type": "nested",
           "properties" : {
              "earliestExpirationDate" : { "type" : "date" },
              "expirationDates"        : { "type": "nested", "properties": { "date" : { "type" : "date" } } }
           }
        }
    }
  }
}'
echo

#
#  create a cleaning policy for subscriberprofile snapshots
#
curl -XPUT http://$MASTER_ESROUTER_SERVER/_ilm/policy/subscriberprofile_snapshot_policy -H'Content-Type: application/json' -d'
{
  "policy": {
    "phases": {
      "delete": {
        "min_age": "'$ELASTICSEARCH_SUBSCRIBERPROFILE_SNAPSHOT_CLEANING'",
        "actions": { "delete": {} }
      }
    }
  }
}'
echo

#
#  override subscriberprofile template for snapshots ONLY with cleaning policy
#
curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/subscriberprofile_snapshot -H'Content-Type: application/json' -d'
{
  "index_patterns": ["subscriberprofile_snapshot-*"],
  "settings" : {
    "index" : {
      "number_of_shards" : "'$ELASTICSEARCH_SHARDS_SNAPSHOT'",
      "number_of_replicas" : "'$ELASTICSEARCH_REPLICAS_SNAPSHOT'",
      "lifecycle.name": "subscriberprofile_snapshot_policy"
    }
  }
}'
echo

# -------------------------------------------------------------------------------
#
# odr, bdr, mdr, token
#
# -------------------------------------------------------------------------------
#
#  create a cleaning policy for bdr
#
curl -XPUT http://$MASTER_ESROUTER_SERVER/_ilm/policy/bdr_policy -H'Content-Type: application/json' -d'
{
  "policy": {
    "phases": {
      "delete": {
         "min_age": "'$ELASTICSEARCH_BDR_CLEANING'",
         "actions": { "delete": {} }
      }
    }
  }
}'
echo

#
#  manually create bdr template
#
curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/bdr -H'Content-Type: application/json' -d'
{
  "index_patterns": ["detailedrecords_bonuses-*"],
  "settings" : {
    "index" : {
      "lifecycle.name": "bdr_policy"
    }
  },
  "mappings" : {
    "properties" : {
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
        "timezone" : "'$DEPLOYMENT_TIMEZONE'",
        "date_rounding" : "d"
      }
    }
  ]
}'
echo

#
#  create a cleaning policy for tokens
#
curl -XPUT http://$MASTER_ESROUTER_SERVER/_ilm/policy/token_policy -H'Content-Type: application/json' -d'
{
  "policy": {
    "phases": {
      "delete": {
         "min_age": "'$ELASTICSEARCH_TOKEN_CLEANING'",
         "actions": { "delete": {} }
      }
    }
  }
}'
echo

#
#  manually create token template
#
curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/token -H'Content-Type: application/json' -d'
{
  "index_patterns": ["detailedrecords_tokens-*"],
  "settings" : {
    "index" : {
      "lifecycle.name": "token_policy"
    }
  },
  "mappings" : {
    "properties" : {
      "subscriberID"  : { "type" : "keyword" },
      "tokenCode"     : { "type" : "keyword" },
      "action"        : { "type" : "keyword" },
      "eventDatetime" : { "type" : "date", "format":"yyyy-MM-dd HH:mm:ss.SSSZZ"},
      "eventID"       : { "type" : "keyword" },
      "returnCode"    : { "type" : "keyword" },
      "origin"        : { "type" : "keyword", "index" : "false" }
    }
  }
}'
echo

#
#  manually create token pipeline
#
curl -XPUT http://$MASTER_ESROUTER_SERVER/_ingest/pipeline/token-daily -H 'Content-Type: application/json' -d'
{
  "description": "daily token index naming",
  "processors" : [
    {
      "date_index_name" : {
        "field" : "eventDatetime",
        "index_name_prefix" : "detailedrecords_tokens-",
        "index_name_format" : "yyyy-MM-dd",
        "date_formats" : ["yyyy-MM-dd HH:mm:ss.SSSZZ"],
        "timezone" : "'$DEPLOYMENT_TIMEZONE'",
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
  "policy": {
    "phases": {
      "delete": {
         "min_age": "'$ELASTICSEARCH_ODR_CLEANING'",
         "actions": { "delete": {} }
      }
    }
  }
}'
echo

#
#  manually create odr template
#
curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/odr -H'Content-Type: application/json' -d'
{
  "index_patterns": ["detailedrecords_offers-*"],
  "settings" : {
    "index" : {
      "lifecycle.name": "odr_policy"
    }
  },
  "mappings" : {
    "properties" : {
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
        "timezone" : "'$DEPLOYMENT_TIMEZONE'",
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
  "policy": {
    "phases": {
      "delete": {
         "min_age": "'$ELASTICSEARCH_MDR_CLEANING'",
         "actions": { "delete": {} }
      }
    }
  }
}'
echo

#
#  manually create mdr template
#
curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/mdr -H'Content-Type: application/json' -d'
{
  "index_patterns": ["detailedrecords_messages-*"],
  "settings" : {
    "index" : {
      "lifecycle.name": "mdr_policy"
    }
  },
  "mappings" : {
    "properties" : {
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
        "timezone" : "'$DEPLOYMENT_TIMEZONE'",
        "date_rounding" : "d"
      }
    }
  ]
}'
echo

# -------------------------------------------------------------------------------
#
# journeystatistic
#
# -------------------------------------------------------------------------------
#
#  manually create journeystatistic index
#
curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/journeystatistic -H'Content-Type: application/json' -d'
{
  "index_patterns": ["journeystatistic*"],
  "mappings" : {
    "properties" : {
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
      "statusTargetGroup" : { "type" : "boolean" },
      "statusControlGroup" : { "type" : "boolean" },
      "statusUniversalControlGroup" : { "type" : "boolean" },
      "journeyComplete" : { "type" : "boolean" },
      "journeyExitDate" : { "type" : "date" }
    }
  }
}'
echo

# -------------------------------------------------------------------------------
#
# reg_criteria, regr_counter (for test environment ONLY)
#
# -------------------------------------------------------------------------------
if [ "${env.USE_REGRESSION}" = "1" ]
then
  #
  #  manually create regr_criteria index
  #
  curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/regr_criteria -H'Content-Type: application/json' -d'
  {
    "index_patterns": ["regr_criteria"],
    "mappings" : {
      "properties" : {
        "subscriberID" : { "type" : "keyword" },
        "offerID" : { "type" : "keyword" },
        "eligible" : { "type" : "keyword" },
        "evaluationDate" : { "type" : "date" }
      }
    }
  }'
  echo

  #
  #  manually create regr_counter index
  #
  curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/regr_counter -H'Content-Type: application/json' -d'
  {
    "index_patterns": ["regr_counter"],
    "mappings" : {
      "properties" : {
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

# -------------------------------------------------------------------------------
#
# datacubes
#
# -------------------------------------------------------------------------------
curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/datacube_subscriberprofile -H'Content-Type: application/json' -d'
{
  "index_patterns": ["datacube_subscriberprofile"],
  "mappings" : {
    "properties" : {
      "timestamp" : { "type" : "date", "format":"yyyy-MM-dd HH:mm:ss.SSSZZ" },
      "period" : { "type" : "long" },
      "count" : { "type" : "integer" }
    }
  }
}'
echo

curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/datacube_loyaltyprogramshistory -H'Content-Type: application/json' -d'
{
  "index_patterns": ["datacube_loyaltyprogramshistory"],
  "mappings" : {
    "properties" : {
      "timestamp" : { "type" : "date", "format":"yyyy-MM-dd HH:mm:ss.SSSZZ" },
      "period" : { "type" : "long" },
      "filter.loyaltyProgram" : { "type" : "keyword" },
      "filter.tier" : { "type" : "keyword" },
      "filter.evolutionSubscriberStatus" : { "type" : "keyword" },
      "filter.redeemer" : { "type" : "boolean" },
      "count" : { "type" : "integer" },
      "metric.rewards.redeemed" : { "type" : "integer" },
      "metric.rewards.earned" : { "type" : "integer" },
      "metric.rewards.expired" : { "type" : "integer" }
    }
  }
}'
echo

curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/datacube_loyaltyprogramschanges -H'Content-Type: application/json' -d'
{
  "index_patterns": ["datacube_loyaltyprogramschanges"],
  "mappings" : {
    "properties" : {
      "timestamp" : { "type" : "date", "format":"yyyy-MM-dd HH:mm:ss.SSSZZ" },
      "period" : { "type" : "long" },
      "filter.loyaltyProgram" : { "type" : "keyword" },
      "filter.newTier" : { "type" : "keyword" },
      "filter.previousTier" : { "type" : "keyword" },
      "filter.tierChangeType" : { "type" : "keyword" },
      "count" : { "type" : "integer" }
    }
  }
}'
echo

curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/datacube_journeytraffic- -H'Content-Type: application/json' -d'
{
  "index_patterns": ["datacube_journeytraffic-*"],
  "mappings" : {
    "properties" : {
      "timestamp" : { "type" : "date", "format":"yyyy-MM-dd HH:mm:ss.SSSZZ" },
      "period" : { "type" : "long" },
      "filter.journey" : { "type" : "keyword" },
      "filter.node" : { "type" : "keyword" },
      "filter.status" : { "type" : "keyword" },
      "count" : { "type" : "integer" }
    }
  }
}'
echo

curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/datacube_journeyrewards- -H'Content-Type: application/json' -d'
{
  "index_patterns": ["datacube_journeyrewards-*"],
  "mappings" : {
    "properties" : {
      "timestamp" : { "type" : "date", "format":"yyyy-MM-dd HH:mm:ss.SSSZZ" },
      "period" : { "type" : "long" },
      "filter.journey" : { "type" : "keyword" },
      "count" : { "type" : "integer" }
    }
  }
}'
echo

curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/datacube_odr -H'Content-Type: application/json' -d'
{
  "index_patterns": ["datacube_odr"],
  "mappings" : {
    "properties" : {
      "timestamp" : { "type" : "date", "format":"yyyy-MM-dd HH:mm:ss.SSSZZ" },
      "period" : { "type" : "long" },
      "filter.offer" : { "type" : "keyword" },
      "filter.module" : { "type" : "keyword" },
      "filter.feature" : { "type" : "keyword" },
      "filter.salesChannel" : { "type" : "keyword" },
      "filter.meanOfPayment" : { "type" : "keyword" },
      "filter.meanOfPaymentProviderID" : { "type" : "keyword" },
      "filter.offerObjectives" : { "type" : "keyword" },
      "count" : { "type" : "integer" },
      "metric.totalAmount" : { "type" : "integer" }
    }
  }
}'
echo

curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/datacube_bdr -H'Content-Type: application/json' -d'
{
  "index_patterns": ["datacube_bdr"],
  "mappings" : {
    "properties" : {
      "timestamp" : { "type" : "date", "format":"yyyy-MM-dd HH:mm:ss.SSSZZ" },
      "period" : { "type" : "long" },
      "filter.module" : { "type" : "keyword" },
      "filter.feature" : { "type" : "keyword" },
      "filter.provider" : { "type" : "keyword" },
      "filter.operation" : { "type" : "keyword" },
      "filter.salesChannel" : { "type" : "keyword" },
      "filter.deliverable" : { "type" : "keyword" },
      "filter.returnCode" : { "type" : "keyword" },
      "count" : { "type" : "integer" },
      "metric.totalQty" : { "type" : "integer" }
    }
  }
}'
echo

curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/datacube_messages -H'Content-Type: application/json' -d'
{
  "index_patterns": ["datacube_messages"],
  "mappings" : {
    "properties" : {
      "timestamp" : { "type" : "date", "format":"yyyy-MM-dd HH:mm:ss.SSSZZ" },
      "period" : { "type" : "long" },
      "filter.module" : { "type" : "keyword" },
      "filter.feature" : { "type" : "keyword" },
      "filter.provider" : { "type" : "keyword" },
      "filter.language" : { "type" : "keyword" },
      "filter.template" : { "type" : "keyword" },
      "filter.channel" : { "type" : "keyword" },
      "filter.returnCode" : { "type" : "keyword" },
      "count" : { "type" : "integer" },
      "metric.totalQty" : { "type" : "integer" }
    }
  }
}'
echo
# -------------------------------------------------------------------------------
#
# mappings
#
# -------------------------------------------------------------------------------
#
# mapping_modules is a static index, filled at deployment.
# Rows are manually insert below.
#
curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/mapping_modules -H'Content-Type: application/json' -d'
{
  "index_patterns": ["mapping_modules"],
  "mappings" : {
    "properties" : {
      "moduleID" : { "type" : "keyword" },
      "moduleName" : { "type" : "keyword" },
      "moduleDisplay" : { "type" : "keyword" },
      "moduleFeature" : { "type" : "keyword" }
    }
  }
}'
echo

curl -XPUT http://$MASTER_ESROUTER_SERVER/mapping_modules/_doc/1 -H'Content-Type: application/json' -d'
{
  "moduleID" : "1", "moduleName": "Journey_Manager", "moduleDisplay" : "Journey Manager", "moduleFeature" : "journeyID"
}'
echo

curl -XPUT http://$MASTER_ESROUTER_SERVER/mapping_modules/_doc/2 -H'Content-Type: application/json' -d'
{
  "moduleID" : "2", "moduleName": "Loyalty_Program", "moduleDisplay" : "Loyalty Program", "moduleFeature" : "loyaltyProgramID"
}'
echo

curl -XPUT http://$MASTER_ESROUTER_SERVER/mapping_modules/_doc/3 -H'Content-Type: application/json' -d'
{
  "moduleID" : "3", "moduleName": "Offer_Catalog", "moduleDisplay" : "Offer Catalog", "moduleFeature" : "offerID"
}'
echo

curl -XPUT http://$MASTER_ESROUTER_SERVER/mapping_modules/_doc/4 -H'Content-Type: application/json' -d'
{
  "moduleID" : "4", "moduleName": "Delivery_Manager", "moduleDisplay" : "Delivery Manager", "moduleFeature" : "deliverableID"
}'
echo

curl -XPUT http://$MASTER_ESROUTER_SERVER/mapping_modules/_doc/5 -H'Content-Type: application/json' -d'
{
  "moduleID" : "5", "moduleName": "Customer_Care", "moduleDisplay" : "Customer Care", "moduleFeature" : "none"
}'
echo

curl -XPUT http://$MASTER_ESROUTER_SERVER/mapping_modules/_doc/6 -H'Content-Type: application/json' -d'
{
  "moduleID" : "6", "moduleName": "REST_API", "moduleDisplay" : "REST API", "moduleFeature" : "none"
}'
echo

curl -XPUT http://$MASTER_ESROUTER_SERVER/mapping_modules/_doc/999 -H'Content-Type: application/json' -d'
{
  "moduleID" : "999", "moduleName": "Unknown", "moduleDisplay" : "Unknown", "moduleFeature" : "none"
}'
echo

curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/mapping_journeys -H'Content-Type: application/json' -d'
{
  "index_patterns": ["mapping_journeys"],
  "mappings" : {
    "properties" : {
      "journeyID" : { "type" : "keyword" },
      "display" : { "type" : "keyword" },
      "description" : { "type" : "keyword" },
      "type" : { "type" : "keyword" },
      "user" : { "type" : "keyword" },
      "targets" : { "type" : "keyword" },
      "targetCount" : { "type" : "integer" },
      "objectives" : { "type" : "keyword" },
      "startDate" : { "type" : "date", "format":"yyyy-MM-dd HH:mm:ss.SSSZZ" },
      "endDate" : { "type" : "date", "format":"yyyy-MM-dd HH:mm:ss.SSSZZ" },
      "active" : { "type" : "boolean" },
      "timestamp" : { "type" : "date", "format":"yyyy-MM-dd HH:mm:ss.SSSZZ" }
    }
  }
}'
echo

curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/mapping_journeyrewards -H'Content-Type: application/json' -d'
{
  "index_patterns": ["mapping_journeyrewards"],
  "mappings" : {
    "properties" : {
      "journeyID" : { "type" : "keyword" },
      "reward" : { "type" : "keyword" },
      "timestamp" : { "type" : "date", "format":"yyyy-MM-dd HH:mm:ss.SSSZZ" }
    }
  }
}'
echo

curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/mapping_deliverables -H'Content-Type: application/json' -d'
{
  "index_patterns": ["mapping_deliverables"],
  "mappings" : {
    "properties" : {
      "deliverableID" : { "type" : "keyword" },
      "deliverableName" : { "type" : "keyword" },
      "deliverableActive" : { "type" : "boolean" },
      "deliverableProviderID" : { "type" : "keyword" }
    }
  }
}'
echo

curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/mapping_basemanagement -H'Content-Type: application/json' -d'
{
  "index_patterns": ["mapping_basemanagement"],
  "mappings" : {
    "properties" : {
      "id"            : { "type" : "keyword" },
      "display"       : { "type" : "keyword" },
      "active"        : { "type" : "boolean" },
      "targetingType" : { "type" : "keyword" },
      "segments"      : { "type" : "nested",
      	   "properties" : {
              "id"    : { "type" : "keyword" },
              "name"  : { "type" : "keyword" }
           }
      },
      "createdDate"   : { "type" : "date" },
      "createdDate from JR": { "type" : "date" },
      "timestamp"     : { "type" : "date", "format":"yyyy-MM-dd HH:mm:ss.SSSZZ" }
    }
  }
}'
echo

curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/mapping_journeyobjective -H'Content-Type: application/json' -d'
{
  "index_patterns": ["mapping_journeyobjective"],
  "mappings" : {
    "properties" : {
      "id"            : { "type" : "keyword" },
      "display"       : { "type" : "keyword" },
      "contactPolicy" : { "type" : "keyword" },
      "timestamp"     : { "type" : "date", "format":"yyyy-MM-dd HH:mm:ss.SSSZZ" },
      "getEffectiveWaitingPeriodEndDate"     : { "type" : "date" },
      "getEffectiveSlidingWindowStartDate"     : { "type" : "date" },
      "getEpoch"     : { "type" : "date" },
      "getEffectiveStartDate"     : { "type" : "date" },
      "getEffectiveEndDate"     : { "type" : "date" },
      "getCreatedDate"     : { "type" : "date" },
      "getUpdatedDate"     : { "type" : "date" }
      
    }
  }
}'
echo

