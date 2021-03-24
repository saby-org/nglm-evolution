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
prepare-es-update-curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/root -u $ELASTICSEARCH_USERNAME:$ELASTICSEARCH_USERPASSWORD -H'Content-Type: application/json' -d'
{
  "index_patterns": ["*"],
  "order": -10,
  "settings" : {
    "index" : {
      "number_of_shards" : "Deployment.getElasticsearchDefaultShards()",
      "number_of_replicas" : "Deployment.getElasticsearchDefaultReplicas()",
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
prepare-es-update-curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/subscriberprofile -u $ELASTICSEARCH_USERNAME:$ELASTICSEARCH_USERPASSWORD -H'Content-Type: application/json' -d'
{
  "index_patterns": ["subscriberprofile*"],
  "order": -1,
  "settings" : {
    "index" : {
      "number_of_shards" : "Deployment.getElasticsearchSubscriberprofileShards()",
      "number_of_replicas" : "Deployment.getElasticsearchSubscriberprofileReplicas()"
    }
  },
  "mappings" : {
    "properties" : {
      "subscriberID"                        : { "type" : "keyword" },
      "tenantID"                            : { "type" : "integer" },
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
          "vouchers"        : { "type": "nested", "properties": { "voucherExpiryDate" : { "type" : "date" }, "voucherDeliveryDate" : { "type" : "date" } } }
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
prepare-es-update-curl -XPUT http://$MASTER_ESROUTER_SERVER/_opendistro/_ism/policies/subscriberprofile_snapshot_policy -u $ELASTICSEARCH_USERNAME:$ELASTICSEARCH_USERPASSWORD -H'Content-Type: application/json' -d'
{
  "policy": {
    "description": "hot delete workflow for subscriber snapshot",
    "default_state": "hot",
    "schema_version": 1,
    "states": [
      {
        "name": "hot",
        "actions": [],
        "transitions": [
          {
            "state_name": "delete",
            "conditions": {
              "min_index_age": "Deployment.getElasticsearchRetentionDaysSnapshots()d"
            }
          }
        ]
      },
      {
        "name": "delete",
        "actions": [
          {
            "delete": {}
          }
        ]
      }
    ]
  }
}'
echo

#
#  override subscriberprofile template for snapshots ONLY with cleaning policy
#
prepare-es-update-curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/subscriberprofile_snapshot -u $ELASTICSEARCH_USERNAME:$ELASTICSEARCH_USERPASSWORD -H'Content-Type: application/json' -d'
{
  "index_patterns": ["subscriberprofile_snapshot-*"],
  "settings" : {
    "index" : {
      "number_of_shards" : "Deployment.getElasticsearchSnapshotShards()",
      "number_of_replicas" : "Deployment.getElasticsearchSnapshotReplicas()"
    },
    "opendistro.index_state_management.policy_id": "subscriberprofile_snapshot_policy"
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
prepare-es-update-curl -XPUT http://$MASTER_ESROUTER_SERVER/_opendistro/_ism/policies/bdr_policy -u $ELASTICSEARCH_USERNAME:$ELASTICSEARCH_USERPASSWORD -H'Content-Type: application/json' -d'
{
  "policy": {
    "description": "hot delete workflow for bdr",
    "default_state": "hot",
    "schema_version": 1,
    "states": [
      {
        "name": "hot",
        "actions": [],
        "transitions": [
          {
            "state_name": "delete",
            "conditions": {
              "min_index_age": "Deployment.getElasticsearchRetentionDaysBDR()d"
            }
          }
        ]
      },
      {
        "name": "delete",
        "actions": [
          {
            "delete": {}
          }
        ]
      }
    ]
  }
}'
echo

#
#  manually create bdr template
#
prepare-es-update-curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/bdr -u $ELASTICSEARCH_USERNAME:$ELASTICSEARCH_USERPASSWORD -H'Content-Type: application/json' -d'
{
  "index_patterns": ["detailedrecords_bonuses-*"],
  "settings" : {
    "opendistro.index_state_management.policy_id": "bdr_policy"
  },
  "mappings" : {
    "properties" : {
      "subscriberID" : { "type" : "keyword" },
      "tenantID" : { "type" : "integer" },
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
prepare-es-update-curl -XPUT http://$MASTER_ESROUTER_SERVER/_ingest/pipeline/bdr-daily -u $ELASTICSEARCH_USERNAME:$ELASTICSEARCH_USERPASSWORD -H 'Content-Type: application/json' -d'
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
prepare-es-update-curl -XPUT http://$MASTER_ESROUTER_SERVER/_opendistro/_ism/policies/token_policy -u $ELASTICSEARCH_USERNAME:$ELASTICSEARCH_USERPASSWORD -H'Content-Type: application/json' -d'
{
  "policy": {
    "description": "hot delete workflow for token",
    "default_state": "hot",
    "schema_version": 1,
    "states": [
      {
        "name": "hot",
        "actions": [],
        "transitions": [
          {
            "state_name": "delete",
            "conditions": {
              "min_index_age": "Deployment.getElasticsearchRetentionDaysTokens()d"
            }
          }
        ]
      },
      {
        "name": "delete",
        "actions": [
          {
            "delete": {}
          }
        ]
      }
    ]
  }
}'
echo

#
#  manually create token template
#
prepare-es-update-curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/token -u $ELASTICSEARCH_USERNAME:$ELASTICSEARCH_USERPASSWORD -H'Content-Type: application/json' -d'
{
  "index_patterns": ["detailedrecords_tokens-*"],
  "settings" : {
    "opendistro.index_state_management.policy_id": "token_policy"
  },
  "mappings" : {
    "properties" : {
      "subscriberID"  : { "type" : "keyword" },
      "tenantID"      : { "type" : "integer" },
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
prepare-es-update-curl -XPUT http://$MASTER_ESROUTER_SERVER/_ingest/pipeline/token-daily -u $ELASTICSEARCH_USERNAME:$ELASTICSEARCH_USERPASSWORD -H 'Content-Type: application/json' -d'
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
prepare-es-update-curl -XPUT http://$MASTER_ESROUTER_SERVER/_opendistro/_ism/policies/odr_policy -u $ELASTICSEARCH_USERNAME:$ELASTICSEARCH_USERPASSWORD -H'Content-Type: application/json' -d'
{
  "policy": {
    "description": "hot delete workflow for odr",
    "default_state": "hot",
    "schema_version": 1,
    "states": [
      {
        "name": "hot",
        "actions": [],
        "transitions": [
          {
            "state_name": "delete",
            "conditions": {
              "min_index_age": "Deployment.getElasticsearchRetentionDaysODR()d"
            }
          }
        ]
      },
      {
        "name": "delete",
        "actions": [
          {
            "delete": {}
          }
        ]
      }
    ]
  }
}'
echo

#
#  manually create odr template
#
prepare-es-update-curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/odr -u $ELASTICSEARCH_USERNAME:$ELASTICSEARCH_USERPASSWORD -H'Content-Type: application/json' -d'
{
  "index_patterns": ["detailedrecords_offers-*"],
  "settings" : {
    "opendistro.index_state_management.policy_id": "odr_policy"
  },
  "mappings" : {
    "properties" : {
      "subscriberID" : { "type" : "keyword" },
      "tenantID" : { "type" : "integer" },
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
prepare-es-update-curl -XPUT http://$MASTER_ESROUTER_SERVER/_ingest/pipeline/odr-daily -u $ELASTICSEARCH_USERNAME:$ELASTICSEARCH_USERPASSWORD -H 'Content-Type: application/json' -d'
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
#  create a cleaning policy for VDR
#
prepare-es-update-curl -XPUT http://$MASTER_ESROUTER_SERVER/_opendistro/_ism/policies/vdr_policy -u $ELASTICSEARCH_USERNAME:$ELASTICSEARCH_USERPASSWORD -H'Content-Type: application/json' -d'
{
  "policy": {
    "description": "hot delete workflow for vdr",
    "default_state": "hot",
    "schema_version": 1,
    "states": [
      {
        "name": "hot",
        "actions": [],
        "transitions": [
          {
            "state_name": "delete",
            "conditions": {
              "min_index_age": "Deployment.getElasticsearchRetentionDaysVDR()d"
            }
          }
        ]
      },
      {
        "name": "delete",
        "actions": [
          {
            "delete": {}
          }
        ]
      }
    ]
  }
}'
echo

#
#  manually create vdr template
#
prepare-es-update-curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/vdr -u $ELASTICSEARCH_USERNAME:$ELASTICSEARCH_USERPASSWORD -H'Content-Type: application/json' -d'
{
  "index_patterns": ["detailedrecords_vouchers-*"],
  "settings" : {
    "opendistro.index_state_management.policy_id": "vdr_policy"
  },
  "mappings" : {
    "properties" : {
      "subscriberID" : { "type" : "keyword" },
      "tenantID" : { "type" : "integer" },
      "eventDatetime" : { "type" : "date", "format":"yyyy-MM-dd HH:mm:ss.SSSZZ"},
      "eventID" : { "type" : "keyword" },     
      "moduleID" : { "type" : "keyword" },
      "featureID" : { "type" : "keyword" },
      "origin" : { "type" : "keyword", "index" : "false" },
      "returnStatus" : { "type" : "keyword" },
      "voucherCode" : { "type" : "keyword" },
      "voucherID" : { "type" : "keyword" },
      "opertaion" : { "type" : "keyword" },
      "expiryDate" : { "type" : "keyword" }
    }
  }
}'
echo

#
#  manually create vdr pipeline
#
prepare-es-update-curl -XPUT http://$MASTER_ESROUTER_SERVER/_ingest/pipeline/vdr-daily -u $ELASTICSEARCH_USERNAME:$ELASTICSEARCH_USERPASSWORD -H 'Content-Type: application/json' -d'
{
  "description": "daily vdr index naming",
  "processors" : [
    {
      "date_index_name" : {
        "field" : "eventDatetime",
        "index_name_prefix" : "detailedrecords_vouchers-",
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
prepare-es-update-curl -XPUT http://$MASTER_ESROUTER_SERVER/_opendistro/_ism/policies/mdr_policy -u $ELASTICSEARCH_USERNAME:$ELASTICSEARCH_USERPASSWORD -H'Content-Type: application/json' -d'
{
  "policy": {
    "description": "hot delete workflow for mdr",
    "default_state": "hot",
    "schema_version": 1,
    "states": [
      {
        "name": "hot",
        "actions": [],
        "transitions": [
          {
            "state_name": "delete",
            "conditions": {
              "min_index_age": "Deployment.getElasticsearchRetentionDaysMDR()d"
            }
          }
        ]
      },
      {
        "name": "delete",
        "actions": [
          {
            "delete": {}
          }
        ]
      }
    ]
  }
}'
echo

#
#  manually create mdr template
#
prepare-es-update-curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/mdr -u $ELASTICSEARCH_USERNAME:$ELASTICSEARCH_USERPASSWORD -H'Content-Type: application/json' -d'
{
  "index_patterns": ["detailedrecords_messages-*"],
  "settings" : {
    "opendistro.index_state_management.policy_id": "mdr_policy"
  },
  "mappings" : {
    "properties" : {
      "subscriberID" : { "type" : "keyword" },
      "tenantID" : { "type" : "integer" },
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
      "returnCodeDetails" : { "type" : "keyword", "index" : "false" },
      "contactType" : { "type" : "keyword" }
    }
  }
}'
echo

#
#  manually create mdr pipeline
#
prepare-es-update-curl -XPUT http://$MASTER_ESROUTER_SERVER/_ingest/pipeline/mdr-daily -u $ELASTICSEARCH_USERNAME:$ELASTICSEARCH_USERPASSWORD -H 'Content-Type: application/json' -d'
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
prepare-es-update-curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/journeystatistic -u $ELASTICSEARCH_USERNAME:$ELASTICSEARCH_USERPASSWORD -H'Content-Type: application/json' -d'
{
  "index_patterns": ["journeystatistic*"],
  "mappings" : {
    "properties" : {
      "journeyInstanceID" : { "type" : "keyword" },
      "journeyID" : { "type" : "keyword" },
      "subscriberID" : { "type" : "keyword" },
      "tenantID" : { "type" : "integer" },
      "transitionDate" : { "type" : "date" },
      "nodeID" : { "type" : "keyword" },
      "nodeHistory" : { "type" : "keyword" },
      "statusHistory" : { "type" : "keyword" },
      "rewardHistory" : { "type" : "keyword" },
      "deliveryRequestID" : { "type" : "keyword" },
      "sample" : { "type" : "keyword" },
      "statusNotified" : { "type" : "boolean" },
      "statusConverted" : { "type" : "boolean" },
      "conversionCount" : { "type" : "long" },
      "lastConversionDate" : { "type" : "date", "format":"yyyy-MM-dd HH:mm:ss.SSSZZ" },
      "statusTargetGroup" : { "type" : "boolean" },
      "statusControlGroup" : { "type" : "boolean" },
      "statusUniversalControlGroup" : { "type" : "boolean" },
      "journeyComplete" : { "type" : "boolean" },
      "journeyExitDate" : { "type" : "date", "format":"yyyy-MM-dd HH:mm:ss.SSSZZ" }
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
  prepare-es-update-curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/regr_criteria -u $ELASTICSEARCH_USERNAME:$ELASTICSEARCH_USERPASSWORD -H'Content-Type: application/json' -d'
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
  prepare-es-update-curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/regr_counter -u $ELASTICSEARCH_USERNAME:$ELASTICSEARCH_USERPASSWORD -H'Content-Type: application/json' -d'
  {
    "index_patterns": ["regr_counter"],
    "mappings" : {
      "properties" : {
        "count" : { "type" : "long" }
      }
    }
  }'

  prepare-es-update-curl -XPUT http://$MASTER_ESROUTER_SERVER/regr_counter/_doc/1 -u $ELASTICSEARCH_USERNAME:$ELASTICSEARCH_USERPASSWORD -H'Content-Type: application/json' -d'
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
prepare-es-update-curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/datacube_subscriberprofile -u $ELASTICSEARCH_USERNAME:$ELASTICSEARCH_USERPASSWORD -H'Content-Type: application/json' -d'
{
  "index_patterns": ["t*_datacube_subscriberprofile"],
  "mappings" : {
    "properties" : {
      "timestamp" : { "type" : "date", "format":"yyyy-MM-dd HH:mm:ss.SSSZZ" },
      "period" : { "type" : "long" },
      "filter.tenantID" : { "type" : "integer" },
      "count" : { "type" : "integer" }
    }
  }
}'
echo

prepare-es-update-curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/datacube_loyaltyprogramshistory -u $ELASTICSEARCH_USERNAME:$ELASTICSEARCH_USERPASSWORD -H'Content-Type: application/json' -d'
{
  "index_patterns": ["t*_datacube_loyaltyprogramshistory"],
  "mappings" : {
    "properties" : {
      "timestamp" : { "type" : "date", "format":"yyyy-MM-dd HH:mm:ss.SSSZZ" },
      "period" : { "type" : "long" },
      "filter.tenantID" : { "type" : "integer" },
      "filter.loyaltyProgram" : { "type" : "keyword" },
      "filter.tier" : { "type" : "keyword" },
      "filter.evolutionSubscriberStatus" : { "type" : "keyword" },
      "filter.redeemer" : { "type" : "boolean" },
      "count" : { "type" : "integer" },
      "metric.rewards.redeemed" : { "type" : "integer" },
      "metric.rewards.earned" : { "type" : "integer" },
      "metric.rewards.expired" : { "type" : "integer" },
      "metric.rewards.redemptions" : { "type" : "integer" }
    }
  }
}'
echo

prepare-es-update-curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/datacube_loyaltyprogramschanges -u $ELASTICSEARCH_USERNAME:$ELASTICSEARCH_USERPASSWORD -H'Content-Type: application/json' -d'
{
  "index_patterns": ["t*_datacube_loyaltyprogramschanges"],
  "mappings" : {
    "properties" : {
      "timestamp" : { "type" : "date", "format":"yyyy-MM-dd HH:mm:ss.SSSZZ" },
      "period" : { "type" : "long" },
      "filter.tenantID" : { "type" : "integer" },
      "filter.loyaltyProgram" : { "type" : "keyword" },
      "filter.newTier" : { "type" : "keyword" },
      "filter.previousTier" : { "type" : "keyword" },
      "filter.tierChangeType" : { "type" : "keyword" },
      "count" : { "type" : "integer" }
    }
  }
}'
echo

prepare-es-update-curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/datacube_journeytraffic- -u $ELASTICSEARCH_USERNAME:$ELASTICSEARCH_USERPASSWORD -H'Content-Type: application/json' -d'
{
  "index_patterns": ["t*_datacube_journeytraffic-*"],
  "mappings" : {
    "properties" : {
      "timestamp" : { "type" : "date", "format":"yyyy-MM-dd HH:mm:ss.SSSZZ" },
      "period" : { "type" : "long" },
      "filter.tenantID" : { "type" : "integer" },
      "filter.journey" : { "type" : "keyword" },
      "filter.node" : { "type" : "keyword" },
      "filter.status" : { "type" : "keyword" },
      "count" : { "type" : "integer" },
      "metric.conversions" : { "type" : "integer" },
      "metric.converted.today" : { "type" : "integer" }
    }
  }
}'
echo

prepare-es-update-curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/datacube_journeyrewards- -u $ELASTICSEARCH_USERNAME:$ELASTICSEARCH_USERPASSWORD -H'Content-Type: application/json' -d'
{
  "index_patterns": ["t*_datacube_journeyrewards-*"],
  "mappings" : {
    "properties" : {
      "timestamp" : { "type" : "date", "format":"yyyy-MM-dd HH:mm:ss.SSSZZ" },
      "period" : { "type" : "long" },
      "filter.tenantID" : { "type" : "integer" },
      "filter.journey" : { "type" : "keyword" },
      "count" : { "type" : "integer" }
    }
  }
}'
echo

prepare-es-update-curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/datacube_odr -u $ELASTICSEARCH_USERNAME:$ELASTICSEARCH_USERPASSWORD -H'Content-Type: application/json' -d'
{
  "index_patterns": ["t*_datacube_odr"],
  "mappings" : {
    "properties" : {
      "timestamp" : { "type" : "date", "format":"yyyy-MM-dd HH:mm:ss.SSSZZ" },
      "period" : { "type" : "long" },
      "filter.tenantID" : { "type" : "integer" },
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

prepare-es-update-curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/datacube_bdr -u $ELASTICSEARCH_USERNAME:$ELASTICSEARCH_USERPASSWORD -H'Content-Type: application/json' -d'
{
  "index_patterns": ["t*_datacube_bdr"],
  "mappings" : {
    "properties" : {
      "timestamp" : { "type" : "date", "format":"yyyy-MM-dd HH:mm:ss.SSSZZ" },
      "period" : { "type" : "long" },
      "filter.tenantID" : { "type" : "integer" },
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

prepare-es-update-curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/datacube_messages -u $ELASTICSEARCH_USERNAME:$ELASTICSEARCH_USERPASSWORD -H'Content-Type: application/json' -d'
{
  "index_patterns": ["t*_datacube_messages"],
  "mappings" : {
    "properties" : {
      "timestamp" : { "type" : "date", "format":"yyyy-MM-dd HH:mm:ss.SSSZZ" },
      "period" : { "type" : "long" },
      "filter.tenantID" : { "type" : "integer" },
      "filter.module" : { "type" : "keyword" },
      "filter.feature" : { "type" : "keyword" },
      "filter.provider" : { "type" : "keyword" },
      "filter.language" : { "type" : "keyword" },
      "filter.template" : { "type" : "keyword" },
      "filter.channel" : { "type" : "keyword" },
      "filter.contactType" : { "type" : "keyword" },
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
prepare-es-update-curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/mapping_modules -u $ELASTICSEARCH_USERNAME:$ELASTICSEARCH_USERPASSWORD -H'Content-Type: application/json' -d'
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

prepare-es-update-curl -XPUT http://$MASTER_ESROUTER_SERVER/mapping_modules/_doc/1 -u $ELASTICSEARCH_USERNAME:$ELASTICSEARCH_USERPASSWORD -H'Content-Type: application/json' -d'
{
  "moduleID" : "1", "moduleName": "Journey_Manager", "moduleDisplay" : "Journey Manager", "moduleFeature" : "journeyID"
}'
echo

prepare-es-update-curl -XPUT http://$MASTER_ESROUTER_SERVER/mapping_modules/_doc/2 -u $ELASTICSEARCH_USERNAME:$ELASTICSEARCH_USERPASSWORD -H'Content-Type: application/json' -d'
{
  "moduleID" : "2", "moduleName": "Loyalty_Program", "moduleDisplay" : "Loyalty Program", "moduleFeature" : "loyaltyProgramID"
}'
echo

prepare-es-update-curl -XPUT http://$MASTER_ESROUTER_SERVER/mapping_modules/_doc/3 -u $ELASTICSEARCH_USERNAME:$ELASTICSEARCH_USERPASSWORD -H'Content-Type: application/json' -d'
{
  "moduleID" : "3", "moduleName": "Offer_Catalog", "moduleDisplay" : "Offer Catalog", "moduleFeature" : "offerID"
}'
echo

prepare-es-update-curl -XPUT http://$MASTER_ESROUTER_SERVER/mapping_modules/_doc/4 -u $ELASTICSEARCH_USERNAME:$ELASTICSEARCH_USERPASSWORD -H'Content-Type: application/json' -d'
{
  "moduleID" : "4", "moduleName": "Delivery_Manager", "moduleDisplay" : "Delivery Manager", "moduleFeature" : "deliverableID"
}'
echo

prepare-es-update-curl -XPUT http://$MASTER_ESROUTER_SERVER/mapping_modules/_doc/5 -u $ELASTICSEARCH_USERNAME:$ELASTICSEARCH_USERPASSWORD -H'Content-Type: application/json' -d'
{
  "moduleID" : "5", "moduleName": "Customer_Care", "moduleDisplay" : "Customer Care", "moduleFeature" : "none"
}'
echo

prepare-es-update-curl -XPUT http://$MASTER_ESROUTER_SERVER/mapping_modules/_doc/6 -u $ELASTICSEARCH_USERNAME:$ELASTICSEARCH_USERPASSWORD -H'Content-Type: application/json' -d'
{
  "moduleID" : "6", "moduleName": "REST_API", "moduleDisplay" : "REST API", "moduleFeature" : "none"
}'
echo

prepare-es-update-curl -XPUT http://$MASTER_ESROUTER_SERVER/mapping_modules/_doc/999 -u $ELASTICSEARCH_USERNAME:$ELASTICSEARCH_USERPASSWORD -H'Content-Type: application/json' -d'
{
  "moduleID" : "999", "moduleName": "Unknown", "moduleDisplay" : "Unknown", "moduleFeature" : "none"
}'
echo

prepare-es-update-curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/mapping_journeys -u $ELASTICSEARCH_USERNAME:$ELASTICSEARCH_USERPASSWORD -H'Content-Type: application/json' -d'
{
  "index_patterns": ["mapping_journeys"],
  "mappings" : {
    "properties" : {
      "journeyID" : { "type" : "keyword" },
      "tenantID" : { "type" : "integer" },
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

prepare-es-update-curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/mapping_journeyrewards -u $ELASTICSEARCH_USERNAME:$ELASTICSEARCH_USERPASSWORD -H'Content-Type: application/json' -d'
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

prepare-es-update-curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/mapping_deliverables -u $ELASTICSEARCH_USERNAME:$ELASTICSEARCH_USERPASSWORD -H'Content-Type: application/json' -d'
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

prepare-es-update-curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/mapping_basemanagement -u $ELASTICSEARCH_USERNAME:$ELASTICSEARCH_USERPASSWORD -H'Content-Type: application/json' -d'
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
      "timestamp"     : { "type" : "date", "format":"yyyy-MM-dd HH:mm:ss.SSSZZ" }
    }
  }
}'
echo

prepare-es-update-curl -XPUT http://$MASTER_ESROUTER_SERVER/_template/mapping_journeyobjective -u $ELASTICSEARCH_USERNAME:$ELASTICSEARCH_USERPASSWORD -H'Content-Type: application/json' -d'
{
  "index_patterns": ["mapping_journeyobjective"],
  "mappings" : {
    "properties" : {
      "id"            : { "type" : "keyword" },
      "display"       : { "type" : "keyword" },
      "contactPolicy" : { "type" : "keyword" },
      "timestamp"     : { "type" : "date", "format":"yyyy-MM-dd HH:mm:ss.SSSZZ" }
    }
  }
}'
echo

