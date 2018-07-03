  #################################################################################
  #
  #  deployment-setup-elasticsearch
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
                  "subscriptionDate" : { "type" : "date" },
                  "contractID" : { "type" : "keyword" },
                  "accountTypeID" : { "type" : "keyword" },
                  "ratePlan" : { "type" : "keyword" },
                  "ratePlanChangeDate" : { "type" : "date" },
                  "subscriberStatus" : { "type" : "keyword" },
                  "previousSubscriberStatus" : { "type" : "keyword" },
                  "statusChangeDate" : { "type" : "date" },
                  "universalControlGroup" : { "type" : "boolean" },
                  "controlGroup" : { "type" : "boolean" },
                  "language" : { "type" : "keyword" },
                  "region" : { "type" : "keyword" },
                  "subscriberGroups" : { "type" : "keyword" },
                  "totalChargeYesterday" : { "type" : "integer" },
                  "totalChargePrevious7Days" : { "type" : "integer" },
                  "totalChargePrevious14Days" : { "type" : "integer" },
                  "totalChargePreviousMonth" : { "type" : "integer" },
                  "rechargeCountYesterday" : { "type" : "integer" },
                  "rechargeCountPrevious7Days" : { "type" : "integer" },
                  "rechargeCountPrevious14Days" : { "type" : "integer" },
                  "rechargeCountPreviousMonth" : { "type" : "integer" },
                  "rechargeChargeYesterday" : { "type" : "integer" },
                  "rechargeChargePrevious7Days" : { "type" : "integer" },
                  "rechargeChargePrevious14Days" : { "type" : "integer" },
                  "rechargeChargePreviousMonth" : { "type" : "integer" },		  
                  "lastRechargeDate" : { "type" : "date" },
                  "mainBalanceValue" : { "type" : "integer" },		  
                  "moCallChargeYesterday" : { "type" : "integer" },
                  "moCallChargePrevious7Days" : { "type" : "integer" },
                  "moCallChargePrevious14Days" : { "type" : "integer" },
                  "moCallChargePreviousMonth" : { "type" : "integer" },		  
                  "moCallCountYesterday" : { "type" : "integer" },
                  "moCallCountPrevious7Days" : { "type" : "integer" },
                  "moCallCountPrevious14Days" : { "type" : "integer" },
                  "moCallCountPreviousMonth" : { "type" : "integer" },		  
                  "moCallDurationYesterday" : { "type" : "integer" },
                  "moCallDurationPrevious7Days" : { "type" : "integer" },
                  "moCallDurationPrevious14Days" : { "type" : "integer" },
                  "moCallDurationPreviousMonth" : { "type" : "integer" },		  
                  "mtCallCountYesterday" : { "type" : "integer" },		  
                  "mtCallCountPrevious7Days" : { "type" : "integer" },
                  "mtCallCountPrevious14Days" : { "type" : "integer" },
                  "mtCallCountPreviousMonth" : { "type" : "integer" },		  
                  "mtCallDurationYesterday" : { "type" : "integer" },		  
                  "mtCallDurationPrevious7Days" : { "type" : "integer" },
                  "mtCallDurationPrevious14Days" : { "type" : "integer" },
                  "mtCallDurationPreviousMonth" : { "type" : "integer" },		  
                  "mtCallIntCountYesterday" : { "type" : "integer" },
                  "mtCallIntCountPrevious7Days" : { "type" : "integer" },
                  "mtCallIntCountPrevious14Days" : { "type" : "integer" },
                  "mtCallIntCountPreviousMonth" : { "type" : "integer" },		  
                  "mtCallIntDurationYesterday" : { "type" : "integer" },
                  "mtCallIntDurationPrevious7Days" : { "type" : "integer" },
                  "mtCallIntDurationPrevious14Days" : { "type" : "integer" },
                  "mtCallIntDurationPreviousMonth" : { "type" : "integer" },		  
                  "moCallIntChargeYesterday" : { "type" : "integer" },
                  "moCallIntChargePrevious7Days" : { "type" : "integer" },
                  "moCallIntChargePrevious14Days" : { "type" : "integer" },
                  "moCallIntChargePreviousMonth" : { "type" : "integer" },		  
                  "moCallIntCountYesterday" : { "type" : "integer" },
                  "moCallIntCountPrevious7Days" : { "type" : "integer" },
                  "moCallIntCountPrevious14Days" : { "type" : "integer" },
                  "moCallIntCountPreviousMonth" : { "type" : "integer" },		  
                  "moCallIntDurationYesterday" : { "type" : "integer" },
                  "moCallIntDurationPrevious7Days" : { "type" : "integer" },
                  "moCallIntDurationPrevious14Days" : { "type" : "integer" },
                  "moCallIntDurationPreviousMonth" : { "type" : "integer" },		  
                  "moSMSChargeYesterday" : { "type" : "integer" },
                  "moSMSChargePrevious7Days" : { "type" : "integer" },
                  "moSMSChargePrevious14Days" : { "type" : "integer" },
                  "moSMSChargePreviousMonth" : { "type" : "integer" },		  
                  "moSMSCountYesterday" : { "type" : "integer" },
                  "moSMSCountPrevious7Days" : { "type" : "integer" },
                  "moSMSCountPrevious14Days" : { "type" : "integer" },
                  "moSMSCountPreviousMonth" : { "type" : "integer" },		  
                  "dataVolumeYesterday" : { "type" : "integer" },
                  "dataVolumePrevious7Days" : { "type" : "integer" },
                  "dataVolumePrevious14Days" : { "type" : "integer" },
                  "dataVolumePreviousMonth" : { "type" : "integer" },		  
                  "dataBundleChargeYesterday" : { "type" : "integer" },
                  "dataBundleChargePrevious7Days" : { "type" : "integer" },
                  "dataBundleChargePrevious14Days" : { "type" : "integer" },
                  "dataBundleChargePreviousMonth" : { "type" : "integer" }
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
                  "exited" : { "type" : "boolean" }
                }
            }
        }
    }'
  echo
  