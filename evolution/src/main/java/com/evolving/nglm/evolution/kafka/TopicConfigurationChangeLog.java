package com.evolving.nglm.evolution.kafka;

import org.apache.kafka.common.config.TopicConfig;

import com.evolving.nglm.core.Deployment;

class TopicConfigurationChangeLog extends TopicConfiguration{

	// different used conf
	final static TopicConfigurationChangeLog SUBSCRIBER_STATESTORE = new TopicConfigurationChangeLog(Deployment.getTopicSubscriberPartitions(),Deployment.getTopicReplication(),Deployment.getTopicMinInSyncReplicas(),_50MB,_20min,_1hour,"0");
	final static TopicConfigurationChangeLog CONFIGURATION = new TopicConfigurationChangeLog(1,Deployment.getTopicReplication(),Deployment.getTopicMinInSyncReplicas(),TopicConfiguration._100KB,_20min,_1hour,"0");

	TopicConfigurationChangeLog(int partitions, int replication, String minInsyncReplicas, String segmentBytes, String minCompactionLagMs, String maxCompactionLagMs, String deleteRetentionMs){
		super(partitions,(short)replication,minInsyncReplicas, TopicConfig.CLEANUP_POLICY_COMPACT,segmentBytes);
		if(minCompactionLagMs!=null) addConfig(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG,minCompactionLagMs);
		if(maxCompactionLagMs!=null) addConfig(TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG,maxCompactionLagMs);
		if(deleteRetentionMs!=null) addConfig(TopicConfig.DELETE_RETENTION_MS_CONFIG,deleteRetentionMs);
	}
}