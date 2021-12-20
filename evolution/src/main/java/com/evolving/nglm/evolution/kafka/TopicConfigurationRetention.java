package com.evolving.nglm.evolution.kafka;

import org.apache.kafka.common.config.TopicConfig;

import com.evolving.nglm.core.Deployment;

class TopicConfigurationRetention extends TopicConfiguration{

	// different used conf
	final static TopicConfigurationRetention TRAFFIC_SHORT = new TopicConfigurationRetention(Deployment.getTopicSubscriberPartitions(),Deployment.getTopicReplication(),Deployment.getTopicMinInSyncReplicas(),Deployment.getTopicRetentionShortMs());
	final static TopicConfigurationRetention TRAFFIC = new TopicConfigurationRetention(Deployment.getTopicSubscriberPartitions(),Deployment.getTopicReplication(),Deployment.getTopicMinInSyncReplicas(),Deployment.getTopicRetentionMs());
	final static TopicConfigurationRetention TRAFFIC_LONG = new TopicConfigurationRetention(Deployment.getTopicSubscriberPartitions(),Deployment.getTopicReplication(),Deployment.getTopicMinInSyncReplicas(),Deployment.getTopicRetentionLongMs());

	TopicConfigurationRetention(int partitions, int replication, String minInsyncReplicas, String retentionMs){
		super(partitions,(short)replication,minInsyncReplicas, TopicConfig.CLEANUP_POLICY_DELETE);
		addConfig(TopicConfig.RETENTION_MS_CONFIG,retentionMs);
	}
}