package com.evolving.nglm.evolution.kafka;

import com.evolving.nglm.evolution.Deployment;
import org.apache.kafka.common.config.TopicConfig;

class TopicConfigurationRetention extends TopicConfiguration{

	// different used conf
	final static TopicConfigurationRetention TRAFFIC_SHORT = new TopicConfigurationRetention(Deployment.getTopicSubscriberPartitions(),Deployment.getTopicReplication(),Deployment.getTopicMinInSyncReplicas(),TopicConfiguration._50MB,Deployment.getTopicRetentionShortMs());
	final static TopicConfigurationRetention TRAFFIC = new TopicConfigurationRetention(Deployment.getTopicSubscriberPartitions(),Deployment.getTopicReplication(),Deployment.getTopicMinInSyncReplicas(),TopicConfiguration._50MB,Deployment.getTopicRetentionMs());
	final static TopicConfigurationRetention TRAFFIC_LONG = new TopicConfigurationRetention(Deployment.getTopicSubscriberPartitions(),Deployment.getTopicReplication(),Deployment.getTopicMinInSyncReplicas(),TopicConfiguration._50MB,Deployment.getTopicRetentionLongMs());

	TopicConfigurationRetention(int partitions, int replication, String minInsyncReplicas, String segmentBytes, String retentionMs){
		super(partitions,(short)replication,minInsyncReplicas, TopicConfig.CLEANUP_POLICY_DELETE,segmentBytes);
		addConfig(TopicConfig.RETENTION_MS_CONFIG,retentionMs);
	}
}