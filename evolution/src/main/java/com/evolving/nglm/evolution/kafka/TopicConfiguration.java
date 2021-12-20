package com.evolving.nglm.evolution.kafka;

import org.apache.kafka.common.config.TopicConfig;

import java.util.HashMap;
import java.util.Map;

class TopicConfiguration{

	// ms conf
	final static String _1hour = ""+(1 * 3600 * 1000);
	final static String _6hours = ""+(6 * 3600 * 1000);

	int partitions;
	short replication;
	Map<String,String> configs;

	TopicConfiguration(int partitions, short replication, String minInsyncReplicas, String cleanupPolicy){
		this.partitions=partitions;
		this.replication=replication;
		this.configs=new HashMap<>();
		if(minInsyncReplicas!=null) addConfig(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG,minInsyncReplicas);
		addConfig(TopicConfig.CLEANUP_POLICY_CONFIG,cleanupPolicy);
	}
	void addConfig(String name, String value){configs.put(name,value);}
}
