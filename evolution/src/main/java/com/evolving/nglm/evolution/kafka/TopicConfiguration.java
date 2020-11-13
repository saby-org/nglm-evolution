package com.evolving.nglm.evolution.kafka;

import com.evolving.nglm.evolution.Deployment;
import org.apache.kafka.common.config.TopicConfig;

import java.util.HashMap;
import java.util.Map;

class TopicConfiguration{

	// bytes conf
	final static String _100KB = ""+(100 * 1024);
	final static String _50MB = ""+(50 * 1024 * 1024);
	// ms conf
	final static String _20min = ""+(20 * 60 * 1000);
	final static String _1hour = ""+(60 * 60 * 1000);

	int partitions;
	short replication;
	Map<String,String> configs;

	TopicConfiguration(int partitions, short replication, String minInsyncReplicas, String cleanupPolicy, String segmentBytes){
		this.partitions=partitions;
		this.replication=replication;
		this.configs=new HashMap<>();
		if(minInsyncReplicas!=null) addConfig(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG,minInsyncReplicas);
		addConfig(TopicConfig.CLEANUP_POLICY_CONFIG,cleanupPolicy);
		if(segmentBytes!=null) addConfig(TopicConfig.SEGMENT_BYTES_CONFIG,segmentBytes);
	}
	void addConfig(String name, String value){configs.put(name,value);}
}
