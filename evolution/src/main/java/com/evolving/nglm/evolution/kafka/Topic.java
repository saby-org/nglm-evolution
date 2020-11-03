package com.evolving.nglm.evolution.kafka;

import org.apache.kafka.clients.admin.NewTopic;

public class Topic {

	public enum TYPE{
		trafficLowRetention(TopicConfigurationRetention.TRAFFIC_SHORT),
		traffic(TopicConfigurationRetention.TRAFFIC),
		trafficLongRetention(TopicConfigurationRetention.TRAFFIC_LONG),
		configuration(TopicConfigurationChangeLog.CONFIGURATION),
		subscriberStateStore(TopicConfigurationChangeLog.SUBSCRIBER_STATESTORE);

		TopicConfiguration config;
		TYPE(TopicConfiguration config){this.config=config;}
	}

	private String name;
	private TYPE type;
	private boolean autoCreated;

	public Topic(String name,TYPE type, boolean autoCreated){
		this.name=name;
		this.type=type;
		this.autoCreated=autoCreated;
	}

	public String getName(){return name;}
	public boolean isAutoCreated(){return autoCreated;}
	public NewTopic getNewTopic(){
		NewTopic toRet = new NewTopic(name,type.config.partitions,type.config.replication);
		toRet.configs(type.config.configs);
		return toRet;
	}

}
