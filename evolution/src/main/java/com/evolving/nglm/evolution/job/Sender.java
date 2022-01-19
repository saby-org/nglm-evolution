package com.evolving.nglm.evolution.job;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.StringKey;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.GUIManager;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Sender {

	private static final Logger log = LoggerFactory.getLogger(Sender.class);

	public static String sendJob(String processID, Integer tenantID, JSONObject json) throws GUIManager.GUIManagerException{
		EvolutionJob evolutionJob = new EvolutionJob(processID,tenantID,json);
		send(evolutionJob);
		return evolutionJob.getJobID();
	}

	public static void send(EvolutionJob evolutionJob){
		evolutionJob.setUpdateTime(SystemTime.getCurrentTime());
		String topic = Deployment.getEvolutionJobTopic();
		byte[] key = StringKey.serde().serializer().serialize(topic,new StringKey(evolutionJob.getJobID()));
		byte[] value = EvolutionJob.serde().serializer().serialize(topic,evolutionJob);
		KafkaProducer<byte[],byte[]> producer = createProducer();
		try {
			producer.send(new ProducerRecord<>(topic,key,value)).get(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
		} catch (InterruptedException|ExecutionException|TimeoutException e) {
			log.warn("sendJob issue while sending "+evolutionJob, e);
		}
		producer.close();
	}

	private static KafkaProducer<byte[],byte[]> createProducer(){
		Properties producerConfig = new Properties();
		producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Deployment.getBrokerServers());
		producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
		producerConfig.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,120000);
		producerConfig.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,Integer.MAX_VALUE);
		producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
		producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
		return new KafkaProducer<>(producerConfig);
	}
}
