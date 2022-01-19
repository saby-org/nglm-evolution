package com.evolving.nglm.evolution.job;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.StringKey;
import com.evolving.nglm.core.SubscriberStreamOutput;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.EvolutionEngine;
import com.evolving.nglm.evolution.SubscriberState;
import com.evolving.nglm.evolution.TimedEvaluation;
import com.evolving.nglm.evolution.event.SubscriberUpdated;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class WorkerEvolutionEngineJobs extends Worker{

	private static final Logger log = LoggerFactory.getLogger(WorkerEvolutionEngineJobs.class);

	private final EvolutionEngine evolutionEngine;

	public WorkerEvolutionEngineJobs(EvolutionEngine evolutionEngine){
		super("evolution-engine-"+evolutionEngine.getEvolutionEngineKey(),1, EvolutionJob.Name.pushAlternateIDsToRedis, EvolutionJob.Name.triggerMicroscope);
		this.evolutionEngine = evolutionEngine;
	}

	private static final int ALL_PARTITIONS = -1;//internal convention
	private ReadOnlyKeyValueStore<StringKey,SubscriberState> getSubscriberStateStore(){ return getSubscriberStateStoreForPartition(ALL_PARTITIONS);}
	// return null if the local engine does not handle partition asked for
	private ReadOnlyKeyValueStore<StringKey,SubscriberState> getSubscriberStateStoreForPartition(int partition){
		if(log.isTraceEnabled()) log.trace(this.getClass().getName()+" getSubscriberStateStoreForPartition "+partition+" called");
		while(true){
			try{
				if(partition!=ALL_PARTITIONS && !getHandledPartitions().contains(partition)){
					log.info(this.getClass().getName()+" evolution engine "+evolutionEngine.getEvolutionEngineKey()+" does not handle partition "+partition+" anymore");
					return null;
				}
				StoreQueryParameters<ReadOnlyKeyValueStore<StringKey, SubscriberState>> params = StoreQueryParameters.fromNameAndType(Deployment.getSubscriberStateChangeLog(), QueryableStoreTypes.keyValueStore());
				if(partition!=ALL_PARTITIONS) params = params.withPartition(partition);
				ReadOnlyKeyValueStore<StringKey,SubscriberState> store = this.evolutionEngine.getKafkaStreams().store(params);
				if(log.isTraceEnabled()) log.trace(this.getClass().getName()+" getSubscriberStateStoreForPartition "+partition+" return store "+store.approximateNumEntries());
				return store;
			}catch (InvalidStateStoreException e){
				log.info(this.getClass().getName()+" getSubscriberStateStoreForPartition "+e.getMessage()+" retrying it");
			}
		}
	}

	private int getTotalPartitions(){return Deployment.getTopicSubscriberPartitions();}
	private Set<Integer> getHandledPartitions(){
		if(log.isTraceEnabled()) log.trace(this.getClass().getName()+" getHandledPartitions called");
		while(true){
			evolutionEngine.waitForStreams();
			Set<Integer> workerParts = new HashSet<>();
			try{
				evolutionEngine.getKafkaStreams().localThreadsMetadata().forEach(threadMetadata -> {
					threadMetadata.activeTasks().forEach(activeTask->{
						activeTask.topicPartitions().forEach(topicPartition -> {
							// use one input product topic to get partitions we handle
							if(topicPartition.topic().equals(Deployment.getProductExternalEventRequestTopic())){
								workerParts.add(topicPartition.partition());
							}
						});
					});
				});
				if(log.isTraceEnabled()) log.trace(this.getClass().getName()+" getHandledPartitions return "+workerParts);
				return workerParts;
			}catch (IllegalStateException e){
				log.info(this.getClass().getName()+" getHandledPartitions "+e.getMessage()+" retrying it");
			}
		}
	}

	@Override protected int getTotalParts(EvolutionJob job){return this.getTotalPartitions();}
	@Override protected Set<Integer> getWorkerParts(EvolutionJob job){return this.getHandledPartitions();}

	@Override
	protected EvolutionJob.Status processPart(EvolutionJob job, int part) {
		while(true){
			try{
				ReadOnlyKeyValueStore<StringKey,SubscriberState> partitionStore = this.getSubscriberStateStoreForPartition(part);
				if(partitionStore==null){
					log.info(this.getClass().getName()+" evolution engine "+evolutionEngine.getEvolutionEngineKey()+" does not handle partition "+part+" anymore, skipping");
					return EvolutionJob.Status.rebalanced;
				}
				KeyValueIterator<StringKey,SubscriberState> iterator = partitionStore.all();
				while(iterator.hasNext()){
					KeyValue<StringKey,SubscriberState> next = iterator.next();
					if(next==null){
						log.info(this.getClass().getName()+" next is null skipping");
						continue;
					}
					if(next.key==null) continue;
					String key = next.key.getKey();
					if(next.value==null){
						log.info(this.getClass().getName()+" null value for "+key+" skipping");
						continue;
					}
					SubscriberState subscriberState = next.value;
					if(job.getTenantID()==0 || subscriberState.getSubscriberProfile().getTenantID()==job.getTenantID()) doJob(job.getName(),subscriberState);
				}
				// part done
				return EvolutionJob.Status.processed;
			}catch (InvalidStateStoreException e){
				log.info(this.getClass().getName()+" "+e.getMessage()+" will retry");
			}
		}
	}

	private void doJob(EvolutionJob.Name job, SubscriberState subscriberState){

		if(job==EvolutionJob.Name.pushAlternateIDsToRedis){
			SubscriberUpdated subscriberUpdated = new SubscriberUpdated(subscriberState.getSubscriberID(),subscriberState.getSubscriberProfile().getTenantID());
			SubscriberStreamOutput.getSubscriberAlternateIDValues(subscriberState.getSubscriberProfile(),evolutionEngine.getSubscriberGroupEpochReader())
					.entrySet().forEach(entry->{
						subscriberUpdated.addAlternateIDToAdd(entry.getKey(),entry.getValue());
					});
			String topic = Deployment.getProductExternalEventResponseTopic();
			send(job,topic,subscriberUpdated.getSubscriberID(),SubscriberUpdated.serde().serializer().serialize(topic,subscriberUpdated));
		}

		else if(job==EvolutionJob.Name.triggerMicroscope){
			TimedEvaluation timedEvaluation = new TimedEvaluation(subscriberState.getSubscriberID(), SystemTime.getCurrentTime(), EvolutionJob.Name.triggerMicroscope.getExternalRepresentation());
			String topic = Deployment.getTimedEvaluationTopic();
			send(job,topic,timedEvaluation.getSubscriberID(),TimedEvaluation.serde().serializer().serialize(topic,timedEvaluation));
		}

	}

	private void send(EvolutionJob.Name job, String topic, String key, byte[] value){
		ProducerRecord<byte[], byte[]> toSend = new ProducerRecord<>(topic, StringKey.serde().serializer().serialize(topic,new StringKey(key)), value);
		if(log.isInfoEnabled()){
			ProducerHolder.kafkaProducer.send(toSend,(recordMetadata,exception)->{
				if(exception==null) return;
				log.info("exception sending to topic "+topic+" for job "+job.getExternalRepresentation()+" for "+key,exception);
			});
		}else{
			ProducerHolder.kafkaProducer.send(toSend);
		}
	}

	private static class ProducerHolder{
		private final static KafkaProducer<byte[], byte[]> kafkaProducer;
		static {
			Properties producerProperties = new Properties();
			producerProperties.put("bootstrap.servers", Deployment.getBrokerServers());
			producerProperties.put("acks", "all");
			producerProperties.put("key.serializer", org.apache.kafka.common.serialization.ByteArraySerializer.class);
			producerProperties.put("value.serializer", org.apache.kafka.common.serialization.ByteArraySerializer.class);
			//under brokers load those default seems not OK (and async send might just hide errors)
			producerProperties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,120000);// instead of 30s previous
			producerProperties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,Integer.MAX_VALUE);// instead of 2 minutes before
			kafkaProducer = new KafkaProducer<>(producerProperties);
		}
	}

}
