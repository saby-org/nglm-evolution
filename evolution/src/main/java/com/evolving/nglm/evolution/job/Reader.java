package com.evolving.nglm.evolution.job;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.StringKey;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.backup.kafka.BackupTopic;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class Reader implements Runnable{

	private static final Logger log = LoggerFactory.getLogger(Reader.class);
	private static final Integer JOB_RETENTION_MS = 48 * 3600 * 1000;

	private final KafkaConsumer<byte[],byte[]> consumer;
	private final Thread jobReader;
	private final Map<String,Map<String,EvolutionJob>> allJobs = new ConcurrentHashMap<>();
	private final Map<String,EvolutionJob> pendingJobs = new ConcurrentHashMap<>();
	private final Map<String,EvolutionJob> terminatedJobs = new ConcurrentHashMap<>();
	private final AtomicBoolean hasLag = new AtomicBoolean(true);
	private final Map<EvolutionJob.Name,List<Worker>> listeners = new ConcurrentHashMap<>();

	private Reader(){
		// init the consumer
		Properties consumerProperties = new Properties();
		consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Deployment.getBrokerServers());
		consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,  "earliest");
		consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
		consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
		consumerProperties.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, 120000);
		consumerProperties.put(CommonClientConfigs.RETRIES_CONFIG, Integer.MAX_VALUE);
		consumerProperties.put(CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG, Integer.MAX_VALUE);
		consumer = new KafkaConsumer<byte[], byte[]>(consumerProperties);
		// init the reader thread
		jobReader = new Thread(this,"EvolutionJobReader");
		jobReader.setDaemon(true);
		jobReader.start();
	}


	@Override
	public void run() {

		// put at the beginning of the topic
		consumer.assign(BackupTopic.getTopicPartition(Deployment.getEvolutionJobTopic(),consumer));
		consumer.seekToBeginning(consumer.assignment());

		while(true){

			// read coming jobs
			Date minRetentionDate = new Date(SystemTime.currentTimeMillis()-JOB_RETENTION_MS);
			consumer.poll(Duration.ofSeconds(10)).forEach(record->{
				String jobID = StringKey.serde().deserializer().deserialize(Deployment.getEvolutionJobTopic(),record.key()).getKey();
				EvolutionJob job = EvolutionJob.serde().deserializer().deserialize(Deployment.getEvolutionJobTopic(),record.value());
				job.setJobID(jobID);
				Map<String,EvolutionJob> jobStatus = allJobs.get(jobID);
				if(job.getCreationTime().after(minRetentionDate)){
					if(jobStatus==null) jobStatus = new ConcurrentHashMap<>();
					jobStatus.put(job.getProcessID(),job);
					// check for jobs status
					EvolutionJob globalStatus = getGlobalStatus(jobStatus);
					// if not finished add to pending job
					if(!EvolutionJob.Status.terminatedStatuses.contains(globalStatus.getStatus())){
						pendingJobs.put(jobID,globalStatus);
					// else remove job, sum up result
					}else{
						pendingJobs.remove(jobID);
						jobStatus.clear();
						jobStatus.put(globalStatus.getProcessID(),globalStatus);
						terminatedJobs.put(jobID,globalStatus);
					}
					allJobs.put(jobID,jobStatus);
				}else if(jobStatus!=null){
					allJobs.remove(jobID);
				}
			});
			// check if we have lag or not on the job topic
			if(BackupTopic.doesConsumerReachedPosition(consumer,BackupTopic.getEndOffsets(consumer.assignment(),consumer))){
				this.hasLag.set(false);
				synchronized (hasLag){
					this.hasLag.notifyAll();
				}
			}else{
				this.hasLag.set(true);
			}
			// check for retention cleaning
			if(!hasLag.get()){
				Iterator<Map.Entry<String, Map<String,EvolutionJob>>> iterator = allJobs.entrySet().iterator();
				while(iterator.hasNext()){
					Map.Entry<String, Map<String,EvolutionJob>> next = iterator.next();
					for(EvolutionJob job:next.getValue().values()){
						if(job.getCreationTime().before(minRetentionDate)){
							iterator.remove();
							break;
						}
					}
				}
			}
			// check for jobs to be done by registered listeners
			if(!listeners.isEmpty() && !hasLag.get() && !pendingJobs.isEmpty()){
				Iterator<EvolutionJob> iterator = pendingJobs.values().iterator();
				while(iterator.hasNext()){
					EvolutionJob next = iterator.next();
					List<Worker> workers = listeners.get(next.getName());
					if(workers!=null) workers.forEach(worker->worker.addJob(next));
					iterator.remove();
				}
			}
			// check for jobs to be cleaned by registered listeners
			if(!listeners.isEmpty() && !hasLag.get() && !terminatedJobs.isEmpty()){
				Iterator<EvolutionJob> iterator = terminatedJobs.values().iterator();
				while(iterator.hasNext()){
					EvolutionJob next = iterator.next();
					List<Worker> workers = listeners.get(next.getName());
					if(workers!=null) workers.forEach(worker->worker.jobTerminated(next));
					iterator.remove();
				}
			}

		}
	}

	private void waitNoLag(){
		while(hasLag.get()){
			synchronized (hasLag){
				try {hasLag.wait();} catch (InterruptedException e) {}
			}
		}
	}

	private Map<String,Map<String,EvolutionJob>> getAllJobs(int tenantID){
		waitNoLag();
		if(tenantID==0) return this.allJobs;
		Map<String,Map<String,EvolutionJob>> result = new HashMap<>(this.allJobs.size());
		for(Map.Entry<String,Map<String,EvolutionJob>> entry:this.allJobs.entrySet()){
			for(Map.Entry<String,EvolutionJob> perProcessEntry:entry.getValue().entrySet()){
				if(perProcessEntry.getValue().getTenantID()==tenantID){
					result.put(entry.getKey(),entry.getValue());
					break;
				}
			}
		}
		return result;
	}

	private void addWorker(EvolutionJob.Name name, Worker worker){this.listeners.computeIfAbsent(name,key->new ArrayList<>()).add(worker);}



	// singleton Reader and public access, static method, thread safe
	private static class ReaderHolder{
		private static final Reader reader = new Reader();
	}
	private static Reader getReader(){return ReaderHolder.reader;}

	protected static void addWorkerFor(EvolutionJob.Name job, Worker worker){getReader().addWorker(job,worker);}

	public static Map<String,Map<String,EvolutionJob>> getAllJobsStatus(int tenantID){ return getReader().getAllJobs(tenantID);}
	public static JSONArray getAllJobsStatusJSon(int tenantID){
		JSONArray result = new JSONArray();
		for(String jobID:getAllJobsStatus(tenantID).keySet()){
			JSONObject jsonJob = getJobStatusJSon(jobID,tenantID);
			if(jsonJob!=null) result.add(jsonJob);
		}
		return result;
	}

	public static Map<String,EvolutionJob> getJobStatus(String jobID, int tenantID){return getAllJobsStatus(tenantID).get(jobID);}
	public static JSONObject getJobStatusJSon(String jobID, int tenantID){
		Map<String,EvolutionJob> allProcessIDJobs = getJobStatus(jobID, tenantID);
		if(allProcessIDJobs==null || allProcessIDJobs.isEmpty()) return null;
		EvolutionJob globalStatus = getGlobalStatus(allProcessIDJobs);
		if(globalStatus.getStatus()!=EvolutionJob.Status.created && !EvolutionJob.Status.terminatedStatuses.contains(globalStatus.getStatus())){
			JSONObject jsonMain = globalStatus.toJSON();
			JSONArray jsonWorkers = new JSONArray();
			allProcessIDJobs.values().forEach(value->{
				JSONArray jsonWorkerPartsStatus = value.getWorkerPartsJSON();
				if(jsonWorkerPartsStatus!=null){
					JSONObject jsonWorker = new JSONObject();
					jsonWorker.put("processID",value.getProcessID());
					jsonWorker.put("partsStatus",jsonWorkerPartsStatus);
					jsonWorkers.add(jsonWorker);
				}
			});
			jsonMain.put("workers",jsonWorkers);
			return jsonMain;
		}else{
			return globalStatus.toJSON();
		}
	}



	// take the map of workers evolution job, and return the global status off all, withing one job only, which is the creation job one
	private static EvolutionJob getGlobalStatus(Map<String,EvolutionJob> perProcessIDStatus){
		EvolutionJob creationJob = null;
		Map<Integer, EvolutionJob.Status> partsStatus = new HashMap<>();
		for(EvolutionJob job:perProcessIDStatus.values()){
			if(job.getStatus()==EvolutionJob.Status.created){
				creationJob = job;
				continue;
			}
			if(creationJob==null) creationJob=job;//in case we don't have anymore the creation job, that should not happen
			if(creationJob.getTotalParts()==null && job.getTotalParts()!=null) creationJob.setTotalParts(job.getTotalParts());
			partsStatus.putAll(job.getWorkerParts());
		}
		if(!partsStatus.isEmpty()) creationJob.setWorkerParts(partsStatus);
		creationJob.computeStatus();
		creationJob.setProcessID("all");
		return creationJob;
	}

}
