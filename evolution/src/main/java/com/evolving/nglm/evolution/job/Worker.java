package com.evolving.nglm.evolution.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public abstract class Worker {

	private static final Logger log = LoggerFactory.getLogger(Worker.class);

	private String workerID;
	// scheduler give and update job, it should be mono threaded
	private Executor scheduler = Executors.newFixedThreadPool(1, new NamedThreadFactory("EvolutionJobScheduler",true));
	// workers execute really the jobs, they can be multi-threaded
	private Executor workers;
	// list of job parts already scheduled/progressing, should be manipulated only by the scheduler
	private Map<String,Set<Integer>> partsBeingProcessed = new HashMap<>();

	protected Worker(String workerID, int threadPoolSize, EvolutionJob.Name... workerForJobNames){
		this.workerID = workerID;
		this.workers = Executors.newFixedThreadPool(threadPoolSize, new NamedThreadFactory("EvolutionJobWorker",true));
		for(EvolutionJob.Name name:workerForJobNames) Reader.addWorkerFor(name,this);
	}

	protected String getWorkerID(){return this.workerID;}

	protected void addJob(EvolutionJob jobToProcess){
		this.scheduler.execute(new Job(this,jobToProcess));
	}
	protected void jobTerminated(EvolutionJob jobToProcess){
		this.scheduler.execute(new JobTerminated(this,jobToProcess));
	}

	abstract protected int getTotalParts(EvolutionJob job);
	abstract protected Set<Integer> getWorkerParts(EvolutionJob job);
	abstract protected EvolutionJob.Status processPart(EvolutionJob job, int part);

	private static class Job implements Runnable{
		private final Worker worker;
		private final EvolutionJob job;
		private Job(Worker worker, EvolutionJob job){
			this.worker = worker;
			this.job = new EvolutionJob(job);
		}
		@Override
		public void run() {
			job.setProcessID(worker.getWorkerID());
			job.setTotalParts(worker.getTotalParts(job));
			Set<Integer> partsToProcess = new HashSet<>();
			Set<Integer> partsHandled = worker.getWorkerParts(job);
			job.getWorkerParts().entrySet().removeIf(entry->!partsHandled.contains(entry.getKey()));// remove part we do not handle
			for(Integer workerPart:partsHandled){
				Set<Integer> onGoingParts = worker.partsBeingProcessed.computeIfAbsent(job.getJobID(),key->new HashSet<>());
				EvolutionJob.Status partStatus = job.getWorkerParts().get(workerPart);
				if(!onGoingParts.contains(workerPart) && !EvolutionJob.Status.terminatedStatuses.contains(partStatus)){
					partsToProcess.add(workerPart);
					onGoingParts.add(workerPart);
				}
			}
			// nothing to do more, returning
			if(partsToProcess.isEmpty()){
				if(log.isTraceEnabled()) log.trace(worker.getClass().getName()+" addJob nothing to do "+job);
				return;
			}
			// update jobs parts status and give job to workers thread pool
			partsToProcess.forEach(part->{
				job.setPartStatus(part, EvolutionJob.Status.pending);
				worker.workers.execute(new PartJob(worker,part,job));
			});
		}
	}

	private static class PartJob implements Runnable{
		private final Worker worker;
		private final int part;
		private final EvolutionJob job;
		private PartJob(Worker worker, int part, EvolutionJob job){
			this.worker = worker;
			this.part = part;
			this.job = job;
		}
		@Override
		public void run() {
			log.info(worker.getClass().getName()+" starting part "+part +" of job "+job);
			// set part in progress and send back on topic the info
			job.setPartStatus(part, EvolutionJob.Status.inProgress);
			Sender.send(job);
			// run the part job
			EvolutionJob.Status partStatus = worker.processPart(job,part);
			// give result back to the scheduler
			worker.scheduler.execute(new UpdatePartJobStatus(worker,part,job,partStatus));
		}
	}

	private static class UpdatePartJobStatus implements Runnable{
		private final Worker worker;
		private final int part;
		private final EvolutionJob job;
		private final EvolutionJob.Status partStatus;
		private UpdatePartJobStatus(Worker worker, int part, EvolutionJob job, EvolutionJob.Status partStatus){
			this.worker = worker;
			this.part = part;
			this.job = job;
			this.partStatus = partStatus;
		}
		@Override
		public void run() {
			// set part status and send back status on topic
			if(log.isTraceEnabled()) log.trace(worker.getClass().getName()+" updating part "+part+" status to "+partStatus);
			job.setPartStatus(part, partStatus);
			Sender.send(job);
			log.info(worker.getClass().getName()+" end part "+part +" of job "+job);
		}
	}

	private static class JobTerminated implements Runnable{
		private final Worker worker;
		private final EvolutionJob job;
		private JobTerminated(Worker worker, EvolutionJob job){
			this.worker = worker;
			this.job = job;
		}
		@Override
		public void run() {
			// cleaning job
			if(log.isTraceEnabled()) log.trace(worker.getClass().getName()+" removing from ongoing job list "+worker.partsBeingProcessed+" job "+job);
			worker.partsBeingProcessed.remove(job.getJobID());
			log.info(worker.getClass().getName()+" job finished "+job);
		}
	}


}
