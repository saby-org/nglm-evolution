package com.evolving.nglm.evolution.elasticsearch;

import java.util.Date;

import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.JobScheduler;
import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.ScheduledJob;
import com.evolving.nglm.evolution.VoucherService;

public class ElasticsearchManager
{
  private JobScheduler elasticsearchJobScheduler;
  private Thread schedulerThread;
  private SnapshotTask subscriberprofileSnapshotTask;
  private JourneyCleanUpTask journeyCleanUpTask;
  
  public ElasticsearchManager(ElasticsearchClientAPI elasticsearchClient, VoucherService voucherService, JourneyService journeyService) {
    this.elasticsearchJobScheduler = new JobScheduler("Elasticsearch jobs");
    
    this.subscriberprofileSnapshotTask = new SnapshotTask("Snapshot:subscriberprofile", "subscriberprofile", "subscriberprofile_snapshot", elasticsearchClient);
    this.journeyCleanUpTask = new JourneyCleanUpTask(journeyService, elasticsearchClient);
    
    //
    // Schedule all Elasticsearch jobs
    //
    long uniqueID = 0;
    uniqueID = ElasticsearchManager.scheduleSnapshot(elasticsearchJobScheduler, uniqueID, this.subscriberprofileSnapshotTask);
    uniqueID = ElasticsearchManager.scheduleJourneystatisticCleanUp(elasticsearchJobScheduler, uniqueID, this.journeyCleanUpTask);
    uniqueID = ElasticsearchManager.scheduleVoucherCleanUp(elasticsearchJobScheduler, uniqueID, voucherService);
  }
  
  public void start() {
    schedulerThread = new Thread(elasticsearchJobScheduler::runScheduler);
    schedulerThread.start();
  }
  
  public void stop() {
    elasticsearchJobScheduler.stop();
  }
  
  private static long scheduleSnapshot(JobScheduler scheduler, long nextAvailableID, SnapshotTask snapshotTask) {
    String jobName = "SubscriberProfileSnapshot";
    ScheduledJob job = new ScheduledJob(nextAvailableID,
        jobName, 
        Deployment.getElasticsearchJobsScheduling().get(jobName).getCronEntry(), 
        Deployment.getBaseTimeZone(),
        Deployment.getElasticsearchJobsScheduling().get(jobName).isScheduledAtRestart())
    {
      @Override
      protected void run()
      {
        Date now = SystemTime.getCurrentTime();
        // This snapshot is done the day after the "saved" day (after midnight, in the morning usually)
        Date yesterday = RLMDateUtils.addDays(now, -1, Deployment.getBaseTimeZone());
        snapshotTask.run(yesterday);
      }
    };
    
    if(Deployment.getElasticsearchJobsScheduling().get(jobName).isEnabled() && job.isProperlyConfigured()) {
      scheduler.schedule(job);
      return nextAvailableID + 1;
    } 
    else {
      return nextAvailableID;
    }
  }

  private static long scheduleJourneystatisticCleanUp(JobScheduler scheduler, long nextAvailableID, JourneyCleanUpTask journeyCleanUpTask) {
    String jobName = "JourneystatisticCleanUp";
    ScheduledJob job = new ScheduledJob(nextAvailableID,
        jobName, 
        Deployment.getElasticsearchJobsScheduling().get(jobName).getCronEntry(), 
        Deployment.getBaseTimeZone(),
        Deployment.getElasticsearchJobsScheduling().get(jobName).isScheduledAtRestart())
    {
      @Override
      protected void run()
      {
        journeyCleanUpTask.start();
      }
    };
    
    if(Deployment.getElasticsearchJobsScheduling().get(jobName).isEnabled() && job.isProperlyConfigured()) {
      scheduler.schedule(job);
      return nextAvailableID + 1;
    } 
    else {
      return nextAvailableID;
    }
  }

  private static long scheduleVoucherCleanUp(JobScheduler scheduler, long nextAvailableID, VoucherService voucherService) {
    String jobName = "ExpiredVoucherCleanUp";
    ScheduledJob job = new ScheduledJob(nextAvailableID,
        jobName, 
        Deployment.getElasticsearchJobsScheduling().get(jobName).getCronEntry(), 
        Deployment.getBaseTimeZone(),
        Deployment.getElasticsearchJobsScheduling().get(jobName).isScheduledAtRestart())
    {
      @Override
      protected void run()
      {
        voucherService.cleanUpVouchersJob();
      }
    };
    
    if(Deployment.getElasticsearchJobsScheduling().get(jobName).isEnabled() && job.isProperlyConfigured()) {
      scheduler.schedule(job);
      return nextAvailableID + 1;
    } 
    else {
      return nextAvailableID;
    }
  }
}
