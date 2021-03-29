package com.evolving.nglm.evolution.elasticsearch;

import java.util.Map;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.evolution.JobScheduler;
import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.ScheduledJob;
import com.evolving.nglm.evolution.ScheduledJobConfiguration;
import com.evolving.nglm.evolution.VoucherService;

public class ElasticsearchManager
{
  /*****************************************
  *
  * Properties
  *
  *****************************************/
  private JobScheduler elasticsearchJobScheduler;
  private Thread schedulerThread;
  
  //
  // Client & Services
  //
  private ElasticsearchClientAPI elasticsearchRestClient;
  private VoucherService voucherService;
  private JourneyService journeyService;

  /*****************************************
  *
  * Constructor
  *
  *****************************************/
  public ElasticsearchManager(ElasticsearchClientAPI elasticsearchClient, VoucherService voucherService, JourneyService journeyService) {
    this.elasticsearchRestClient = elasticsearchClient;
    this.voucherService = voucherService;
    this.journeyService = journeyService;
    
    this.elasticsearchJobScheduler = new JobScheduler("Elasticsearch jobs");

    //
    // Set all jobs from configuration
    //
    for(Integer tenantID: Deployment.getTenantIDs()) {
      Map<String, ScheduledJobConfiguration> jobConfigs = Deployment.getDeployment(tenantID).getElasticsearchJobsScheduling();
      if(jobConfigs == null) {
        continue;
      }
      
      for(String jobID: jobConfigs.keySet()) {
        ScheduledJobConfiguration config = jobConfigs.get(jobID);
        
        if(config.isEnabled()) {
          ScheduledJob job = ElasticsearchJobs.createElasticsearchJob(config, this);
          elasticsearchJobScheduler.schedule(job);
        }
      }
    }
  }

  /*****************************************
  *
  * Getters
  *
  *****************************************/
  public ElasticsearchClientAPI getElasticsearchClientAPI() { return this.elasticsearchRestClient; }
  public VoucherService getVoucherService() { return this.voucherService; }
  public JourneyService getJourneyService() { return this.journeyService; }
  
  /*****************************************
  *
  * Start & Stop scheduler thread
  *
  *****************************************/
  public void start() {
    schedulerThread = new Thread(elasticsearchJobScheduler::runScheduler);
    schedulerThread.start();
  }
  
  public void stop() {
    elasticsearchJobScheduler.stop();
  }
}
