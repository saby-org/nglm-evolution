package com.evolving.nglm.evolution.elasticsearch;

import java.util.Map;

import org.elasticsearch.ElasticsearchException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.evolution.JobScheduler;
import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.LoggerInitialization;
import com.evolving.nglm.evolution.ScheduledJob;
import com.evolving.nglm.evolution.ScheduledJobConfiguration;
import com.evolving.nglm.evolution.VoucherService;
import com.evolving.nglm.evolution.tenancy.Tenant;

/**
 * ElasticsearchManager is a singleton process.
 */
public class ElasticsearchManager
{
  private static final Logger log = LoggerFactory.getLogger(ElasticsearchManager.class);
  
  /*****************************************
  *
  * Static data (for the singleton instance)
  *
  *****************************************/
  //
  // Services
  //
  private static VoucherService voucherService;
  private static JourneyService journeyService;
  
  //
  // Elasticsearch Client
  //
  private static ElasticsearchClientAPI elasticsearchRestClient;
  
  //
  // Schedulers
  //
  private static JobScheduler elasticsearchJobScheduler;
  private static Thread schedulerThread;

  /*****************************************
  *
  * Getters
  *
  *****************************************/
  public static ElasticsearchClientAPI getElasticsearchClientAPI() { return elasticsearchRestClient; }
  public static VoucherService getVoucherService() { return voucherService; }
  public static JourneyService getJourneyService() { return journeyService; }

  /*****************************************
  *
  * class ShutdownHook
  *
  *****************************************/
  private static class ShutdownHook implements NGLMRuntime.NGLMShutdownHook
  {
    @Override public void shutdown(boolean normalShutdown)
    {
      ElasticsearchManager.shutdownUCGEngine(normalShutdown);
    }
  }

  /****************************************
  *
  * shutdownUCGEngine
  *
  ****************************************/
  private static void shutdownUCGEngine(boolean normalShutdown)
  {
    /*****************************************
    *
    *  stop threads
    *
    *****************************************/
    elasticsearchJobScheduler.stop();

    /*****************************************
    *
    *  stop services
    *
    *****************************************/
    journeyService.stop();
    voucherService.stop();
    
    /*****************************************
    *
    *  log
    *
    *****************************************/
    log.info("Stopped ElasticsearchManager");
  }

  /*****************************************
  *
  * Engine
  *
  *****************************************/
  public static void main(String[] args)
  {
    //
    // NGLMRuntime  
    //
    NGLMRuntime.initialize(true);
    NGLMRuntime.addShutdownHook(new ShutdownHook());

    //
    // Logger
    //
    new LoggerInitialization().initLogger();

    //
    // Services
    //
    String bootstrapServers = Deployment.getBrokerServers();
    journeyService = new JourneyService(bootstrapServers, "NOT_USED", Deployment.getJourneyTopic(), false);
    journeyService.start();
    voucherService = new VoucherService(bootstrapServers, "NOT_USED", Deployment.getVoucherTopic());
    voucherService.start();
    
    //
    // initialize ES client & GUI client
    //
    try
      {
        elasticsearchRestClient = new ElasticsearchClientAPI("ElasticsearchManager");
      }
    catch (ElasticsearchException e)
      {
        throw new ServerRuntimeException("could not initialize elasticsearch client", e);
      }
    
    //
    // Job scheduling
    //
    elasticsearchJobScheduler = new JobScheduler("Elasticsearch jobs");

    //
    // Set all jobs from configuration
    //
    for(Tenant tenant: Deployment.getTenants()) {
      int tenantID = tenant.getTenantID();
      Map<String, ScheduledJobConfiguration> jobConfigs = Deployment.getDeployment(tenantID).getElasticsearchJobsScheduling();
      if(jobConfigs == null) {
        continue;
      }
      
      for(String jobID: jobConfigs.keySet()) {
        ScheduledJobConfiguration config = jobConfigs.get(jobID);
        
        if(config.isEnabled()) {
          ScheduledJob job = ElasticsearchJobs.createElasticsearchJob(config);
          elasticsearchJobScheduler.schedule(job);
        }
      }
    }
    
    log.info("Service initialized.");

    //
    //  run
    //
    schedulerThread = new Thread(elasticsearchJobScheduler::runScheduler);
    schedulerThread.start();
  }
}
