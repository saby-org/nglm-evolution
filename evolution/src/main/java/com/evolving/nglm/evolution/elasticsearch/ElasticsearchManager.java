package com.evolving.nglm.evolution.elasticsearch;

import java.util.Map;

import org.elasticsearch.ElasticsearchException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.evolution.CriterionContext;
import com.evolving.nglm.evolution.DynamicCriterionFieldService;
import com.evolving.nglm.evolution.JobScheduler;
import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.LoggerInitialization;
import com.evolving.nglm.evolution.PredictionOrderService;
import com.evolving.nglm.evolution.PredictionOrganizer;
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
  private static DynamicCriterionFieldService dynamicCriterionFieldService;
  private static VoucherService voucherService;
  private static JourneyService journeyService;
  private static PredictionOrderService predictionOrderService;
  
  //
  // Elasticsearch Client
  //
  private static ElasticsearchClientAPI elasticsearchRestClient;
  
  //
  // Schedulers
  //
  private static JobScheduler elasticsearchJobScheduler;
  private static Thread schedulerThread;
  private static Thread predictionThread;

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
    *  stop prediction
    *
    *****************************************/
    PredictionOrganizer.close();
    
    /*****************************************
    *
    *  stop threads
    *
    *****************************************/
    if (elasticsearchJobScheduler != null) elasticsearchJobScheduler.stop();

    /*****************************************
    *
    *  stop services
    *
    *****************************************/
    if (dynamicCriterionFieldService != null) dynamicCriterionFieldService.stop();
    if (journeyService != null) journeyService.stop();
    if (voucherService != null) voucherService.stop();
    if (predictionOrderService != null) predictionOrderService.stop();
    
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
    // Services
    //
    String bootstrapServers = Deployment.getBrokerServers();
    // TODO: HUGLY but necessary (dependency CriterionContext.initialize(dynamicCriterionFieldService) with JourneyService)
    dynamicCriterionFieldService = new DynamicCriterionFieldService(bootstrapServers, "NOT_USED", Deployment.getDynamicCriterionFieldTopic(), false);
    CriterionContext.initialize(dynamicCriterionFieldService);
    journeyService = new JourneyService(bootstrapServers, "NOT_USED", Deployment.getJourneyTopic(), false);
    journeyService.start();
    voucherService = new VoucherService(bootstrapServers, "NOT_USED", Deployment.getVoucherTopic(), elasticsearchRestClient);
    voucherService.start();
    predictionOrderService = new PredictionOrderService(bootstrapServers, Deployment.getPredictionOrderTopic(), false);
    predictionOrderService.start();
    dynamicCriterionFieldService.start();
    
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
    //  run ES scheduler
    //
    schedulerThread = new Thread(elasticsearchJobScheduler::runScheduler);
    schedulerThread.start();
    
    //
    //  run prediction organizer
    //
    predictionThread = new Thread(new Runnable() {
      @Override public void run()
      {
        PredictionOrganizer.start(elasticsearchRestClient, predictionOrderService);
      }
    });
    predictionThread.start();
  }
}
