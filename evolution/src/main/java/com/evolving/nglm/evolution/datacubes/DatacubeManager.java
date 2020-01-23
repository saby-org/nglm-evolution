/****************************************************************************
*
*  DatacubeManager.java 
*
****************************************************************************/

package com.evolving.nglm.evolution.datacubes;

import com.evolving.nglm.core.*;
import com.evolving.nglm.evolution.datacubes.journeys.JourneyTrafficDatacubeGenerator;
import com.evolving.nglm.evolution.CriterionContext;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.DynamicCriterionFieldService;
import com.evolving.nglm.evolution.datacubes.journeys.JourneyTrafficDatacubeDefinitiveJob;
import com.evolving.nglm.evolution.datacubes.journeys.JourneyTrafficDatacubeTemporaryJob;
import com.evolving.nglm.evolution.datacubes.loyalty.LoyaltyDatacubeOnTodayJob;
import com.evolving.nglm.evolution.datacubes.loyalty.LoyaltyDatacubeOnYesterdayJob;
import com.evolving.nglm.evolution.datacubes.mapping.GUIManagerClient;
import com.evolving.nglm.evolution.datacubes.odr.ODRDatacubeOnTodayJob;
import com.evolving.nglm.evolution.datacubes.odr.ODRDatacubeOnYesterdayJob;
import com.evolving.nglm.evolution.datacubes.snapshots.SubscriberProfileSnapshot;
import com.evolving.nglm.evolution.datacubes.tiers.TiersDatacubeOnTodayJob;
import com.evolving.nglm.evolution.datacubes.tiers.TiersDatacubeOnYesterdayJob;

import org.apache.http.HttpHost;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatacubeManager
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(DatacubeManager.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/
  
  private RestHighLevelClient elasticsearchRestClient;
  private GUIManagerClient guiManagerClient;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public DatacubeManager(String[] args)
  {
    /*****************************************
    *
    *  args
    *
    *****************************************/
    
    String bootstrapServers = args[1];
    String elasticsearchServerHost = args[2];
    Integer elasticsearchServerPort = Integer.parseInt(args[3]);
    String guiManagerServerHost = args[4];
    String guiManagerServerPort = args[5];

    /*****************************************
    *
    *  dynamic Criterion
    *
    *****************************************/

    String dynamicCriterionFieldTopic = Deployment.getDynamicCriterionFieldTopic();
    DynamicCriterionFieldService dynamicCriterionFieldService = new DynamicCriterionFieldService(bootstrapServers, "datacubeManager-dynamiccriterionfieldservice-001", dynamicCriterionFieldTopic, true);
    dynamicCriterionFieldService.start();
    CriterionContext.initialize(dynamicCriterionFieldService);
    
    /*****************************************
    *
    *  runtime
    *
    *****************************************/
    
    NGLMRuntime.initialize(true);

    /*****************************************
    *
    *  shutdown hook
    *
    *****************************************/
    
    NGLMRuntime.addShutdownHook(new ShutdownHook(this, dynamicCriterionFieldService));
    
    /*****************************************
    *
    *  initialize ES client & GUI client
    *  
    *****************************************/
    
    guiManagerClient = new GUIManagerClient(guiManagerServerHost, guiManagerServerPort);
    try
      {
        elasticsearchRestClient = new RestHighLevelClient(RestClient.builder(new HttpHost(elasticsearchServerHost, elasticsearchServerPort, "http")));
      }
    catch (ElasticsearchException e)
      {
        throw new ServerRuntimeException("could not initialize elasticsearch client", e);
      }
  }

  /*****************************************
  *
  *  run
  *
  *****************************************/

  public void run()
  {
    JobScheduler datacubeScheduler = new JobScheduler();
    
    //
    // Adding datacubes scheduling
    //
    
    long uniqueID = 0;
    
    //
    // Temporary datacubes (will be updated later by the definitive version)
    //
    
    ScheduledJob temporaryODR = new ODRDatacubeOnTodayJob(uniqueID++, elasticsearchRestClient, guiManagerClient);
    if(temporaryODR.properlyConfigured)
      {
        datacubeScheduler.schedule(temporaryODR);
      }
    
    ScheduledJob temporaryLoyalty = new LoyaltyDatacubeOnTodayJob(uniqueID++, elasticsearchRestClient, guiManagerClient);
    if(temporaryLoyalty.properlyConfigured)
      {
        datacubeScheduler.schedule(temporaryLoyalty);
      }
    
    ScheduledJob temporaryTiers = new TiersDatacubeOnTodayJob(uniqueID++, elasticsearchRestClient, guiManagerClient);
    if(temporaryTiers.properlyConfigured)
      {
        datacubeScheduler.schedule(temporaryTiers);
      }
    
//    ScheduledJob temporaryJourneyTraffic = new JourneyTrafficDatacubeTemporaryJob(uniqueID++, elasticsearchRestClient, guiManagerClient);
//    if(temporaryJourneyTraffic.properlyConfigured)
//      {
//        datacubeScheduler.schedule(temporaryJourneyTraffic);
//      }
    
    //
    // Definitives datacubes 
    //
    
    ScheduledJob definitiveODR = new ODRDatacubeOnYesterdayJob(uniqueID++, elasticsearchRestClient, guiManagerClient);
    if(definitiveODR.properlyConfigured)
      {
        datacubeScheduler.schedule(definitiveODR);
      }
    
    ScheduledJob definitiveLoyalty = new LoyaltyDatacubeOnYesterdayJob(uniqueID++, elasticsearchRestClient, guiManagerClient);
    if(definitiveLoyalty.properlyConfigured)
      {
        datacubeScheduler.schedule(definitiveLoyalty);
      }
    
    ScheduledJob definitiveTiers = new TiersDatacubeOnYesterdayJob(uniqueID++, elasticsearchRestClient, guiManagerClient);
    if(definitiveTiers.properlyConfigured)
      {
        datacubeScheduler.schedule(definitiveTiers);
      }
    
    ScheduledJob definitiveJourneyTraffic = new JourneyTrafficDatacubeDefinitiveJob(uniqueID++, elasticsearchRestClient, guiManagerClient);
    if(definitiveJourneyTraffic.properlyConfigured)
      {
        datacubeScheduler.schedule(definitiveJourneyTraffic);
      }
    
    //
    // Snapshots
    //
    
    ScheduledJob subscriberprofileSnapshot = new SubscriberProfileSnapshot(uniqueID++, elasticsearchRestClient);
    if(subscriberprofileSnapshot.properlyConfigured)
      {
        datacubeScheduler.schedule(subscriberprofileSnapshot);
      }

    log.info("Starting scheduler");
    datacubeScheduler.runScheduler();
  }

  /*****************************************
  *
  *  class ShutdownHook
  *
  *****************************************/

  private static class ShutdownHook implements NGLMRuntime.NGLMShutdownHook
  {
    //
    //  data
    //

    private DatacubeManager datacubemanager;
    private DynamicCriterionFieldService dynamicCriterionFieldService;

    //
    //  constructor
    //

    private ShutdownHook(DatacubeManager datacubemanager, DynamicCriterionFieldService dynamicCriterionFieldService)
    {
      this.datacubemanager = datacubemanager;
      this.dynamicCriterionFieldService = dynamicCriterionFieldService;
    }

    //
    //  shutdown
    //

    @Override public void shutdown(boolean normalShutdown)
    {
      datacubemanager.shutdownUCGEngine(normalShutdown);
      if (dynamicCriterionFieldService != null) dynamicCriterionFieldService.stop();
    }
  }

  /****************************************
  *
  *  shutdownUCGEngine
  *
  ****************************************/
  
  private void shutdownUCGEngine(boolean normalShutdown)
  {
    /*****************************************
    *
    *  stop threads
    *
    *****************************************/

    /*****************************************
    *
    *  close resources
    *
    *****************************************/

    /*****************************************
    *
    *  log
    *
    *****************************************/

    log.info("Stopped DatacubeManager");
  }

  /*****************************************
  *
  *  engine
  *
  *****************************************/

  public static void main(String[] args)
  {
    //
    //  instance  
    //

    DatacubeManager datacubemanager = new DatacubeManager(args);
    log.info("Service initialized");

    //
    //  run
    //

    datacubemanager.run();
  }
}
