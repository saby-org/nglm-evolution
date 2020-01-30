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
import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.LoyaltyProgramService;
import com.evolving.nglm.evolution.OfferService;
import com.evolving.nglm.evolution.PaymentMeanService;
import com.evolving.nglm.evolution.SalesChannelService;
import com.evolving.nglm.evolution.SegmentationDimensionService;
import com.evolving.nglm.evolution.datacubes.journeys.JourneyDatacubesDefinitiveJob;
import com.evolving.nglm.evolution.datacubes.journeys.JourneyDatacubesTemporaryJob;
import com.evolving.nglm.evolution.datacubes.loyalty.LoyaltyDatacubesOnTodayJob;
import com.evolving.nglm.evolution.datacubes.loyalty.LoyaltyDatacubesOnYesterdayJob;
import com.evolving.nglm.evolution.datacubes.odr.ODRDatacubeOnTodayJob;
import com.evolving.nglm.evolution.datacubes.odr.ODRDatacubeOnYesterdayJob;
import com.evolving.nglm.evolution.datacubes.snapshots.SubscriberProfileSnapshot;

import org.apache.http.HttpHost;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DatacubeManager is a singleton process.
 * 
 * In the future, it could be scalable on journey datacubes for instance.
 *  Reminder: at the moment (2020-01-30) there is two datacubes for EACH active journey (journeytraffic & journeyrewards)
 *  If each instance of datacubemanager is a consumer of journey topic AND partitioning of the journey topic change from 1 to many. 
 *  Then it could help to split the work of computing those many datacubes if in the future it takes too much time.
 *  
 * @author Remi
 */

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

  //
  //  static data (for the singleton instance)
  //

  private static DynamicCriterionFieldService dynamicCriterionFieldService; 
  private static JourneyService journeyService;
  private static LoyaltyProgramService loyaltyProgramService;
  private static SegmentationDimensionService segmentationDimensionService;
  private static OfferService offerService;
  private static SalesChannelService salesChannelService;
  private static PaymentMeanService paymentMeanService;
  private static RestHighLevelClient elasticsearchRestClient;

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
    String applicationID = "datacubemanager";
    String instanceID = args[2];
    String elasticsearchServerHost = args[3];
    Integer elasticsearchServerPort = Integer.parseInt(args[4]);

    /*****************************************
    *
    *  configuration
    *
    *****************************************/
    
    //
    //  dynamicCriterionFieldsService
    //

    dynamicCriterionFieldService = new DynamicCriterionFieldService(bootstrapServers, applicationID + "dynamiccriterionfieldservice-" + instanceID, Deployment.getDynamicCriterionFieldTopic(), false);
    dynamicCriterionFieldService.start();
    CriterionContext.initialize(dynamicCriterionFieldService); // Workaround: CriterionContext must be initialized before creating the JourneyService. (explain ?)

    //
    //  journeyService
    //

    journeyService = new JourneyService(bootstrapServers, applicationID + "-journeyservice-" + instanceID, Deployment.getJourneyTopic(), false);
    journeyService.start();
    
    //
    //  loyaltyProgramService
    // 
    
    loyaltyProgramService = new LoyaltyProgramService(bootstrapServers, applicationID + "-loyaltyProgramService-" + instanceID, Deployment.getLoyaltyProgramTopic(), false);
    loyaltyProgramService.start();

    //
    //  segmentationDimensionService
    //

    segmentationDimensionService = new SegmentationDimensionService(bootstrapServers, applicationID + "-segmentationdimensionservice-" + instanceID, Deployment.getSegmentationDimensionTopic(), false);
    segmentationDimensionService.start();

    //
    //  offerService
    //

    offerService = new OfferService(bootstrapServers, applicationID + "-offer-" + instanceID, Deployment.getOfferTopic(), false);
    offerService.start();

    //
    //  salesChannelService
    //

    salesChannelService = new SalesChannelService(bootstrapServers, applicationID + "-saleschannel-" + instanceID, Deployment.getSalesChannelTopic(), false);
    salesChannelService.start();
    
    //
    // pointService
    //
    
    paymentMeanService = new PaymentMeanService(bootstrapServers, applicationID + "-paymentmeanservice-" + instanceID, Deployment.getPaymentMeanTopic(), false);
    paymentMeanService.start();


    /*****************************************
    *
    *  shutdown hook
    *
    *****************************************/
    
    NGLMRuntime.addShutdownHook(new ShutdownHook(this));
    
    /*****************************************
    *
    *  initialize ES client & GUI client
    *  
    *****************************************/
    
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
    
    ScheduledJob temporaryODR = new ODRDatacubeOnTodayJob(uniqueID++, elasticsearchRestClient, offerService, salesChannelService, paymentMeanService, loyaltyProgramService, journeyService);
    if(temporaryODR.properlyConfigured)
      {
        datacubeScheduler.schedule(temporaryODR);
      }
    
    ScheduledJob temporaryLoyalty = new LoyaltyDatacubesOnTodayJob(uniqueID++, elasticsearchRestClient, loyaltyProgramService);
    if(temporaryLoyalty.properlyConfigured)
      {
        datacubeScheduler.schedule(temporaryLoyalty);
      }
    
//    ScheduledJob temporaryJourneyTraffic = new JourneyDatacubesTemporaryJob(uniqueID++, elasticsearchRestClient, segmentationDimensionService, journeyService);
//    if(temporaryJourneyTraffic.properlyConfigured)
//      {
//        datacubeScheduler.schedule(temporaryJourneyTraffic);
//      }
    
    //
    // Definitives datacubes 
    //
    
    ScheduledJob definitiveODR = new ODRDatacubeOnYesterdayJob(uniqueID++, elasticsearchRestClient, offerService, salesChannelService, paymentMeanService, loyaltyProgramService, journeyService);
    if(definitiveODR.properlyConfigured)
      {
        datacubeScheduler.schedule(definitiveODR);
      }
    
    ScheduledJob definitiveLoyalty = new LoyaltyDatacubesOnYesterdayJob(uniqueID++, elasticsearchRestClient, loyaltyProgramService);
    if(definitiveLoyalty.properlyConfigured)
      {
        datacubeScheduler.schedule(definitiveLoyalty);
      }
    
    ScheduledJob definitiveJourneyTraffic = new JourneyDatacubesDefinitiveJob(uniqueID++, elasticsearchRestClient, segmentationDimensionService, journeyService);
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

    //
    //  constructor
    //

    private ShutdownHook(DatacubeManager datacubemanager)
    {
      this.datacubemanager = datacubemanager;
    }

    //
    //  shutdown
    //

    @Override public void shutdown(boolean normalShutdown)
    {
      datacubemanager.shutdownUCGEngine(normalShutdown);
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
    *  stop services
    *
    *****************************************/

    dynamicCriterionFieldService.stop(); 
    journeyService.stop();
    loyaltyProgramService.stop();
    segmentationDimensionService.stop();
    offerService.stop();
    salesChannelService.stop();
    paymentMeanService.stop();

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

    NGLMRuntime.initialize(true);
    DatacubeManager datacubemanager = new DatacubeManager(args);
    
    log.info("Service initialized");

    //
    //  run
    //

    datacubemanager.run();
  }
}
