/****************************************************************************
*
*  DatacubeManager.java 
*
****************************************************************************/

package com.evolving.nglm.evolution.datacubes;

import com.evolving.nglm.core.*;
import com.evolving.nglm.evolution.CriterionContext;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.DynamicCriterionFieldService;
import com.evolving.nglm.evolution.JobScheduler;
import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.LoggerInitialization;
import com.evolving.nglm.evolution.LoyaltyProgramService;
import com.evolving.nglm.evolution.OfferObjectiveService;
import com.evolving.nglm.evolution.OfferService;
import com.evolving.nglm.evolution.PaymentMeanService;
import com.evolving.nglm.evolution.SalesChannelService;
import com.evolving.nglm.evolution.ScheduledJob;
import com.evolving.nglm.evolution.SegmentationDimensionService;
import com.evolving.nglm.evolution.SubscriberMessageTemplateService;
import com.evolving.nglm.evolution.datacubes.generator.BDRDatacubeGenerator;
import com.evolving.nglm.evolution.datacubes.generator.JourneyRewardsDatacubeGenerator;
import com.evolving.nglm.evolution.datacubes.generator.JourneyTrafficDatacubeGenerator;
import com.evolving.nglm.evolution.datacubes.generator.MDRDatacubeGenerator;
import com.evolving.nglm.evolution.datacubes.generator.ODRDatacubeGenerator;
import com.evolving.nglm.evolution.datacubes.generator.ProgramsChangesDatacubeGenerator;
import com.evolving.nglm.evolution.datacubes.generator.ProgramsHistoryDatacubeGenerator;
import com.evolving.nglm.evolution.datacubes.generator.SubscriberProfileDatacubeGenerator;
import com.evolving.nglm.evolution.datacubes.mapping.JourneysMap;
import com.evolving.nglm.evolution.datacubes.snapshots.SubscriberProfileSnapshot;

import java.util.Calendar;
import java.util.Date;

import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
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
  private static final Logger log = LoggerFactory.getLogger(DatacubeManager.class);

  /*****************************************
  *
  * Static data (for the singleton instance)
  *
  *****************************************/
  //
  // Services
  //
  private static DynamicCriterionFieldService dynamicCriterionFieldService; 
  private static JourneyService journeyService;
  private static LoyaltyProgramService loyaltyProgramService;
  private static SegmentationDimensionService segmentationDimensionService;
  private static OfferService offerService;
  private static SalesChannelService salesChannelService;
  private static PaymentMeanService paymentMeanService;
  private static OfferObjectiveService offerObjectiveService;
  private static RestHighLevelClient elasticsearchRestClient;
  private static SubscriberMessageTemplateService subscriberMessageTemplateService;
  
  //
  // Maps
  //
  private static JourneysMap journeysMap;
  
  //
  // Datacube generators
  //
  private static ProgramsHistoryDatacubeGenerator loyaltyHistoryDatacube;
  private static ProgramsChangesDatacubeGenerator tierChangesDatacube;
  private static JourneyTrafficDatacubeGenerator trafficDatacube;
  private static JourneyRewardsDatacubeGenerator rewardsDatacube;
  private static ODRDatacubeGenerator odrDatacube;
  private static BDRDatacubeGenerator bdrDatacube;
  private static MDRDatacubeGenerator mdrDatacube;
  private static SubscriberProfileDatacubeGenerator subscriberProfileDatacube;
  
  /*****************************************
  *
  * Constructor
  *
  *****************************************/
  public DatacubeManager(String[] args)
  {
    String bootstrapServers = args[1];
    String applicationID = "datacubemanager";
    String instanceID = args[2];
    String elasticsearchServerHost = args[3];
    Integer elasticsearchServerPort = Integer.parseInt(args[4]);
    
    //
    // Shutdown hook
    //
    NGLMRuntime.addShutdownHook(new ShutdownHook(this));
    
    //
    // Logger
    //
    new LoggerInitialization().initLogger();

    //
    // Services
    //
    dynamicCriterionFieldService = new DynamicCriterionFieldService(bootstrapServers, applicationID + "dynamiccriterionfieldservice-" + instanceID, Deployment.getDynamicCriterionFieldTopic(), false);
    dynamicCriterionFieldService.start();
    CriterionContext.initialize(dynamicCriterionFieldService); // Workaround: CriterionContext must be initialized before creating the JourneyService. (explain ?)
    journeyService = new JourneyService(bootstrapServers, applicationID + "-journeyservice-" + instanceID, Deployment.getJourneyTopic(), false);
    journeyService.start();
    loyaltyProgramService = new LoyaltyProgramService(bootstrapServers, applicationID + "-loyaltyProgramService-" + instanceID, Deployment.getLoyaltyProgramTopic(), false);
    loyaltyProgramService.start();
    segmentationDimensionService = new SegmentationDimensionService(bootstrapServers, applicationID + "-segmentationdimensionservice-" + instanceID, Deployment.getSegmentationDimensionTopic(), false);
    segmentationDimensionService.start();
    offerService = new OfferService(bootstrapServers, applicationID + "-offer-" + instanceID, Deployment.getOfferTopic(), false);
    offerService.start();
    salesChannelService = new SalesChannelService(bootstrapServers, applicationID + "-saleschannel-" + instanceID, Deployment.getSalesChannelTopic(), false);
    salesChannelService.start();
    paymentMeanService = new PaymentMeanService(bootstrapServers, applicationID + "-paymentmeanservice-" + instanceID, Deployment.getPaymentMeanTopic(), false);
    paymentMeanService.start();
    offerObjectiveService = new OfferObjectiveService(bootstrapServers, applicationID + "-offerobjectiveservice-" + instanceID, Deployment.getOfferObjectiveTopic(), false);
    offerObjectiveService.start();
    subscriberMessageTemplateService = new SubscriberMessageTemplateService(bootstrapServers, applicationID + "-subscribermessagetemplateservice-" + instanceID, Deployment.getSubscriberMessageTemplateTopic(), false);
    subscriberMessageTemplateService.start();
    
    //
    // initialize ES client & GUI client
    //
    // @rl TODO use ElasticsearchClientAPI ?
    try
      {
        RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost(elasticsearchServerHost, elasticsearchServerPort, "http"));
        restClientBuilder.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback()
        {
          @Override
          public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder)
          {
            return requestConfigBuilder.setConnectTimeout(Deployment.getElasticSearchConnectTimeout()).setSocketTimeout(Deployment.getElasticSearchQueryTimeout());
          }
        });
        elasticsearchRestClient = new RestHighLevelClient(restClientBuilder);
      }
    catch (ElasticsearchException e)
      {
        throw new ServerRuntimeException("could not initialize elasticsearch client", e);
      }
    
    //
    // Maps 
    //
    journeysMap = new JourneysMap(journeyService);
    
    //
    // Datacube generators
    //
    loyaltyHistoryDatacube = new ProgramsHistoryDatacubeGenerator("LoyaltyPrograms:History", elasticsearchRestClient, loyaltyProgramService);
    tierChangesDatacube = new ProgramsChangesDatacubeGenerator("LoyaltyPrograms:Changes", elasticsearchRestClient, loyaltyProgramService);
    trafficDatacube = new JourneyTrafficDatacubeGenerator("Journey:Traffic", elasticsearchRestClient, segmentationDimensionService, journeyService);
    rewardsDatacube = new JourneyRewardsDatacubeGenerator("Journey:Rewards", elasticsearchRestClient, segmentationDimensionService, journeyService);
    odrDatacube = new ODRDatacubeGenerator("ODR", elasticsearchRestClient, offerService, salesChannelService, paymentMeanService, offerObjectiveService, loyaltyProgramService, journeyService);
    bdrDatacube = new BDRDatacubeGenerator("BDR", elasticsearchRestClient, offerService, salesChannelService, paymentMeanService, offerObjectiveService, loyaltyProgramService, journeyService);
    mdrDatacube = new MDRDatacubeGenerator("MDR", elasticsearchRestClient, offerService, salesChannelService, paymentMeanService, offerObjectiveService, loyaltyProgramService, journeyService, subscriberMessageTemplateService);
    subscriberProfileDatacube = new SubscriberProfileDatacubeGenerator("SubscriberProfile", elasticsearchRestClient, segmentationDimensionService);
  }

  /*****************************************
  *
  * Datacube jobs
  *
  *****************************************/
  /*
   * Loyalty programs preview  
   *
   * This will generate a datacube preview of the day from the subscriberprofile index (not a snapshot one).
   * Those data are not definitive, the day is not ended yet, metrics can still change.
   */
  private static long scheduleLoyaltyProgramsPreview(JobScheduler scheduler, long nextAvailableID) {
    String datacubeName = "LoyaltyPrograms-preview";
    
    ScheduledJob job = new ScheduledJob(nextAvailableID,
        datacubeName, 
        Deployment.getDatacubeJobsScheduling().get(datacubeName).getCronEntry(), 
        Deployment.getBaseTimeZone(),
        Deployment.getDatacubeJobsScheduling().get(datacubeName).isScheduledAtRestart())
    {
      @Override
      protected void run()
      {
        loyaltyHistoryDatacube.preview();
        tierChangesDatacube.preview();
      }
    };
    
    if(Deployment.getDatacubeJobsScheduling().get(datacubeName).isEnabled() && job.isProperlyConfigured()) {
      scheduler.schedule(job);
      return nextAvailableID + 1;
    } 
    else {
      return nextAvailableID;
    }
  }

  /*
   * Loyalty programs definitive  
   *
   * This will generated a datacube every day from the subscriberprofile snapshot index of the previous day.
   */
  private static long scheduleLoyaltyProgramsDefinitive(JobScheduler scheduler, long nextAvailableID) {
    String datacubeName = "LoyaltyPrograms-definitive";
    
    ScheduledJob job = new ScheduledJob(nextAvailableID,
        datacubeName, 
        Deployment.getDatacubeJobsScheduling().get(datacubeName).getCronEntry(), 
        Deployment.getBaseTimeZone(),
        Deployment.getDatacubeJobsScheduling().get(datacubeName).isScheduledAtRestart())
    {
      @Override
      protected void run()
      {
        loyaltyHistoryDatacube.definitive();
        tierChangesDatacube.definitive();
      }
    };
    
    if(Deployment.getDatacubeJobsScheduling().get(datacubeName).isEnabled() && job.isProperlyConfigured()) {
      scheduler.schedule(job);
      return nextAvailableID + 1;
    } 
    else {
      return nextAvailableID;
    }
  }
  
  /*
   * SubscriberProfile preview
   *
   * This will generated a datacube preview of the day from the subscriberprofile index (not a snapshot one).
   * Those data are not definitive, the day is not ended yet, metrics can still change.
   */
  private static long scheduleSubscriberProfilePreview(JobScheduler scheduler, long nextAvailableID) {
    String datacubeName = "SubscriberProfile-preview";
    
    ScheduledJob job = new ScheduledJob(nextAvailableID,
        datacubeName, 
        Deployment.getDatacubeJobsScheduling().get(datacubeName).getCronEntry(), 
        Deployment.getBaseTimeZone(),
        Deployment.getDatacubeJobsScheduling().get(datacubeName).isScheduledAtRestart())
    {
      @Override
      protected void run()
      {
        subscriberProfileDatacube.preview();
      }
    };
    
    if(Deployment.getDatacubeJobsScheduling().get(datacubeName).isEnabled() && job.isProperlyConfigured()) {
      scheduler.schedule(job);
      return nextAvailableID + 1;
    } 
    else {
      return nextAvailableID;
    }
  }
  
  /*
   * SubscriberProfile definitive
   * 
   * This will generated a datacube every day from the subscriberprofile snapshot index of the previous day.
   */
  private static long scheduleSubscriberProfileDefinitive(JobScheduler scheduler, long nextAvailableID) {
    String datacubeName = "SubscriberProfile-definitive";
    
    ScheduledJob job = new ScheduledJob(nextAvailableID,
        datacubeName, 
        Deployment.getDatacubeJobsScheduling().get(datacubeName).getCronEntry(), 
        Deployment.getBaseTimeZone(),
        Deployment.getDatacubeJobsScheduling().get(datacubeName).isScheduledAtRestart())
    {
      @Override
      protected void run()
      {
        subscriberProfileDatacube.definitive();
      }
    };
    
    if(Deployment.getDatacubeJobsScheduling().get(datacubeName).isEnabled() && job.isProperlyConfigured()) {
      scheduler.schedule(job);
      return nextAvailableID + 1;
    } 
    else {
      return nextAvailableID;
    }
  }
  
  /*
   * ODR preview
   *
   * This will generated a datacube preview of the day from the detailedrecords_offers-YYYY-MM-dd index of the day
   * Those data are not definitive, the day is not ended yet, new ODR can still be added.
   */
  private static long scheduleODRPreview(JobScheduler scheduler, long nextAvailableID) {
    String datacubeName = "ODR-preview";
    
    ScheduledJob job = new ScheduledJob(nextAvailableID,
        datacubeName, 
        Deployment.getDatacubeJobsScheduling().get(datacubeName).getCronEntry(), 
        Deployment.getBaseTimeZone(),
        Deployment.getDatacubeJobsScheduling().get(datacubeName).isScheduledAtRestart())
    {
      @Override
      protected void run()
      {
        odrDatacube.preview();
      }
    };
    
    if(Deployment.getDatacubeJobsScheduling().get(datacubeName).isEnabled() && job.isProperlyConfigured()) {
      scheduler.schedule(job);
      return nextAvailableID + 1;
    } 
    else {
      return nextAvailableID;
    }
  }
  
  /*
   * ODR definitive
   *
   * This will generated a datacube every day from the detailedrecords_offers-YYYY-MM-dd index of the previous day.
   */
  private static long scheduleODRDefinitive(JobScheduler scheduler, long nextAvailableID) {
    String datacubeName = "ODR-definitive";
    
    ScheduledJob job = new ScheduledJob(nextAvailableID,
        datacubeName, 
        Deployment.getDatacubeJobsScheduling().get(datacubeName).getCronEntry(), 
        Deployment.getBaseTimeZone(),
        Deployment.getDatacubeJobsScheduling().get(datacubeName).isScheduledAtRestart())
    {
      @Override
      protected void run()
      {
        odrDatacube.definitive();
      }
    };
    
    if(Deployment.getDatacubeJobsScheduling().get(datacubeName).isEnabled() && job.isProperlyConfigured()) {
      scheduler.schedule(job);
      return nextAvailableID + 1;
    } 
    else {
      return nextAvailableID;
    }
  }

  /*
   * BDR preview
   *
   * This will generated a datacube preview of the day from the detailedrecords_bonuses-YYYY-MM-dd index of the day
   * Those data are not definitive, the day is not ended yet, new BDR can still be added.
   */
  private static long scheduleBDRPreview(JobScheduler scheduler, long nextAvailableID) {
    String datacubeName = "BDR-preview";
    
    ScheduledJob job = new ScheduledJob(nextAvailableID,
        datacubeName, 
        Deployment.getDatacubeJobsScheduling().get(datacubeName).getCronEntry(), 
        Deployment.getBaseTimeZone(),
        Deployment.getDatacubeJobsScheduling().get(datacubeName).isScheduledAtRestart())
    {
      @Override
      protected void run()
      {
        bdrDatacube.preview();
      }
    };
    
    if(Deployment.getDatacubeJobsScheduling().get(datacubeName).isEnabled() && job.isProperlyConfigured()) {
      scheduler.schedule(job);
      return nextAvailableID + 1;
    } 
    else {
      return nextAvailableID;
    }
  }
  
  /*
   * BDR definitive
   *
   * This will generated a datacube every day from the detailedrecords_bonuses-YYYY-MM-dd index of the previous day.
   */
  private static long scheduleBDRDefinitive(JobScheduler scheduler, long nextAvailableID) {
    String datacubeName = "BDR-definitive";
    
    ScheduledJob job = new ScheduledJob(nextAvailableID,
        datacubeName, 
        Deployment.getDatacubeJobsScheduling().get(datacubeName).getCronEntry(), 
        Deployment.getBaseTimeZone(),
        Deployment.getDatacubeJobsScheduling().get(datacubeName).isScheduledAtRestart())
    {
      @Override
      protected void run()
      {
        bdrDatacube.definitive();
      }
    };
    
    if(Deployment.getDatacubeJobsScheduling().get(datacubeName).isEnabled() && job.isProperlyConfigured()) {
      scheduler.schedule(job);
      return nextAvailableID + 1;
    } 
    else {
      return nextAvailableID;
    }
  }

  /*
   * MDR preview
   *
   * This will generated a datacube preview of the day from the detailedrecords_messages-YYYY-MM-dd index of the day
   * Those data are not definitive, the day is not ended yet, new MDR can still be added.
   */
  private static long scheduleMDRPreview(JobScheduler scheduler, long nextAvailableID) {
    String datacubeName = "MDR-preview";
    
    ScheduledJob job = new ScheduledJob(nextAvailableID,
        datacubeName, 
        Deployment.getDatacubeJobsScheduling().get(datacubeName).getCronEntry(), 
        Deployment.getBaseTimeZone(),
        Deployment.getDatacubeJobsScheduling().get(datacubeName).isScheduledAtRestart())
    {
      @Override
      protected void run()
      {
        mdrDatacube.preview();
      }
    };
    
    if(Deployment.getDatacubeJobsScheduling().get(datacubeName).isEnabled() && job.isProperlyConfigured()) {
      scheduler.schedule(job);
      return nextAvailableID + 1;
    } 
    else {
      return nextAvailableID;
    }
  }
  
  /*
   * MDR definitive
   *
   * This will generated a datacube every day from the detailedrecords_messages-YYYY-MM-dd index of the previous day.
   */
  private static long scheduleMDRDefinitive(JobScheduler scheduler, long nextAvailableID) {
    String datacubeName = "MDR-definitive";
    
    ScheduledJob job = new ScheduledJob(nextAvailableID,
        datacubeName, 
        Deployment.getDatacubeJobsScheduling().get(datacubeName).getCronEntry(), 
        Deployment.getBaseTimeZone(),
        Deployment.getDatacubeJobsScheduling().get(datacubeName).isScheduledAtRestart())
    {
      @Override
      protected void run()
      {
        mdrDatacube.definitive();
      }
    };
    
    if(Deployment.getDatacubeJobsScheduling().get(datacubeName).isEnabled() && job.isProperlyConfigured()) {
      scheduler.schedule(job);
      return nextAvailableID + 1;
    } 
    else {
      return nextAvailableID;
    }
  }

  /*
   * Journey datacube
   *
   * This will generated both datacube_journeytraffic and datacube_journeyrewards every hour from journeystatistic indexes
   * /!\ Do not launch at start in production, there is no override mechanism for this datacube (no preview)
   * /!\ Do not configure a cron period lower than 1 hour (require code changes)
   */
  private static long scheduleJourneyDatacubeDefinitive(JobScheduler scheduler, long nextAvailableID) {
    String datacubeName = "Journeys";
    
    ScheduledJob job = new ScheduledJob(nextAvailableID,
        datacubeName, 
        Deployment.getDatacubeJobsScheduling().get(datacubeName).getCronEntry(), 
        Deployment.getBaseTimeZone(),
        Deployment.getDatacubeJobsScheduling().get(datacubeName).isScheduledAtRestart()) 
    {
      @Override
      protected void run()
      {
        // We need to push all journey datacubes at the same timestamp.
        // For the moment we truncate at the HOUR. 
        // Therefore, we must not configure a cron period lower than 1 hour
        // If we want a lower period we will need to retrieve the schedule due date from the job !
        Date now = SystemTime.getCurrentTime();
        Date truncatedHour = RLMDateUtils.truncate(now, Calendar.HOUR, Deployment.getBaseTimeZone());
        Date endOfLastHour = RLMDateUtils.addMilliseconds(truncatedHour, -1); // XX:59:59.999
        
        journeysMap.update();
        for(String journeyID : journeysMap.keySet()) {
          trafficDatacube.definitive(journeyID, journeysMap.getStartDateTime(journeyID), endOfLastHour);
          rewardsDatacube.definitive(journeyID, journeysMap.getStartDateTime(journeyID), endOfLastHour);
        }
      }
    };
    
    if(Deployment.getDatacubeJobsScheduling().get(datacubeName).isEnabled() && job.isProperlyConfigured()) {
      scheduler.schedule(job);
      return nextAvailableID + 1;
    } 
    else {
      return nextAvailableID;
    }
  }
  
  /*****************************************
  *
  * run
  *
  *****************************************/
  public void run()
  {
    JobScheduler datacubeScheduler = new JobScheduler("datacube");
    
    //
    // Adding datacubes scheduling
    //
    long uniqueID = 0;
    
    //
    // Datacube previews (will be updated later by the definitive version)
    //
    uniqueID = scheduleLoyaltyProgramsPreview(datacubeScheduler, uniqueID);
    uniqueID = scheduleSubscriberProfilePreview(datacubeScheduler, uniqueID);
    uniqueID = scheduleODRPreview(datacubeScheduler, uniqueID);
    uniqueID = scheduleBDRPreview(datacubeScheduler, uniqueID);
    uniqueID = scheduleMDRPreview(datacubeScheduler, uniqueID);
    
    //
    // Definitives datacubes 
    //
    uniqueID = scheduleLoyaltyProgramsDefinitive(datacubeScheduler, uniqueID);
    uniqueID = scheduleSubscriberProfileDefinitive(datacubeScheduler, uniqueID);
    uniqueID = scheduleODRDefinitive(datacubeScheduler, uniqueID);
    uniqueID = scheduleBDRDefinitive(datacubeScheduler, uniqueID);
    uniqueID = scheduleMDRDefinitive(datacubeScheduler, uniqueID);
    uniqueID = scheduleJourneyDatacubeDefinitive(datacubeScheduler, uniqueID);
    
    //
    // Snapshots
    //
    ScheduledJob subscriberprofileSnapshot = new SubscriberProfileSnapshot(uniqueID++, elasticsearchRestClient);
    if(subscriberprofileSnapshot.isProperlyConfigured())
      {
        datacubeScheduler.schedule(subscriberprofileSnapshot);
      }

    log.info("Starting scheduler");
    datacubeScheduler.runScheduler();
  }

  /*****************************************
  *
  * class ShutdownHook
  *
  *****************************************/
  private static class ShutdownHook implements NGLMRuntime.NGLMShutdownHook
  {
    private DatacubeManager datacubemanager;

    private ShutdownHook(DatacubeManager datacubemanager)
    {
      this.datacubemanager = datacubemanager;
    }

    @Override public void shutdown(boolean normalShutdown)
    {
      datacubemanager.shutdownUCGEngine(normalShutdown);
    }
  }

  /****************************************
  *
  * shutdownUCGEngine
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
    offerObjectiveService.stop();
    
    /*****************************************
    *
    *  log
    *
    *****************************************/
    log.info("Stopped DatacubeManager");
  }

  /*****************************************
  *
  * Engine
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
