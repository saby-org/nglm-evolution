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
import com.evolving.nglm.evolution.elasticsearch.ElasticsearchClientAPI;

import java.util.Calendar;
import java.util.Date;

import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
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
  private static ElasticsearchClientAPI elasticsearchRestClient;
  private static SubscriberMessageTemplateService subscriberMessageTemplateService;

  //
  // Datacube writer
  //
  private static DatacubeWriter datacubeWriter;
  
  //
  // Maps
  //
  private static JourneysMap journeysMap;
  
  //
  // Datacube generators - Those classes are NOT thread-safe and must be used by only one thread.
  //
  private static JourneyTrafficDatacubeGenerator trafficDatacube;
  private static JourneyRewardsDatacubeGenerator rewardsDatacube;
  private static ProgramsHistoryDatacubeGenerator loyaltyHistoryDatacubePreview;
  private static ProgramsHistoryDatacubeGenerator loyaltyHistoryDatacubeDefinitive;
  private static ProgramsChangesDatacubeGenerator tierChangesDatacubePreview;
  private static ProgramsChangesDatacubeGenerator tierChangesDatacubeDefinitive;
  private static ODRDatacubeGenerator dailyOdrDatacubePreview;
  private static ODRDatacubeGenerator dailyOdrDatacubeDefinitive;
  private static ODRDatacubeGenerator hourlyOdrDatacubePreview;
  private static ODRDatacubeGenerator hourlyOdrDatacubeDefinitive;
  private static BDRDatacubeGenerator dailyBdrDatacubePreview;
  private static BDRDatacubeGenerator dailyBdrDatacubeDefinitive;
  private static BDRDatacubeGenerator hourlyBdrDatacubePreview;
  private static BDRDatacubeGenerator hourlyBdrDatacubeDefinitive;
  private static MDRDatacubeGenerator dailyMdrDatacubePreview;
  private static MDRDatacubeGenerator dailyMdrDatacubeDefinitive;
  private static MDRDatacubeGenerator hourlyMdrDatacubePreview;
  private static MDRDatacubeGenerator hourlyMdrDatacubeDefinitive;
  private static SubscriberProfileDatacubeGenerator subscriberProfileDatacubePreview;
  private static SubscriberProfileDatacubeGenerator subscriberProfileDatacubeDefinitive;
  
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
    int connectTimeout = Deployment.getElasticsearchConnectionSettings().get("DatacubeManager").getConnectTimeout();
    int queryTimeout = Deployment.getElasticsearchConnectionSettings().get("DatacubeManager").getQueryTimeout();
    String userName = args[5];
    String userPassword = args[6];
    
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
    try
      {
        elasticsearchRestClient = new ElasticsearchClientAPI(elasticsearchServerHost, elasticsearchServerPort, connectTimeout, queryTimeout, userName, userPassword);
      }
    catch (ElasticsearchException e)
      {
        throw new ServerRuntimeException("could not initialize elasticsearch client", e);
      }
    
    //
    // Datacube writer
    //
    datacubeWriter = new DatacubeWriter(elasticsearchRestClient);
    
    //
    // Maps 
    //
    journeysMap = new JourneysMap(journeyService);
    
    //
    // Datacube generators - one instance per usage (because it is not thread safe)
    //
    trafficDatacube = new JourneyTrafficDatacubeGenerator("Journey:Traffic", elasticsearchRestClient, datacubeWriter, segmentationDimensionService, journeyService);
    rewardsDatacube = new JourneyRewardsDatacubeGenerator("Journey:Rewards", elasticsearchRestClient, datacubeWriter, segmentationDimensionService, journeyService);
    loyaltyHistoryDatacubePreview = new ProgramsHistoryDatacubeGenerator("LoyaltyPrograms:History(Preview)", elasticsearchRestClient, datacubeWriter, loyaltyProgramService);
    loyaltyHistoryDatacubeDefinitive = new ProgramsHistoryDatacubeGenerator("LoyaltyPrograms:History(Definitive)", elasticsearchRestClient, datacubeWriter, loyaltyProgramService);
    tierChangesDatacubePreview = new ProgramsChangesDatacubeGenerator("LoyaltyPrograms:Changes(Preview)", elasticsearchRestClient, datacubeWriter, loyaltyProgramService);
    tierChangesDatacubeDefinitive = new ProgramsChangesDatacubeGenerator("LoyaltyPrograms:Changes(Definitive)", elasticsearchRestClient, datacubeWriter, loyaltyProgramService);
    dailyOdrDatacubePreview = new ODRDatacubeGenerator("ODR:Daily(Preview)", elasticsearchRestClient, datacubeWriter, offerService, salesChannelService, paymentMeanService, offerObjectiveService, loyaltyProgramService, journeyService);
    dailyOdrDatacubeDefinitive = new ODRDatacubeGenerator("ODR:Daily(Definitive)", elasticsearchRestClient, datacubeWriter, offerService, salesChannelService, paymentMeanService, offerObjectiveService, loyaltyProgramService, journeyService);
    hourlyOdrDatacubePreview = new ODRDatacubeGenerator("ODR:Hourly(Preview)", elasticsearchRestClient, datacubeWriter, offerService, salesChannelService, paymentMeanService, offerObjectiveService, loyaltyProgramService, journeyService);
    hourlyOdrDatacubeDefinitive = new ODRDatacubeGenerator("ODR:Hourly(Definitive)", elasticsearchRestClient, datacubeWriter, offerService, salesChannelService, paymentMeanService, offerObjectiveService, loyaltyProgramService, journeyService);
    dailyBdrDatacubePreview = new BDRDatacubeGenerator("BDR:Daily(Preview)", elasticsearchRestClient, datacubeWriter, offerService, offerObjectiveService, loyaltyProgramService, journeyService);
    dailyBdrDatacubeDefinitive = new BDRDatacubeGenerator("BDR:Daily(Definitive)", elasticsearchRestClient, datacubeWriter, offerService, offerObjectiveService, loyaltyProgramService, journeyService);
    hourlyBdrDatacubePreview = new BDRDatacubeGenerator("BDR:Hourly(Preview)", elasticsearchRestClient, datacubeWriter, offerService, offerObjectiveService, loyaltyProgramService, journeyService);
    hourlyBdrDatacubeDefinitive = new BDRDatacubeGenerator("BDR:Hourly(Definitive)", elasticsearchRestClient, datacubeWriter, offerService, offerObjectiveService, loyaltyProgramService, journeyService);
    dailyMdrDatacubePreview = new MDRDatacubeGenerator("MDR:Daily(Preview)", elasticsearchRestClient, datacubeWriter, offerService, offerObjectiveService, loyaltyProgramService, journeyService, subscriberMessageTemplateService);
    dailyMdrDatacubeDefinitive = new MDRDatacubeGenerator("MDR:Daily(Definitive)", elasticsearchRestClient, datacubeWriter, offerService, offerObjectiveService, loyaltyProgramService, journeyService, subscriberMessageTemplateService);
    hourlyMdrDatacubePreview = new MDRDatacubeGenerator("MDR:Hourly(Preview)", elasticsearchRestClient, datacubeWriter, offerService, offerObjectiveService, loyaltyProgramService, journeyService, subscriberMessageTemplateService);
    hourlyMdrDatacubeDefinitive = new MDRDatacubeGenerator("MDR:Hourly(Definitive)", elasticsearchRestClient, datacubeWriter, offerService, offerObjectiveService, loyaltyProgramService, journeyService, subscriberMessageTemplateService);
    subscriberProfileDatacubePreview = new SubscriberProfileDatacubeGenerator("SubscriberProfile(Preview)", elasticsearchRestClient, datacubeWriter, segmentationDimensionService);
    subscriberProfileDatacubeDefinitive = new SubscriberProfileDatacubeGenerator("SubscriberProfile(Definitive)", elasticsearchRestClient, datacubeWriter, segmentationDimensionService);
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
    String jobName = "LoyaltyPrograms-preview";
    
    AsyncScheduledJob job = new AsyncScheduledJob(nextAvailableID,
        jobName, 
        Deployment.getDatacubeJobsScheduling().get(jobName).getCronEntry(), 
        Deployment.getBaseTimeZone(),
        Deployment.getDatacubeJobsScheduling().get(jobName).isScheduledAtRestart())
    {
      @Override
      protected void asyncRun()
      {
        loyaltyHistoryDatacubePreview.preview();
        tierChangesDatacubePreview.preview();
      }
    };
    
    if(Deployment.getDatacubeJobsScheduling().get(jobName).isEnabled() && job.isProperlyConfigured()) {
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
    String jobName = "LoyaltyPrograms-definitive";
    
    AsyncScheduledJob job = new AsyncScheduledJob(nextAvailableID,
        jobName, 
        Deployment.getDatacubeJobsScheduling().get(jobName).getCronEntry(), 
        Deployment.getBaseTimeZone(),
        Deployment.getDatacubeJobsScheduling().get(jobName).isScheduledAtRestart())
    {
      @Override
      protected void asyncRun()
      {
        loyaltyHistoryDatacubeDefinitive.definitive();
        tierChangesDatacubeDefinitive.definitive();
      }
    };
    
    if(Deployment.getDatacubeJobsScheduling().get(jobName).isEnabled() && job.isProperlyConfigured()) {
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
    String jobName = "SubscriberProfile-preview";
    
    AsyncScheduledJob job = new AsyncScheduledJob(nextAvailableID,
        jobName, 
        Deployment.getDatacubeJobsScheduling().get(jobName).getCronEntry(), 
        Deployment.getBaseTimeZone(),
        Deployment.getDatacubeJobsScheduling().get(jobName).isScheduledAtRestart())
    {
      @Override
      protected void asyncRun()
      {
        subscriberProfileDatacubePreview.preview();
      }
    };
    
    if(Deployment.getDatacubeJobsScheduling().get(jobName).isEnabled() && job.isProperlyConfigured()) {
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
    String jobName = "SubscriberProfile-definitive";
    
    AsyncScheduledJob job = new AsyncScheduledJob(nextAvailableID,
        jobName, 
        Deployment.getDatacubeJobsScheduling().get(jobName).getCronEntry(), 
        Deployment.getBaseTimeZone(),
        Deployment.getDatacubeJobsScheduling().get(jobName).isScheduledAtRestart())
    {
      @Override
      protected void asyncRun()
      {
        subscriberProfileDatacubeDefinitive.definitive();
      }
    };
    
    if(Deployment.getDatacubeJobsScheduling().get(jobName).isEnabled() && job.isProperlyConfigured()) {
      scheduler.schedule(job);
      return nextAvailableID + 1;
    } 
    else {
      return nextAvailableID;
    }
  }
  
  /*
   * ODR daily preview
   *
   * This will generated a datacube preview of the day from the detailedrecords_offers-YYYY-MM-dd index of the current day
   * Those data are not definitive, the day is not ended yet, new ODR can still be added.
   */
  private static long scheduleODRDailyPreview(JobScheduler scheduler, long nextAvailableID) {
    String jobName = "ODR-daily-preview";
    
    AsyncScheduledJob job = new AsyncScheduledJob(nextAvailableID,
        jobName, 
        Deployment.getDatacubeJobsScheduling().get(jobName).getCronEntry(), 
        Deployment.getBaseTimeZone(),
        Deployment.getDatacubeJobsScheduling().get(jobName).isScheduledAtRestart())
    {
      @Override
      protected void asyncRun()
      {
        dailyOdrDatacubePreview.dailyPreview();
      }
    };
    
    if(Deployment.getDatacubeJobsScheduling().get(jobName).isEnabled() && job.isProperlyConfigured()) {
      scheduler.schedule(job);
      return nextAvailableID + 1;
    } 
    else {
      return nextAvailableID;
    }
  }
  
  /*
   * ODR daily definitive
   *
   * This will generated a datacube every day from the detailedrecords_offers-YYYY-MM-dd index of the previous day.
   */
  private static long scheduleODRDailyDefinitive(JobScheduler scheduler, long nextAvailableID) {
    String jobName = "ODR-daily-definitive";
    
    AsyncScheduledJob job = new AsyncScheduledJob(nextAvailableID,
        jobName, 
        Deployment.getDatacubeJobsScheduling().get(jobName).getCronEntry(), 
        Deployment.getBaseTimeZone(),
        Deployment.getDatacubeJobsScheduling().get(jobName).isScheduledAtRestart())
    {
      @Override
      protected void asyncRun()
      {
        dailyOdrDatacubeDefinitive.dailyDefinitive();
      }
    };
    
    if(Deployment.getDatacubeJobsScheduling().get(jobName).isEnabled() && job.isProperlyConfigured()) {
      scheduler.schedule(job);
      return nextAvailableID + 1;
    } 
    else {
      return nextAvailableID;
    }
  }
  
  
  /*
   * ODR hourly preview
   *
   * This will generated a datacube preview of every hour from the detailedrecords_offers-YYYY-MM-dd index of the current day
   * Those data are not definitive, the day is not ended yet, new ODR can still be added.
   */
  private static long scheduleODRHourlyPreview(JobScheduler scheduler, long nextAvailableID) {
    String jobName = "ODR-hourly-preview";
    
    AsyncScheduledJob job = new AsyncScheduledJob(nextAvailableID,
        jobName, 
        Deployment.getDatacubeJobsScheduling().get(jobName).getCronEntry(), 
        Deployment.getBaseTimeZone(),
        Deployment.getDatacubeJobsScheduling().get(jobName).isScheduledAtRestart())
    {
      @Override
      protected void asyncRun()
      {
        hourlyOdrDatacubePreview.hourlyPreview();
      }
    };
    
    if(Deployment.getDatacubeJobsScheduling().get(jobName).isEnabled() && job.isProperlyConfigured()) {
      scheduler.schedule(job);
      return nextAvailableID + 1;
    } 
    else {
      return nextAvailableID;
    }
  }
  
  /*
   * ODR hourly definitive
   *
   * This will generated a datacube of every hour from the detailedrecords_offers-YYYY-MM-dd index of the previous day.
   */
  private static long scheduleODRHourlyDefinitive(JobScheduler scheduler, long nextAvailableID) {
    String jobName = "ODR-hourly-definitive";
    
    AsyncScheduledJob job = new AsyncScheduledJob(nextAvailableID,
        jobName, 
        Deployment.getDatacubeJobsScheduling().get(jobName).getCronEntry(), 
        Deployment.getBaseTimeZone(),
        Deployment.getDatacubeJobsScheduling().get(jobName).isScheduledAtRestart())
    {
      @Override
      protected void asyncRun()
      {
        hourlyOdrDatacubeDefinitive.hourlyDefinitive();
      }
    };
    
    if(Deployment.getDatacubeJobsScheduling().get(jobName).isEnabled() && job.isProperlyConfigured()) {
      scheduler.schedule(job);
      return nextAvailableID + 1;
    } 
    else {
      return nextAvailableID;
    }
  }

  /*
   * BDR daily preview
   *
   * This will generated a datacube preview of the day from the detailedrecords_bonuses-YYYY-MM-dd index of the day
   * Those data are not definitive, the day is not ended yet, new BDR can still be added.
   */
  private static long scheduleBDRDailyPreview(JobScheduler scheduler, long nextAvailableID) {
    String jobName = "BDR-daily-preview";
    
    AsyncScheduledJob job = new AsyncScheduledJob(nextAvailableID,
        jobName, 
        Deployment.getDatacubeJobsScheduling().get(jobName).getCronEntry(), 
        Deployment.getBaseTimeZone(),
        Deployment.getDatacubeJobsScheduling().get(jobName).isScheduledAtRestart())
    {
      @Override
      protected void asyncRun()
      {
        dailyBdrDatacubePreview.dailyPreview();
      }
    };
    
    if(Deployment.getDatacubeJobsScheduling().get(jobName).isEnabled() && job.isProperlyConfigured()) {
      scheduler.schedule(job);
      return nextAvailableID + 1;
    } 
    else {
      return nextAvailableID;
    }
  }
  
  /*
   * BDR daily definitive
   *
   * This will generated a datacube every day from the detailedrecords_bonuses-YYYY-MM-dd index of the previous day.
   */
  private static long scheduleBDRDailyDefinitive(JobScheduler scheduler, long nextAvailableID) {
    String jobName = "BDR-daily-definitive";
    
    AsyncScheduledJob job = new AsyncScheduledJob(nextAvailableID,
        jobName, 
        Deployment.getDatacubeJobsScheduling().get(jobName).getCronEntry(), 
        Deployment.getBaseTimeZone(),
        Deployment.getDatacubeJobsScheduling().get(jobName).isScheduledAtRestart())
    {
      @Override
      protected void asyncRun()
      {
        dailyBdrDatacubeDefinitive.dailyDefinitive();
      }
    };
    
    if(Deployment.getDatacubeJobsScheduling().get(jobName).isEnabled() && job.isProperlyConfigured()) {
      scheduler.schedule(job);
      return nextAvailableID + 1;
    } 
    else {
      return nextAvailableID;
    }
  }
  
  /*
   * BDR hourly preview
   *
   * This will generated a datacube preview of every hour from the detailedrecords_bonuses-YYYY-MM-dd index of the current day
   * Those data are not definitive, the day is not ended yet, new BDR can still be added.
   */
  private static long scheduleBDRHourlyPreview(JobScheduler scheduler, long nextAvailableID) {
    String jobName = "BDR-hourly-preview";
    
    AsyncScheduledJob job = new AsyncScheduledJob(nextAvailableID,
        jobName, 
        Deployment.getDatacubeJobsScheduling().get(jobName).getCronEntry(), 
        Deployment.getBaseTimeZone(),
        Deployment.getDatacubeJobsScheduling().get(jobName).isScheduledAtRestart())
    {
      @Override
      protected void asyncRun()
      {
        hourlyBdrDatacubePreview.hourlyPreview();
      }
    };
    
    if(Deployment.getDatacubeJobsScheduling().get(jobName).isEnabled() && job.isProperlyConfigured()) {
      scheduler.schedule(job);
      return nextAvailableID + 1;
    } 
    else {
      return nextAvailableID;
    }
  }
  
  /*
   * BDR hourly definitive
   *
   * This will generated a datacube of every hour from the detailedrecords_bonuses-YYYY-MM-dd index of the previous day.
   */
  private static long scheduleBDRHourlyDefinitive(JobScheduler scheduler, long nextAvailableID) {
    String jobName = "BDR-hourly-definitive";
    
    AsyncScheduledJob job = new AsyncScheduledJob(nextAvailableID,
        jobName, 
        Deployment.getDatacubeJobsScheduling().get(jobName).getCronEntry(), 
        Deployment.getBaseTimeZone(),
        Deployment.getDatacubeJobsScheduling().get(jobName).isScheduledAtRestart())
    {
      @Override
      protected void asyncRun()
      {
        hourlyBdrDatacubeDefinitive.hourlyDefinitive();
      }
    };
    
    if(Deployment.getDatacubeJobsScheduling().get(jobName).isEnabled() && job.isProperlyConfigured()) {
      scheduler.schedule(job);
      return nextAvailableID + 1;
    } 
    else {
      return nextAvailableID;
    }
  }

  /*
   * MDR daily preview
   *
   * This will generated a datacube preview of the day from the detailedrecords_messages-YYYY-MM-dd index of the day
   * Those data are not definitive, the day is not ended yet, new MDR can still be added.
   */
  private static long scheduleMDRDailyPreview(JobScheduler scheduler, long nextAvailableID) {
    String jobName = "MDR-daily-preview";
    
    AsyncScheduledJob job = new AsyncScheduledJob(nextAvailableID,
        jobName, 
        Deployment.getDatacubeJobsScheduling().get(jobName).getCronEntry(), 
        Deployment.getBaseTimeZone(),
        Deployment.getDatacubeJobsScheduling().get(jobName).isScheduledAtRestart())
    {
      @Override
      protected void asyncRun()
      {
        dailyMdrDatacubePreview.dailyPreview();
      }
    };
    
    if(Deployment.getDatacubeJobsScheduling().get(jobName).isEnabled() && job.isProperlyConfigured()) {
      scheduler.schedule(job);
      return nextAvailableID + 1;
    } 
    else {
      return nextAvailableID;
    }
  }
  
  /*
   * MDR daily definitive
   *
   * This will generated a datacube every day from the detailedrecords_messages-YYYY-MM-dd index of the previous day.
   */
  private static long scheduleMDRDailyDefinitive(JobScheduler scheduler, long nextAvailableID) {
    String jobName = "MDR-daily-definitive";
    
    AsyncScheduledJob job = new AsyncScheduledJob(nextAvailableID,
        jobName, 
        Deployment.getDatacubeJobsScheduling().get(jobName).getCronEntry(), 
        Deployment.getBaseTimeZone(),
        Deployment.getDatacubeJobsScheduling().get(jobName).isScheduledAtRestart())
    {
      @Override
      protected void asyncRun()
      {
        dailyMdrDatacubeDefinitive.dailyDefinitive();
      }
    };
    
    if(Deployment.getDatacubeJobsScheduling().get(jobName).isEnabled() && job.isProperlyConfigured()) {
      scheduler.schedule(job);
      return nextAvailableID + 1;
    } 
    else {
      return nextAvailableID;
    }
  }
  
  /*
   * MDR hourly preview
   *
   * This will generated a datacube preview of every hour from the detailedrecords_messages-YYYY-MM-dd index of the current day
   * Those data are not definitive, the day is not ended yet, new MDR can still be added.
   */
  private static long scheduleMDRHourlyPreview(JobScheduler scheduler, long nextAvailableID) {
    String jobName = "MDR-hourly-preview";
    
    AsyncScheduledJob job = new AsyncScheduledJob(nextAvailableID,
        jobName, 
        Deployment.getDatacubeJobsScheduling().get(jobName).getCronEntry(), 
        Deployment.getBaseTimeZone(),
        Deployment.getDatacubeJobsScheduling().get(jobName).isScheduledAtRestart())
    {
      @Override
      protected void asyncRun()
      {
        hourlyMdrDatacubePreview.hourlyPreview();
      }
    };
    
    if(Deployment.getDatacubeJobsScheduling().get(jobName).isEnabled() && job.isProperlyConfigured()) {
      scheduler.schedule(job);
      return nextAvailableID + 1;
    } 
    else {
      return nextAvailableID;
    }
  }
  
  /*
   * MDR hourly definitive
   *
   * This will generated a datacube of every hour from the detailedrecords_messages-YYYY-MM-dd index of the previous day.
   */
  private static long scheduleMDRHourlyDefinitive(JobScheduler scheduler, long nextAvailableID) {
    String jobName = "MDR-hourly-definitive";
    
    AsyncScheduledJob job = new AsyncScheduledJob(nextAvailableID,
        jobName, 
        Deployment.getDatacubeJobsScheduling().get(jobName).getCronEntry(), 
        Deployment.getBaseTimeZone(),
        Deployment.getDatacubeJobsScheduling().get(jobName).isScheduledAtRestart())
    {
      @Override
      protected void asyncRun()
      {
        hourlyMdrDatacubeDefinitive.hourlyDefinitive();
      }
    };
    
    if(Deployment.getDatacubeJobsScheduling().get(jobName).isEnabled() && job.isProperlyConfigured()) {
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
    String jobName = "Journeys";
    
    AsyncScheduledJob job = new AsyncScheduledJob(nextAvailableID,
        jobName, 
        Deployment.getDatacubeJobsScheduling().get(jobName).getCronEntry(), 
        Deployment.getBaseTimeZone(),
        Deployment.getDatacubeJobsScheduling().get(jobName).isScheduledAtRestart()) 
    {
      @Override
      protected void asyncRun()
      {
        // We need to push all journey datacubes at the same timestamp.
        // For the moment we truncate at the HOUR. 
        // Therefore, we must not configure a cron period lower than 1 hour
        // If we want a lower period we will need to retrieve the schedule due date from the job !
        Date now = SystemTime.getCurrentTime();
        Date truncatedHour = RLMDateUtils.truncate(now, Calendar.HOUR, Deployment.getFirstDayOfTheWeek(), Deployment.getBaseTimeZone());
        Date endOfLastHour = RLMDateUtils.addMilliseconds(truncatedHour, -1); // XX:59:59.999
       
        journeysMap.update();
        
        // Special: All those datacubes are still made sequentially, therefore we prevent any writing during it to optimize computation time.
        datacubeWriter.pause();
        
        for(String journeyID : journeysMap.keySet()) {
          trafficDatacube.definitive(journeyID, journeysMap.getStartDateTime(journeyID), endOfLastHour);
          rewardsDatacube.definitive(journeyID, journeysMap.getStartDateTime(journeyID), endOfLastHour);
        }
        
        // Restart writing if allowed. Flush all data generated
        datacubeWriter.restart();
      }
    };
    
    if(Deployment.getDatacubeJobsScheduling().get(jobName).isEnabled() && job.isProperlyConfigured()) {
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
    uniqueID = scheduleODRDailyPreview(datacubeScheduler, uniqueID);
    uniqueID = scheduleODRHourlyPreview(datacubeScheduler, uniqueID);
    uniqueID = scheduleBDRDailyPreview(datacubeScheduler, uniqueID);
    uniqueID = scheduleBDRHourlyPreview(datacubeScheduler, uniqueID);
    uniqueID = scheduleMDRDailyPreview(datacubeScheduler, uniqueID);
    uniqueID = scheduleMDRHourlyPreview(datacubeScheduler, uniqueID);
    
    //
    // Definitives datacubes 
    //
    uniqueID = scheduleLoyaltyProgramsDefinitive(datacubeScheduler, uniqueID);
    uniqueID = scheduleSubscriberProfileDefinitive(datacubeScheduler, uniqueID);
    uniqueID = scheduleODRDailyDefinitive(datacubeScheduler, uniqueID);
    uniqueID = scheduleODRHourlyDefinitive(datacubeScheduler, uniqueID);
    uniqueID = scheduleBDRDailyDefinitive(datacubeScheduler, uniqueID);
    uniqueID = scheduleBDRHourlyDefinitive(datacubeScheduler, uniqueID);
    uniqueID = scheduleMDRDailyDefinitive(datacubeScheduler, uniqueID);
    uniqueID = scheduleMDRHourlyDefinitive(datacubeScheduler, uniqueID);
    uniqueID = scheduleJourneyDatacubeDefinitive(datacubeScheduler, uniqueID);

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
