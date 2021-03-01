/****************************************************************************
*
*  DatacubeManager.java 
*
****************************************************************************/

package com.evolving.nglm.evolution.datacubes;

import com.evolving.nglm.core.*;
import com.evolving.nglm.evolution.CriterionContext;
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
import com.evolving.nglm.evolution.ScheduledJobConfiguration;
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
import java.util.Map;

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
  private static SubscriberMessageTemplateService subscriberMessageTemplateService;
  
  //
  // Elasticsearch Client
  //
  private static ElasticsearchClientAPI elasticsearchRestClient;

  //
  // Datacube writer
  //
  private static DatacubeWriter datacubeWriter;
  
  //
  // Maps
  //
  private static JourneysMap journeysMap;
  
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
  }

  /*****************************************
  *
  * getters
  *
  *****************************************/
  // Those getters return the static instance of corresponding service.
  // But because those getters can only be called with an instantiated DatacubeManager, therefore we are sure all Services are started.  
  public DynamicCriterionFieldService getDynamicCriterionFieldService() { return dynamicCriterionFieldService; }
  public JourneyService getJourneyService() { return journeyService; }
  public LoyaltyProgramService getLoyaltyProgramService() { return loyaltyProgramService; }
  public SegmentationDimensionService getSegmentationDimensionService() { return segmentationDimensionService; }
  public OfferService getOfferService() { return offerService; }
  public SalesChannelService getSalesChannelService() { return salesChannelService; }
  public PaymentMeanService getPaymentMeanService() { return paymentMeanService; }
  public OfferObjectiveService getOfferObjectiveService() { return offerObjectiveService; }
  public SubscriberMessageTemplateService getSubscriberMessageTemplateService() { return subscriberMessageTemplateService; }
  
  public ElasticsearchClientAPI getElasticsearchClientAPI() { return elasticsearchRestClient; }
  
  public DatacubeWriter getDatacubeWriter() { return datacubeWriter; }
  
  public JourneysMap getJourneysMap() { return journeysMap; }
  
  /*****************************************
  *
  * run
  *
  *****************************************/
  public void run()
  {
    JobScheduler datacubeScheduler = new JobScheduler("datacube");
    
    //
    // Set all jobs from configuration
    //
    for(Integer tenantID: Deployment.getTenantIDs()) {
      Map<String, ScheduledJobConfiguration> jobConfigs = Deployment.getDeployment(tenantID).getDatacubeJobsScheduling();
      if(jobConfigs == null) {
        continue;
      }
      
      for(String jobID: jobConfigs.keySet()) {
        ScheduledJobConfiguration config = jobConfigs.get(jobID);
        
        if(config.isEnabled()) {
          ScheduledJob job = DatacubeJobs.createDatacubeJob(config, this);
          datacubeScheduler.schedule(job);
        }
      }
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
