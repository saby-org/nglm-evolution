package com.evolving.nglm.evolution;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.LongKey;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.StringKey;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.CommodityDeliveryManager.CommodityDeliveryRequest;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionException;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.SubscriberPredictions.SubscriberPredictionsRequest;
import com.evolving.nglm.evolution.elasticsearch.ElasticsearchClientAPI;

/**
 * Singleton class. Should be instantiated once.
 * 
 * Organize predictions by: 
 * - reading scheduling settings from PredictionSettings (GUIManagedObject) 
 * - scheduling jobs accordingly that extract a sub-list of subscribers and push them in subscriberpredictionsrequest topic
 */
public class PredictionOrganizer
{
  private static final Logger log = LoggerFactory.getLogger(PredictionOrganizer.class);
  
  /*****************************************
  *
  * SubscriberPredictionsRequest producer
  *
  *****************************************/
  private static int BATCH_SIZE = 1000;  
  private static String requestTopic = Deployment.getSubscriberPredictionsRequestTopic();   // Topic(SubscriberID, SubscriberPredictionRequest)
  
  private static ConnectSerde<StringKey> requestKeySerde = StringKey.serde();
  private static ConnectSerde<SubscriberPredictionsRequest> requestValueSerde = SubscriberPredictionsRequest.serde();
  
  private static KafkaProducer<byte[], byte[]> kafkaProducer;
  static {
    Properties producerProperties = new Properties();
    producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Deployment.getBrokerServers());
    producerProperties.put(ProducerConfig.ACKS_CONFIG, "all");
    producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
    producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
    kafkaProducer = new KafkaProducer<byte[], byte[]>(producerProperties);
  }
  
  public static void sendBatch(String predictionID, int executionID, boolean trainingMode, Set<String> subscriberIDs) {
    for (String subscriberID: subscriberIDs) {
      try {
        kafkaProducer.send(new ProducerRecord<byte[], byte[]>(requestTopic, 
            requestKeySerde.serializer().serialize(requestTopic, new StringKey(subscriberID)),
            requestValueSerde.serializer().serialize(requestTopic, new SubscriberPredictionsRequest(subscriberID, predictionID, executionID, trainingMode)))).get();
      } 
      catch (InterruptedException|ExecutionException e) {
        log.error("Error while trying to push a new prediction request.",e);
      }
    }
  }
  
  /*****************************************
  *
  * PredictionSettingsMetadata producer (PredictionSettingsService is read-only for this topic - see ReferenceDataReader)
  *
  *****************************************/
  private static String metadataTopic = Deployment.getPredictionSettingsMetadataTopic();       // Topic(PredictionSettingsID, PredictionSettingsMetadata)
  
  private static ConnectSerde<StringKey> metadataKeySerde = StringKey.serde();
  private static ConnectSerde<PredictionSettingsMetadata> metadataValueSerde = PredictionSettingsMetadata.serde();
  
  /**
   * Update lastExecutionID when all request have been pushed for this executionID
   */
  public static void updateMetadata(String predictionID) 
  {
    // Retrieve the complete object (metadata) from ReferenceDataReader. Therefore only the concerning variable will be overriden.
    PredictionSettingsMetadata metadata = predictionSettingsService.getPredictionSettingsMetadata(predictionID);
    metadata.incrementExecutionID();
    
    try {
      kafkaProducer.send(new ProducerRecord<byte[], byte[]>(metadataTopic, 
          metadataKeySerde.serializer().serialize(metadataTopic, new StringKey(predictionID)),
          metadataValueSerde.serializer().serialize(metadataTopic, metadata))).get();
    } 
    catch (InterruptedException|ExecutionException e) {
      log.error("Error while trying to update prediction settings metadata.",e);
    }
  }
  
  /*****************************************
  *
  * Job scheduler
  *
  *****************************************/
  private static JobScheduler predictionJobScheduler;
  private static ElasticsearchClientAPI elasticsearchRestClient;
  private static PredictionSettingsService predictionSettingsService;

  // Remember : those settings are references from the GUIService, but in fact GUIManagedObject are never modified
  // they are always re-created from ser/de. Theoretically we should deep copy them to be able to detect any modification.
  // But here we can only keep the same reference as the one from GUIService because when a modification occur, 
  // the complete object is replaced.
  // We can directly check if references are equals to detect any modification
  private static Map<String, PredictionSettings> predictions; // all settings             Map(PredictionSettingsID, PredictionSettings)
  private static Map<String, ScheduledJob> predictionJobs;    // all corresponding jobs   Map(PredictionSettingsID, ScheduledJob)
  
  // StartDate: date when this "version" of the job ran for the first time - for "every duration" management.
  // StartDate will be initialize when the job run for the first time, it will be null before.
  private static Map<String, Date> predictionStartDate;
  
  private static void removeScheduledJob(String predictionSettingsID) {
    ScheduledJob oldJob = predictionJobs.remove(predictionSettingsID);
    predictionStartDate.remove(predictionSettingsID);
    log.info("Prediction settings (ID="+predictionSettingsID+") has been removed.");
    if(oldJob != null) {
      log.info("Removing job ("+oldJob.jobName+") from scheduling.");
      predictionJobScheduler.deschedule(oldJob);
    }
  }
  
  private static void updateScheduledJob(String predictionSettingsID, PredictionSettings predictionSettings) {
    ScheduledJob oldJob = predictionJobs.remove(predictionSettingsID);
    predictionStartDate.remove(predictionSettingsID);
    log.info("Prediction settings (ID="+predictionSettingsID+") has been modified.");
    if(oldJob != null) {
      log.info("Removing job ("+oldJob.jobName+") from scheduling.");
      predictionJobScheduler.deschedule(oldJob);
    }
    
    int tenantID = predictionSettings.getTenantID();
    String cronScheduler;
    try {
      cronScheduler = predictionSettings.retrieveCronFrequency();
    } catch (GUIManagerException e) {
      StringWriter stackTraceWriter = new StringWriter();
      e.printStackTrace(new PrintWriter(stackTraceWriter, true));
      log.error("Error while scheduling a new job: "+stackTraceWriter.toString()+"");
      return;
    }
    
    ScheduledJobConfiguration config = new ScheduledJobConfiguration("PredictionRequest-"+predictionSettingsID, 
        ScheduledJobConfiguration.Type.PredictionRequest, 
        true, // enabled 
        false, // schedule at restart
        cronScheduler,
        tenantID,
        Deployment.getDeployment(tenantID).getTimeZone());
    
    String predictionID = predictionSettingsID;
    ScheduledJob newJob = new ScheduledJob(config)
    {
      @Override protected void run() { predictionJobRun(predictionID); }
    };
    
    // replace old job by new one
    predictionJobs.put(predictionSettingsID, newJob);
    predictions.put(predictionSettingsID, predictionSettings);
    log.info("Scheduling new job ("+newJob.jobName+").");
    predictionJobScheduler.schedule(newJob);
  }
  
  public static void start(ElasticsearchClientAPI es, PredictionSettingsService predictionService) {
    predictionJobScheduler = new JobScheduler("Prediction jobs");
    elasticsearchRestClient = es;
    predictionSettingsService = predictionService;
    predictions = new HashMap<String, PredictionSettings>();
    predictionJobs = new HashMap<String, ScheduledJob>();
    predictionStartDate = new HashMap<String, Date>();

    //
    // Wake-up job: look if there is any new PredictionSettings - this mechanism could be replace by a listener in GUIService (if we had a modifiedListener and newListener)
    //
    ScheduledJobConfiguration config = new ScheduledJobConfiguration("PredictionWakeUp", 
        ScheduledJobConfiguration.Type.PredictionWakeUp, 
        true, // enabled 
        true, // schedule at restart
        "* * * * *", // Every minute        // TODO CHANGE AFTER DEV ? 
        0,
        Deployment.getDefault().getTimeZone());
    
    predictionJobScheduler.schedule(new ScheduledJob(config)
      {
        @Override
        protected void run()
        {
          Collection<PredictionSettings> predictionsList = predictionSettingsService.getActivePredictionSettings(SystemTime.getCurrentTime(), 0); // Tenant 0: retrieve all
          Map<String, PredictionSettings> predictionsCopy = new HashMap<String, PredictionSettings>(predictions); // a copy of the list that will contain remaining ones at the end
          
          //
          // Check if any changes since last wake up
          //
          
          // Check for new / modified
          for(PredictionSettings pred: predictionsList) {
            PredictionSettings current = predictionsCopy.remove(pred.getGUIManagedObjectID());
            if(pred != current) { // This is special, and due to the way GUIManagedObjects work (see comment above for predictions variable)
              updateScheduledJob(pred.getGUIManagedObjectID(), pred);
            }
          }

          // Check for removed (remaining)
          for(String predictionID: predictionsCopy.keySet()) {
            removeScheduledJob(predictionID);
          }
        }
      });
    
    predictionJobScheduler.runScheduler();
  }
  
  public static void close() {
    predictionJobScheduler.stop();
    kafkaProducer.close();
  }

  /*****************************************
  *
  * Prediction Job
  *
  *****************************************/
  //
  // predictionJobRun
  //
  public static void predictionJobRun(String predictionID) {
    PredictionSettings prediction = predictions.get(predictionID);
    PredictionSettingsMetadata metadata = predictionSettingsService.getPredictionSettingsMetadata(predictionID);
    
    if(prediction != null) {
      try {
        if(skipRun(predictionID, prediction)) {
          return;
        }
        
        int executionID = metadata.getLastExecutionID() + 1; // +1 because it is the next execution
        getSubscribersAndPush(predictionID, executionID, prediction.getTargetCriteria(), prediction.getTenantID(), elasticsearchRestClient);
        
        // End of requests generation phase - notify metadata (increase lastExecutionID)
        updateMetadata(predictionID);
      } 
      catch (ElasticsearchStatusException | IOException | GUIManagerException e) {
        log.error("Unable to retrieve list of subscribers matching target criteria {}", e.getMessage());
      }
    }
    else {
      log.error("Something wrong happened, lost prediction reference.");
    }
  }
  
  /**
   * skipRun
   * Jobs are scheduled every day/week/month by CRON settings but, the "real" scheduling can be "every 3 weeks". 
   * This cannot be managed by CRON directly (do not fit cron standard), therefore, before each run we need to 
   * check if we skip it or not
   */
  private static boolean skipRun(String predictionID, PredictionSettings predictionSettings) {
    Date now = SystemTime.getCurrentTime();
    Date start = predictionStartDate.get(predictionID);
    if(start == null) {
      //
      // First run
      //
      predictionStartDate.put(predictionID, now);
      return false;
    }
    
    return !predictionSettings.isValidRun(start, now);
  }
  
  // 
  // Elasticsearch search request
  //
  /**
   * Retrieve the list of all subscribers matching a list of EvaluationCriterion
   * Push them in the request topic using send() call
   */
  private static void getSubscribersAndPush(String predictionID, int executionID, List<EvaluationCriterion> criteriaList, int tenantID, ElasticsearchClientAPI elasticsearch) throws IOException, ElasticsearchStatusException, GUIManagerException {
    int scrollSize = Math.min(BATCH_SIZE, ElasticsearchClientAPI.MAX_RESULT_WINDOW); // Capped by max size of ES request (10000)
    
    //
    // Build Elasticsearch request (retrieving subscriberIDs matching criteria)
    //
    BoolQueryBuilder query = EvaluationCriterion.esCountMatchCriteriaGetQuery(criteriaList);
    query.filter().add(QueryBuilders.termQuery("tenantID", tenantID)); // filter to keep only tenant related subscribers.
    
    SearchSourceBuilder searchSourceRequest = new SearchSourceBuilder().query(query);
    log.info(searchSourceRequest.toString());  // TODO debug
    
    SearchRequest searchRequest = new SearchRequest("subscriberprofile").source(searchSourceRequest);
    Scroll scroll = new Scroll(TimeValue.timeValueSeconds(10L));
    searchRequest.scroll(scroll);
    searchRequest.source().size(scrollSize); // batch size
    try
      {
        SearchResponse searchResponse = elasticsearch.search(searchRequest, RequestOptions.DEFAULT);
        String scrollId = searchResponse.getScrollId(); // always null
        SearchHit[] searchHits = searchResponse.getHits().getHits();
        while (searchHits != null && searchHits.length > 0)
          {
            //
            // Construct batch (list of subscriberIDs) from ES hits
            //
            Set<String> batch = new HashSet<String>(); // Will contain subscriberIDs - Set to avoid duplicate

            for (SearchHit hit : searchHits) {
              Map<String, Object> esFields = hit.getSourceAsMap();
              if((String) esFields.get("subscriberID") != null) {
                batch.add((String) esFields.get("subscriberID"));
              }
            }
            
            //
            // push batch - we need to do that in the same function otherwise we will reach heap space (OOM) 
            //
            sendBatch(predictionID, executionID, false, batch); // TODO implements training mode
            
            //
            //  scroll
            //
            
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId); 
            scrollRequest.scroll(scroll);
            searchResponse = elasticsearch.searchScroll(scrollRequest, RequestOptions.DEFAULT);
            scrollId = searchResponse.getScrollId();
            searchHits = searchResponse.getHits().getHits();
          }
      } 
    catch (IOException e)
      {
        log.error("IOException in ES query {}", e.getMessage());
        throw new GUIManagerException(e);
      }
  }
}

