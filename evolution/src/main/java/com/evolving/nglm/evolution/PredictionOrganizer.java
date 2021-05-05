package com.evolving.nglm.evolution;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.LongKey;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.SubscriberPredictions.SubscriberPredictionsRequest;
import com.evolving.nglm.evolution.elasticsearch.ElasticsearchClientAPI;

/**
 * Singleton class. Should be instantiated once.
 * 
 * Organize predictions by: 
 * - reading scheduling settings from PredictionOrder (GUIManagedObject) 
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
  private static String requestTopic = Deployment.getSubscriberPredictionsRequestTopic(); // Topic(SubscriberID, List<SubscriberID>)  
  private static ConnectSerde<LongKey> keySerde = LongKey.serde();
  private static ConnectSerde<SubscriberPredictionsRequest> valueSerde = SubscriberPredictionsRequest.serde();
  private static KafkaProducer<byte[], byte[]> kafkaProducer;
  static {
    Properties producerProperties = new Properties();
    producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Deployment.getBrokerServers());
    producerProperties.put(ProducerConfig.ACKS_CONFIG, "all");
    producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
    producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
    kafkaProducer = new KafkaProducer<byte[], byte[]>(producerProperties);
  }
  
  public static void send(Long predictionID, Set<String> subscriberIDs) {
    try {
      kafkaProducer.send(new ProducerRecord<byte[], byte[]>(requestTopic, 
          keySerde.serializer().serialize(requestTopic, new LongKey(predictionID)),
          valueSerde.serializer().serialize(requestTopic, new SubscriberPredictionsRequest(subscriberIDs)))).get();
    } 
    catch (InterruptedException|ExecutionException e) {
      log.error("Error while trying to push a new prediction request.",e);
    }
  }
  
  /*****************************************
  *
  * Job scheduler
  *
  *****************************************/
  private static JobScheduler predictionJobScheduler;
  private static ElasticsearchClientAPI elasticsearchRestClient;
  private static PredictionOrderService predictionOrderService;

  // Remember : those orders are references from the GUIService, but in fact GUIManagedObject are never modified
  // they are always re-created from ser/de. Theoretically we should deep copy them to be able to detect any modification.
  // But here we can only keep the same reference as the one from GUIService because when a modification occur, 
  // the complete object is replaced.
  // We can directly check if references are equals to detect any modification
  private static Map<String, PredictionOrder> predictionOrders; // all orders               Map(PredictionOrderID, PredictionOrder)
  private static Map<String, ScheduledJob> predictionOrderJobs; // all corresponding jobs   Map(PredictionOrderID, ScheduledJob)
  
  private static void removeScheduledJob(String predictionOrderID) {
    ScheduledJob oldJob = predictionOrderJobs.remove(predictionOrderID);
    log.info("Prediction order (ID="+predictionOrderID+") has been removed.");
    if(oldJob != null) {
      log.info("Removing job ("+oldJob.jobName+") from scheduling.");
      predictionJobScheduler.deschedule(oldJob);
    }
  }
  
  private static void updateScheduledJob(String predictionOrderID, PredictionOrder predictionOrder) {
    ScheduledJob oldJob = predictionOrderJobs.remove(predictionOrderID);
    log.info("Prediction order (ID="+predictionOrderID+") has been modified.");
    if(oldJob != null) {
      log.info("Removing job ("+oldJob.jobName+") from scheduling.");
      predictionJobScheduler.deschedule(oldJob);
    }
    
    int tenantID = predictionOrder.getTenantID();
    ScheduledJobConfiguration config = new ScheduledJobConfiguration("PredictionRequest-"+predictionOrderID, 
        ScheduledJobConfiguration.Type.PredictionRequest, 
        true, // enabled 
        false, // schedule at restart
        "0,5,10,15,20,25,30,35,40,45,50,55 * * * *", // TODO !!!!!!!!!!!!! from frequency 
        tenantID,
        Deployment.getDeployment(tenantID).getTimeZone());
    
    ScheduledJob newJob = new ScheduledJob(config)
    {
      @Override
      protected void run()
      {
        log.info("JOB ------ DO SOMETHING ???? ");
      }
    };
    
    // replace old job by new one
    predictionOrderJobs.put(predictionOrderID, newJob);
    predictionOrders.put(predictionOrderID, predictionOrder);
    log.info("Scheduling new job ("+newJob.jobName+").");
    predictionJobScheduler.schedule(newJob);
  }
  
  public static void start(ElasticsearchClientAPI es, PredictionOrderService orderService) {
    predictionJobScheduler = new JobScheduler("Prediction jobs");
    elasticsearchRestClient = es;
    predictionOrderService = orderService;
    predictionOrders = new HashMap<String, PredictionOrder>();
    predictionOrderJobs = new HashMap<String, ScheduledJob>();

    //
    // Wake-up job: look if there is any new PredictionOrder - this mechanism could be replace by a listener in GUIService (if we had a modifiedListener and newListener)
    //
    ScheduledJobConfiguration config = new ScheduledJobConfiguration("PredictionWakeUp", 
        ScheduledJobConfiguration.Type.PredictionWakeUp, 
        true, // enabled 
        true, // schedule at restart
        "* * * * *", // Every minute
        0,
        Deployment.getDefault().getTimeZone());
    
    predictionJobScheduler.schedule(new ScheduledJob(config)
      {
        @Override
        protected void run()
        {
          Collection<PredictionOrder> orders = predictionOrderService.getActivePredictionOrders(SystemTime.getCurrentTime(), 0); // Tenat 0: retrieve all
          Map<String, PredictionOrder> predictionOrdersCopy = new HashMap<String, PredictionOrder>(predictionOrders); // to keep track removed ones.
          
          //
          // Check if any changes since last wake up
          //
          
          // Check for new / modified
          for(PredictionOrder order: orders) {
            PredictionOrder current = predictionOrdersCopy.remove(order.getGUIManagedObjectID());
            if(order != current) { // This is special, and due to the way GUIManagedObjects work (see comment above for predictionOrders variable)
              updateScheduledJob(order.getGUIManagedObjectID(), order);
            }
          }

          // Check for removed (remaining)
          for(String orderID: predictionOrdersCopy.keySet()) {
            removeScheduledJob(orderID);
          }
        }
      });
    
    predictionJobScheduler.runScheduler();
  }
  
  public static void close() {
    predictionJobScheduler.stop();
    kafkaProducer.close();
  }
}

