/****************************************************************************
*
*  DeliveryManager.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.StringKey;
import com.evolving.nglm.core.StringValue;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.connect.data.Schema;

import com.rii.utilities.JSONUtilities;
import com.rii.utilities.JSONUtilities.JSONUtilitiesException;
import com.rii.utilities.SystemTime;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

public abstract class DeliveryManager
{
  /*****************************************
  *
  *  enums
  *
  *****************************************/

  //
  //  DeliveryStatus
  //

  public enum DeliveryStatus
  {
    Pending("pending"),
    Delivered("delivered"),
    Indeterminate("indeterminate"),
    Failed("failed"),
    Control("control"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private DeliveryStatus(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static DeliveryStatus fromExternalRepresentation(String externalRepresentation) { for (DeliveryStatus enumeratedValue : DeliveryStatus.values()) { if (enumeratedValue.getExternalRepresentation().equals(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }

  //
  //  DeliveryGuarantee
  //

  public enum DeliveryGuarantee
  {
    AtLeastOnce("atLeastOnce"),
    AtMostOnce("atMostOnce"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private DeliveryGuarantee(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static DeliveryGuarantee fromExternalRepresentation(String externalRepresentation) { for (DeliveryGuarantee enumeratedValue : DeliveryGuarantee.values()) { if (enumeratedValue.getExternalRepresentation().equals(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }

  //
  //  ManagerStatus
  //

  public enum ManagerStatus
  {
    Created,
    Delivering,
    SuspendRequested,
    Suspended,
    ResumeRequested,
    Stopping,
    Aborting,
    Stopped;
    public boolean isRunning() { return EnumSet.of(Created,Delivering,SuspendRequested,Suspended,ResumeRequested).contains(this); }
    public boolean isSuspending() { return EnumSet.of(SuspendRequested).contains(this); }
    public boolean isSuspended() { return EnumSet.of(Created, SuspendRequested,Suspended,ResumeRequested).contains(this); }
    public boolean isWaitingToDeliverRequests() { return EnumSet.of(Created, Suspended).contains(this); }
    public boolean isDeliveringRequests() { return EnumSet.of(Delivering).contains(this); }
    public boolean isProcessingResponses() { return EnumSet.of(Created, Delivering,SuspendRequested,Suspended,ResumeRequested,Stopping).contains(this); }
  }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(DeliveryManager.class);
  
  //
  //  configuration
  //
  
  private String applicationID;
  private String deliveryManagerKey;
  private String bootstrapServers;
  private ConnectSerde<DeliveryRequest> requestSerde;
  private String requestTopic;
  private String responseTopic;
  private String internalTopic;
  private String routingTopic;
  private int deliveryRatePerMinute;
  private DeliveryGuarantee deliveryGuarantee;

  //
  //  derived configuration
  //

  private int millisecondsPerDelivery;
  private int maxOutstandingRequests;

  //
  //  status
  //

  private volatile ManagerStatus managerStatus = ManagerStatus.Created;
  private volatile boolean requestConsumerRunning = false;
  private Date lastSubmitDate = NGLMRuntime.BEGINNING_OF_TIME;

  //
  //  serdes
  //

  private ConnectSerde<StringKey> stringKeySerde = StringKey.serde();
  private ConnectSerde<StringValue> stringValueSerde = StringValue.serde();

  //
  //  transactions
  //

  private List<DeliveryRequest> waitingForRestart = new LinkedList<DeliveryRequest>();
  private Map<String,DeliveryRequest> waitingForAcknowledgement = new HashMap<String,DeliveryRequest>();
  private Map<String,DeliveryRequest> waitingForCorrelatorUpdate = new HashMap<String,DeliveryRequest>();

  //
  //  queue to decouple calls to/from subclass
  //

  private BlockingQueue<DeliveryRequest> submitRequestQueue = new LinkedBlockingQueue<DeliveryRequest>();

  //
  //  consumers
  //

  private KafkaConsumer<byte[], byte[]> requestConsumer = null;
  private KafkaConsumer<byte[], byte[]> routingConsumer = null;
  private KafkaProducer<byte[], byte[]> kafkaProducer = null;
  private Set<TopicPartition> requestConsumerAssignment = new HashSet<TopicPartition>();

  //
  //  threads
  //

  private Thread submitRequestWorkerThread = null;
  private Thread receiveCorrelatorUpdateWorkerThread = null;
  
  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getDeliveryManagerKey() { return deliveryManagerKey; }
  public String getRequestTopic() { return requestTopic; }
  public String getResponseTopic() { return responseTopic; }
  public String getInternalTopic() { return internalTopic; }
  public String getRoutingTopic() { return routingTopic; }
  public int getDeliveryRatePerMinute() { return deliveryRatePerMinute; }
  public DeliveryGuarantee getDeliveryGuarantee() { return deliveryGuarantee; }

  /*****************************************
  *
  *  isProcessing
  *
  *****************************************/

  protected boolean isProcessing() { return managerStatus.isProcessingResponses(); }

  /*****************************************
  *
  *  startDelivery
  *
  *****************************************/

  protected void startDelivery()
  {
    log.info("Starting DeliveryManager " + applicationID + "-" + deliveryManagerKey);
    synchronized (this)
      {
        managerStatus = ManagerStatus.ResumeRequested;
        this.notifyAll();
      }
  }

  /*****************************************
  *
  *  suspendDelivery
  *
  *****************************************/

  protected void suspendDelivery()
  {
    log.info("Suspending DeliveryManager " + applicationID + "-" + deliveryManagerKey);
    synchronized (this)
      {
        managerStatus = ManagerStatus.SuspendRequested;
        this.notifyAll();
      }
  }

  /*****************************************
  *
  *  nextRequest
  *
  *****************************************/

  protected DeliveryRequest nextRequest()
  {
    DeliveryRequest result = null;
    while (managerStatus.isProcessingResponses() && result == null)
      {
        try
          {
            result = submitRequestQueue.take();
          }
        catch (InterruptedException e)
          {
          }
      }
    return result;
  }

  /*****************************************
  *
  *  updateRequest (note:  runs on subclass thread "blocking" until complete)
  *
  *****************************************/

  protected void updateRequest(DeliveryRequest deliveryRequest)
  {
    processUpdateRequest(deliveryRequest);
  }

  /*****************************************
  *
  *  processCorrelatorUpdate -- abstract w/ default implementation
  *
  *****************************************/

  protected void processCorrelatorUpdate(DeliveryRequest deliveryRequest, JSONObject correlatorUpdate) { }

  /*****************************************
  *
  *  completeRequest  (note:  runs on subclass thread "blocking" until complete)
  *
  *****************************************/
  
  protected void completeRequest(DeliveryRequest deliveryRequest)
  {
    processCompleteRequest(deliveryRequest);
  }

  /*****************************************
  *
  *  submitCorrelatorUpdate  (note:  runs on subclass thread "blocking" until complete)
  *
  *****************************************/

  protected void submitCorrelatorUpdate(String correlator, JSONObject correlatorUpdate)
  {
    processSubmitCorrelatorUpdate(correlator, correlatorUpdate);
  }

  /*****************************************
  *
  *  shutdown -- abstract with default implementation
  *
  *****************************************/

  protected void shutdown() { }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  protected DeliveryManager(String applicationID, String deliveryManagerKey, String bootstrapServers, ConnectSerde<? extends DeliveryRequest> requestSerde, DeliveryManagerDeclaration deliveryManagerDeclaration)
  {
    /*****************************************
    *
    *  runtime
    *
    *****************************************/

    NGLMRuntime.initialize();
    
    /*****************************************
    *
    *  status
    *
    *****************************************/

    this.managerStatus = ManagerStatus.Suspended;

    /*****************************************
    *
    *  configuration
    *
    *****************************************/
        
    this.applicationID = applicationID;
    this.deliveryManagerKey = deliveryManagerKey;
    this.bootstrapServers = bootstrapServers;
    this.requestSerde = (ConnectSerde<DeliveryRequest>) requestSerde;
    this.requestTopic = deliveryManagerDeclaration.getRequestTopic();
    this.responseTopic = deliveryManagerDeclaration.getResponseTopic();
    this.internalTopic = deliveryManagerDeclaration.getInternalTopic();
    this.routingTopic = deliveryManagerDeclaration.getRoutingTopic();
    this.deliveryRatePerMinute = deliveryManagerDeclaration.getDeliveryRatePerMinute();
    this.deliveryGuarantee = (deliveryManagerDeclaration.getDeliveryGuarantee() != null) ? deliveryManagerDeclaration.getDeliveryGuarantee() : DeliveryGuarantee.AtLeastOnce;

    /*****************************************
    *
    *  calculated
    *
    *****************************************/

    millisecondsPerDelivery = (int) Math.ceil(1.0 / ((double) deliveryRatePerMinute / (60.0 * 1000.0)));
    maxOutstandingRequests = Math.min((int) Math.ceil((double) deliveryRatePerMinute / 60.0), 100);

    /*****************************************
    *
    *  shutdown hook
    *
    *****************************************/
    
    NGLMRuntime.addShutdownHook(new ShutdownHook(this));
    
    /*****************************************
    *
    *  request consumer
    *
    *****************************************/

    Properties requestConsumerProperties = new Properties();
    requestConsumerProperties.put("bootstrap.servers", bootstrapServers);
    requestConsumerProperties.put("group.id", applicationID + "-request");
    requestConsumerProperties.put("auto.offset.reset", "earliest");
    requestConsumerProperties.put("enable.auto.commit", "false");
    requestConsumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    requestConsumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    requestConsumer = new KafkaConsumer<>(requestConsumerProperties);
    
    /*****************************************
    *
    *  routing consumer
    *
    *****************************************/

    Properties routingConsumerProperties = new Properties();
    routingConsumerProperties.put("bootstrap.servers", bootstrapServers);
    routingConsumerProperties.put("group.id", applicationID + "-routing-" + deliveryManagerKey);
    routingConsumerProperties.put("auto.offset.reset", "earliest");
    routingConsumerProperties.put("enable.auto.commit", "false");
    routingConsumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    routingConsumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    routingConsumer = new KafkaConsumer<>(routingConsumerProperties);

    /*****************************************
    *
    *  kafka producer
    *
    *****************************************/

    Properties kafkaProducerProperties = new Properties();
    kafkaProducerProperties.put("bootstrap.servers", bootstrapServers);
    kafkaProducerProperties.put("acks", "all");
    kafkaProducerProperties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    kafkaProducerProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    kafkaProducer = new KafkaProducer<byte[], byte[]>(kafkaProducerProperties);
    
    /*****************************************
    *
    *  submitRequestWorker
    *
    *****************************************/

    Runnable submitRequestWorker = new Runnable() { @Override public void run() { runSubmitRequestWorker(); } };
    submitRequestWorkerThread = new Thread(submitRequestWorker, applicationID + "-SubmitRequestWorker");
    submitRequestWorkerThread.start();
    
    /*****************************************
    *
    *  receiveCorrelatorUpdate worker
    *
    *****************************************/

    Runnable receiveCorrelatorUpdateWorker = new Runnable() { @Override public void run() { runReceiveCorrelatorWorker(); } };
    receiveCorrelatorUpdateWorkerThread = new Thread(receiveCorrelatorUpdateWorker, applicationID + "-ReceiveCorrelatorWorker");
    receiveCorrelatorUpdateWorkerThread.start();
  }

  /*****************************************
  *
  *  revokeRequestConsumerPartitions
  *
  *****************************************/

  private void revokeRequestConsumerPartitions(Collection<TopicPartition> partitions)
  {
    //
    //  log
    //

    log.info("requestConsumer partitions revoked: {}", partitions);

    //
    //  requestConsumerRunning
    //

    synchronized (this)
      {
        this.requestConsumerRunning = false;
        routingConsumer.wakeup();
        this.notifyAll();
      }
    
    //
    //  drainOutstandingRequests
    //

    drainOutstandingRequests(partitions);
  }

  /*****************************************
  *
  *  assignRequestConsumerPartitions
  *
  *****************************************/

  private void assignRequestConsumerPartitions(Collection<TopicPartition> partitions)
  {
    //
    //  log
    //

    log.info("requestConsumer partitions assigned: {}", partitions);

    //
    //  restart
    //

    restart(partitions);

    //
    //  requestConsumerRunning
    //

    synchronized (this)
      {
        this.requestConsumerRunning = true;
        this.notifyAll();
      }
  }

  /*****************************************
  *
  *  drainOutstandingRequests
  *
  *****************************************/

  private void drainOutstandingRequests(Collection<TopicPartition> partitions)
  {
    synchronized (this)
      {
        //
        //  clear waitingForRestart
        //

        waitingForRestart.clear();

        //
        //  clear submitRequestQueue
        //

        while (submitRequestQueue.size() > 0)
          {
            try
              {
                DeliveryRequest deliveryRequest = submitRequestQueue.take();
                waitingForAcknowledgement.remove(deliveryRequest.getDeliveryRequestID());
                this.notifyAll();
              }
            catch (InterruptedException e)
              {
                // ignore
              }
          }

        //
        //  drain waitingForAcknowledgement
        //

        Date now = SystemTime.getCurrentTime();
        Date timeout = RLMDateUtils.addSeconds(now, 30);
        while (managerStatus.isProcessingResponses() && waitingForAcknowledgement.size() > 0 && now.before(timeout))
          {
            try
              {
                this.wait(timeout.getTime() - now.getTime());
              }
            catch (InterruptedException e)
              {
                // ignore
              }
            now = SystemTime.getCurrentTime();
          }

        //
        //  clear waitingForAcknowledgement (if timeout)
        //

        waitingForAcknowledgement.clear();

        //
        //  clear waitingForCorrelatorUpdate 
        //

        log.debug("waitingForCorrelatorUpdate.clear");
        waitingForCorrelatorUpdate.clear();
      }
  }

  /*****************************************
  *
  *  restart
  *
  *****************************************/

  private void restart(Collection<TopicPartition> partitions)
  {
    /*****************************************
    *
    *  invariant
    *
    *****************************************/

    synchronized (this)
      {
        if (waitingForRestart.size() > 0) throw new RuntimeException("waitingForRestart size: " + waitingForRestart.size());
        if (waitingForAcknowledgement.size() > 0) throw new RuntimeException("waitingForAcknowledgement size: " + waitingForAcknowledgement.size());
        if (waitingForCorrelatorUpdate.size() > 0) throw new RuntimeException("waitingForCorrelatorUpdate size: " + waitingForCorrelatorUpdate.size());
        if (submitRequestQueue.size() > 0) throw new RuntimeException("submitRequestQueue size: " + submitRequestQueue.size());
      }

    /*****************************************
    *
    *  read progress to (re-) populate data structures
    *
    *****************************************/

    //
    // set up progressConsumer
    //

    Properties progressConsumerProperties = new Properties();
    progressConsumerProperties.put("bootstrap.servers", bootstrapServers);
    progressConsumerProperties.put("group.id", applicationID + "-progress");
    progressConsumerProperties.put("auto.offset.reset", "earliest");
    progressConsumerProperties.put("enable.auto.commit", "false");
    progressConsumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    progressConsumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    KafkaConsumer<byte[], byte[]> progressConsumer = new KafkaConsumer<>(progressConsumerProperties);

    //
    //  assign topic partitions
    //

    Set<TopicPartition> progressConsumerPartitions = new HashSet<TopicPartition>();
    for (TopicPartition topicPartition : partitions)
      {
        progressConsumerPartitions.add(new TopicPartition(internalTopic, topicPartition.partition()));
      }
    progressConsumer.assign(progressConsumerPartitions);

    //
    //  read
    //

    SortedMap<String,DeliveryRequest> restartRequests = new TreeMap<String,DeliveryRequest>();
    Map<TopicPartition,Long> consumedOffsets = new HashMap<TopicPartition,Long>();
    boolean consumedAllAvailable = false;
    do
      {
        //
        // poll
        //

        ConsumerRecords<byte[], byte[]> progressRecords = (progressConsumer.assignment().size() > 0) ? progressConsumer.poll(5000) : ConsumerRecords.<byte[], byte[]>empty();

        //
        //  process
        //

        for (ConsumerRecord<byte[], byte[]> progressRecord : progressRecords)
          {
            //
            //  topicPartition
            //

            TopicPartition topicPartition = new TopicPartition(progressRecord.topic(), progressRecord.partition());

            //
            //  parse
            //

            String deliveryRequestID = stringKeySerde.deserializer().deserialize(topicPartition.topic(), progressRecord.key()).getKey();
            DeliveryRequest deliveryRequest = requestSerde.optionalDeserializer().deserialize(topicPartition.topic(), progressRecord.value());
            if (deliveryRequest != null)
              restartRequests.put(deliveryRequestID, deliveryRequest);
            else
              restartRequests.remove(deliveryRequestID);

            //
            //  offsets
            //

            consumedOffsets.put(topicPartition, progressRecord.offset());
          }

        //
        //  consumed all available?
        //

        Map<TopicPartition,Long> availableOffsets = progressConsumer.endOffsets(progressConsumerPartitions);
        consumedAllAvailable = true;
        for (TopicPartition topicPartition : availableOffsets.keySet())
          {
            Long availableOffsetForPartition = availableOffsets.get(topicPartition);
            Long consumedOffsetForPartition = (consumedOffsets.get(topicPartition) != null) ? consumedOffsets.get(topicPartition) : -1L;
            if (consumedOffsetForPartition < availableOffsetForPartition-1)
              {
                consumedAllAvailable = false;
                break;
              }
          }
      }
    while (managerStatus.isRunning() && ! consumedAllAvailable);

    //
    //  close the consumer
    //

    progressConsumer.close();

    /****************************************
    *
    *  restart
    *
    ****************************************/

    synchronized (this)
      {
        for (DeliveryRequest deliveryRequest : restartRequests.values())
          {
            if (deliveryRequest.getCorrelator() != null)
              {
                log.debug("waitingForCorrelatorUpdate.put (restart): {} {}", deliveryRequest.getCorrelator(), deliveryRequest);
                waitingForCorrelatorUpdate.put(deliveryRequest.getCorrelator(), deliveryRequest);
              }
            else if (deliveryGuarantee == DeliveryGuarantee.AtLeastOnce)
              {
                waitingForRestart.add(deliveryRequest);
              }
            else
              {
                deliveryRequest.setDeliveryStatus(DeliveryStatus.Indeterminate);
                deliveryRequest.setDeliveryDate(SystemTime.getCurrentTime());
                completeRequest(deliveryRequest);
              }
          }
      }
  }
  
  /*****************************************
  *
  *  runSubmitRequestWorker
  *
  *****************************************/

  private void runSubmitRequestWorker()
  {
    while (managerStatus.isRunning())
      {
        /*****************************************
        *
        *  startDelivery/suspendDelivery
        *
        *****************************************/

        if (managerStatus.isSuspended())
          {
            /*****************************************
            *
            *  resetRequired
            *
            *****************************************/

            boolean resetRequired = managerStatus.isSuspending();

            /*****************************************
            *
            *  reset (if required)
            *
            *****************************************/

            if (resetRequired)
              {
                //
                //  log
                //

                log.info("Resetting DeliveryManager (requestWorker) " + applicationID);

                //
                //  drain  
                //

                drainOutstandingRequests(requestConsumerAssignment);

                //
                //  unsubscribe from requestTopic
                //

                requestConsumer.unsubscribe();
                requestConsumerAssignment = new HashSet<TopicPartition>();

                //
                //  suspsend
                //

                switch (managerStatus)
                  {
                    case SuspendRequested:
                      managerStatus = ManagerStatus.Suspended;
                      break;
                  }
              }

            /*****************************************
            *
            *  wait for startDelivery
            *
            *****************************************/

            synchronized (this)
              {
                //
                //  wait
                //

                while (managerStatus.isWaitingToDeliverRequests())
                  {
                    try
                      {
                        this.wait();
                      }
                    catch (InterruptedException e)
                      {
                      }
                  }

                //
                //  mark resumed
                //

                switch (managerStatus)
                  {
                    case ResumeRequested:
                      managerStatus = ManagerStatus.Delivering;
                      break;
                  }
              }

            /*****************************************
            *
            *  delivering?
            *
            *****************************************/

            if (! managerStatus.isDeliveringRequests())
              {
                continue;
              }

            /*****************************************
            *
            *  log
            *
            *****************************************/

            log.info("Starting DeliveryManager (requestWorker) " + applicationID);

            /*****************************************
            *
            *  subscribe to request topic
            *
            *****************************************/

            ConsumerRebalanceListener listener = new ConsumerRebalanceListener()
            {
              @Override public void onPartitionsRevoked(Collection<TopicPartition> partitions) {  revokeRequestConsumerPartitions(partitions); }
              @Override public void onPartitionsAssigned(Collection<TopicPartition> partitions) { assignRequestConsumerPartitions(partitions); }
            };
            requestConsumer.subscribe(Arrays.asList(requestTopic), listener);
            requestConsumerAssignment = new HashSet<TopicPartition>(requestConsumer.assignment());
          }

        /****************************************
        *
        *  poll for requests
        *
        *  NOTE: if the requestConsumer is newly subscribed (on start or repartition) then restart will be called during the requestConsumer poll
        *
        ****************************************/

        ConsumerRecords<byte[], byte[]> requestRecords;
        try
          {
            requestRecords = requestConsumer.poll(5000);
          }
        catch (WakeupException e)
          {
            requestRecords = ConsumerRecords.<byte[], byte[]>empty();
          }

        /*****************************************
        *
        *  submit restarted requests
        *
        ****************************************/

        Iterator<DeliveryRequest> restartRequests = waitingForRestart.iterator();
        while (restartRequests.hasNext())
          {
            //
            //  running?
            //

            if (! managerStatus.isDeliveringRequests()) break;

            //
            //  get next
            //

            DeliveryRequest deliveryRequest = restartRequests.next();

            //
            //  deliver
            //
            
            submitDeliveryRequest(deliveryRequest, true, null);

            //
            //  remove
            //

            restartRequests.remove();
          }

        /****************************************
        *
        *  submit requests
        *
        ****************************************/

        for (ConsumerRecord<byte[], byte[]> requestRecord : requestRecords)
          {
            //
            //  running?
            //

            if (! managerStatus.isDeliveringRequests()) break;
            
            //
            //  topicPartition
            //

            TopicPartition topicPartition = new TopicPartition(requestRecord.topic(), requestRecord.partition());

            //
            //  parse
            //

            DeliveryRequest deliveryRequest = requestSerde.deserializer().deserialize(topicPartition.topic(), requestRecord.value());

            //
            //  partition
            //

            deliveryRequest.setDeliveryPartition(requestRecord.partition());

            //
            //  process
            //

            submitDeliveryRequest(deliveryRequest, false, requestRecord.offset());
          }
      }
  }

  /*****************************************
  *
  *  submitDeliveryRequest
  *
  *****************************************/

  private void submitDeliveryRequest(DeliveryRequest deliveryRequest, boolean restart, Long deliveryRequestOffset)
  {
    /****************************************
    *
    *  in progress
    *
    ****************************************/
    
    synchronized(this)
      {
        //
        //  abort if already in progress
        //

        if (waitingForAcknowledgement.containsKey(deliveryRequest.getDeliveryRequestID()))
          {
            return;
          }
        
        //
        //  maxOutstandingRequests
        //
        
        while (managerStatus.isDeliveringRequests() && waitingForAcknowledgement.size() >= maxOutstandingRequests)
          {
            try
              {
                this.wait();
              }
            catch (InterruptedException e)
              {
              }
          }

        //
        //  abort if no longer delivering
        //
        
        if (! managerStatus.isDeliveringRequests())
          {
            return;
          }
      }
    
    /****************************************
    *
    *  throttle
    *
    ****************************************/

    //
    //  wait
    //
        
    if (! deliveryRequest.getControl())
      {
        Date now = SystemTime.getCurrentTime();
        Date nextSubmitDate = RLMDateUtils.addMilliseconds(lastSubmitDate, millisecondsPerDelivery);
        while (managerStatus.isDeliveringRequests() && now.before(nextSubmitDate))
          {
            synchronized (this)
              {
                try
                  {
                    this.wait(nextSubmitDate.getTime() - now.getTime());
                  }
                catch (InterruptedException e)
                  {
                  }
              }
            now = SystemTime.getCurrentTime();
          }
        lastSubmitDate = now;
      }

    //
    //  abort if no longer delivering
    //
        
    if (! managerStatus.isDeliveringRequests())
      {
        return;
      }

    /*****************************************
    *
    *  log (debug)
    *
    *****************************************/

    log.debug("submitDeliveryRequest: {}", deliveryRequest);

    /****************************************
    *
    *  write to internal topic
    *
    ****************************************/

    if (! restart)
      {
        while (managerStatus.isRunning())
          {
            try
              {
                kafkaProducer.send(new ProducerRecord<byte[], byte[]>(internalTopic, stringKeySerde.serializer().serialize(internalTopic, new StringKey(deliveryRequest.getDeliveryRequestID())), requestSerde.optionalSerializer().serialize(internalTopic, deliveryRequest))).get();
                break;
              }
            catch (InterruptedException e)
              {
                // ignore and resend
              }
            catch (ExecutionException e)
              {
                throw new RuntimeException(e);
              }
          }
      }
    
    /*****************************************
    *
    *  waitingForAcknowledgement
    *
    *****************************************/
    
    synchronized (this)
      {
        waitingForAcknowledgement.put(deliveryRequest.getDeliveryRequestID(), deliveryRequest);
      }

    /****************************************
    *
    *  commit request topic offsets
    *
    ****************************************/

    if (! restart)
      {
        requestConsumer.commitSync(Collections.<TopicPartition,OffsetAndMetadata>singletonMap(new TopicPartition(requestTopic, deliveryRequest.getDeliveryPartition()), new OffsetAndMetadata(deliveryRequestOffset + 1)));
      }
    
    /****************************************
    *
    *  submit -- normal
    *
    ****************************************/

    if (! deliveryRequest.getControl())
      {
        submitRequestQueue.add(deliveryRequest);
      }

    /*****************************************
    *
    *  simulate response -- control
    *
    *****************************************/

    if (deliveryRequest.getControl())
      {
        deliveryRequest.setDeliveryStatus(DeliveryStatus.Control);
        deliveryRequest.setDeliveryDate(SystemTime.getCurrentTime());
        completeRequest(deliveryRequest);
      }
  }
  
  /*****************************************
  *
  *  processUpdateRequest
  *
  *****************************************/

  private void processUpdateRequest(DeliveryRequest deliveryRequest)
  {
    if (managerStatus.isProcessingResponses())
      {
        /*****************************************
        *
        *  log (debug)
        *
        *****************************************/

        log.debug("processUpdateRequest: {}", deliveryRequest);

        /*****************************************
        *
        *  commit
        *
        *****************************************/

        while (managerStatus.isProcessingResponses())
          {
            try
              {
                kafkaProducer.send(new ProducerRecord<byte[], byte[]>(internalTopic, stringKeySerde.serializer().serialize(internalTopic, new StringKey(deliveryRequest.getDeliveryRequestID())), requestSerde.optionalSerializer().serialize(internalTopic, deliveryRequest))).get();
                break;
              }
            catch (InterruptedException e)
              {
                // ignore and resend
              }
            catch (ExecutionException e)
              {
                throw new RuntimeException(e);
              }
          }

        /*****************************************
        *
        *  move to next state
        *
        *****************************************/

        synchronized (this)
          {
            //
            //  no longer waiting on subclass
            //

            waitingForAcknowledgement.remove(deliveryRequest.getDeliveryRequestID());
            this.notifyAll();

            //
            //  register correlator
            //

            if (deliveryRequest.getCorrelator() != null)
              {
                log.debug("waitingForCorrelatorUpdate.put (processUpdateRequest): {} {}", deliveryRequest.getCorrelator(), deliveryRequest);
                waitingForCorrelatorUpdate.put(deliveryRequest.getCorrelator(), deliveryRequest);
              }
          }
      }
  }

  /*****************************************
  *
  *  processCompleteRequest
  *
  *****************************************/

  private void processCompleteRequest(DeliveryRequest deliveryRequest)
  {
    if (managerStatus.isProcessingResponses())
      {
        /*****************************************
        *
        *  log (debug)
        *
        *****************************************/

        log.debug("processCompleteRequest: {}", deliveryRequest);

        /*****************************************
        *
        *  state
        *
        *****************************************/

        synchronized (this)
          {
            //
            //  remove from waitingForAcknowledgement (if necessary)
            //

            waitingForAcknowledgement.remove(deliveryRequest.getDeliveryRequestID());
            this.notifyAll();

            //
            //  deregister correlator
            //

            if (deliveryRequest.getCorrelator() != null)
              {
                log.debug("waitingForCorrelatorUpdate.remove: {} {}", deliveryRequest.getCorrelator(), deliveryRequest);
                waitingForCorrelatorUpdate.remove(deliveryRequest.getCorrelator());
              }
          }

        /*****************************************
        *
        *  ensure invariants
        *
        *****************************************/

        if (deliveryRequest.getDeliveryStatus() == null) deliveryRequest.setDeliveryStatus(DeliveryStatus.Indeterminate);
        if (deliveryRequest.getDeliveryDate() == null) deliveryRequest.setDeliveryDate(SystemTime.getCurrentTime());

        /****************************************
        *
        *  write response message
        *
        ****************************************/

        while (managerStatus.isProcessingResponses())
          {
            try
              {
                kafkaProducer.send(new ProducerRecord<byte[], byte[]>(responseTopic, stringKeySerde.serializer().serialize(responseTopic, new StringKey(deliveryRequest.getSubscriberID())), requestSerde.serializer().serialize(responseTopic, deliveryRequest))).get();
                break;
              }
            catch (InterruptedException e)
              {
                // ignore and resend
              }
            catch (ExecutionException e)
              {
                throw new RuntimeException(e);
              }
          }
        
        /****************************************
        *
        *  update progress
        *
        ****************************************/

        while (managerStatus.isProcessingResponses())
          {
            try
              {
                kafkaProducer.send(new ProducerRecord<byte[], byte[]>(internalTopic, stringKeySerde.serializer().serialize(internalTopic, new StringKey(deliveryRequest.getDeliveryRequestID())), requestSerde.optionalSerializer().serialize(internalTopic, null))).get();
                break;
              }
            catch (InterruptedException e)
              {
                // ignore and resend
              }
            catch (ExecutionException e)
              {
                throw new RuntimeException(e);
              }
          }
      }
  }

  /*****************************************
  *
  *  processSubmitCorrelatorUpdate  (note:  runs on subclass thread "blocking" until complete)
  *
  *****************************************/

  private void processSubmitCorrelatorUpdate(String correlator, JSONObject correlatorUpdate)
  {
    while (managerStatus.isRunning())
      {
        try
          {
            kafkaProducer.send(new ProducerRecord<byte[], byte[]>(routingTopic, stringKeySerde.serializer().serialize(routingTopic, new StringKey(correlator)), stringValueSerde.serializer().serialize(routingTopic, new StringValue(correlatorUpdate.toString())))).get();
            break;
          }
        catch (InterruptedException e)
          {
            // ignore and resend
          }
        catch (ExecutionException e)
          {
            throw new RuntimeException(e);
          }
      }
  }

  /*****************************************
  *
  *  runReceiveCorrelatorWorker
  *
  *****************************************/

  private void runReceiveCorrelatorWorker()
  {
    routingConsumer.subscribe(Arrays.asList(routingTopic));
    while (managerStatus.isProcessingResponses())
      {
        /*****************************************
        *
        *  wait for requestConsumer rebalance/restart
        *
        *****************************************/

        synchronized (this)
          {
            //
            //  wait
            //

            while (managerStatus.isProcessingResponses() && ! requestConsumerRunning)
              {
                try
                  {
                    this.wait();
                  }
                catch (InterruptedException e)
                  {
                  }
              }

            //
            //  abort if no longer processing responses
            //

            if (! managerStatus.isProcessingResponses())
              {
                continue;
              }
          }

        /****************************************
        *
        *  poll for correlatorUpdates
        *
        ****************************************/

        ConsumerRecords<byte[], byte[]> correlatorUpdateRecords;
        try
          {
            correlatorUpdateRecords = routingConsumer.poll(5000);
          }
        catch (WakeupException e)
          {
            correlatorUpdateRecords = ConsumerRecords.<byte[], byte[]>empty();
          }

        /*****************************************
        *
        *  process
        *
        *****************************************/

        for (ConsumerRecord<byte[], byte[]> correlatorUpdateRecord : correlatorUpdateRecords)
          {
            //
            //  running
            //

            if (! managerStatus.isDeliveringRequests()) break;

            //
            //  deserialize
            //

            String correlator = stringKeySerde.deserializer().deserialize(routingTopic, correlatorUpdateRecord.key()).getKey();
            StringValue correlatorUpdateStringValue = stringValueSerde.optionalDeserializer().deserialize(routingTopic, correlatorUpdateRecord.value());
            String correlatorUpdateJSON = (correlatorUpdateStringValue != null) ? correlatorUpdateStringValue.getValue() : null;

            //
            //  parse
            //

            JSONObject correlatorUpdate = null;
            try
              {
                if (correlatorUpdateJSON != null)
                  {
                    correlatorUpdate = (JSONObject) (new JSONParser()).parse(correlatorUpdateJSON);
                  }
              }
            catch (org.json.simple.parser.ParseException e)
              {
                StringWriter stackTraceWriter = new StringWriter();
                e.printStackTrace(new PrintWriter(stackTraceWriter, true));
                log.error("Exception processing DeliveryManager correlatorUpdate: {}", stackTraceWriter.toString());
                correlatorUpdate = null;
              }

            //
            //  deliveryRequest
            //

            DeliveryRequest deliveryRequest = null;
            if (correlatorUpdate != null)
              {
                synchronized (this)
                  {
                    deliveryRequest = waitingForCorrelatorUpdate.get(correlator);
                  }
              }

            //
            //  log (debug)
            //

            log.debug("receiveCorrelatorUpdate: {} {} {}", correlator, correlatorUpdate, deliveryRequest);

            //
            //  update (if necessary)
            //

            if (deliveryRequest != null)
              {
                processCorrelatorUpdate(deliveryRequest, correlatorUpdate);
              }

            //
            //  delete the correlatorUpdate entry (if used)
            //

            if (deliveryRequest != null)
              {
                while (managerStatus.isProcessingResponses())
                  {
                    try
                      {
                        kafkaProducer.send(new ProducerRecord<byte[], byte[]>(routingTopic, stringKeySerde.serializer().serialize(routingTopic, new StringKey(correlator)), stringValueSerde.optionalSerializer().serialize(routingTopic, null))).get();
                        break;
                      }
                    catch (InterruptedException e)
                      {
                        // ignore and resend
                      }
                    catch (ExecutionException e)
                      {
                        throw new RuntimeException(e);
                      }
                  }
              }
          }
      }
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

    private DeliveryManager deliveryManager;

    //
    //  constructor
    //

    private ShutdownHook(DeliveryManager deliveryManager)
    {
      this.deliveryManager = deliveryManager;
    }

    //
    //  shutdown
    //

    @Override public void shutdown(boolean normalShutdown)
    {
      deliveryManager.shutdownDeliveryManager(normalShutdown);
    }
  }

  /****************************************
  *
  *  shutdownDeliveryManager
  *
  ****************************************/
  
  private void shutdownDeliveryManager(boolean normalShutdown)
  {
    /*****************************************
    *
    *  stop threads
    *
    *****************************************/

    synchronized (this)
      {
        managerStatus = normalShutdown ? ManagerStatus.Stopping : ManagerStatus.Aborting;
        requestConsumer.wakeup();
        routingConsumer.wakeup();
        this.notifyAll();
      }

    /*****************************************
    *
    *  drainOutstandingRequests
    *
    *****************************************/

    switch (managerStatus)
      {
        case Stopping:
          drainOutstandingRequests(requestConsumerAssignment);
          break;
      }

    /*****************************************
    *
    *  stopped
    *
    *****************************************/

    synchronized (this)
      {
        managerStatus = ManagerStatus.Stopped;
        this.notifyAll();
      }

    /*****************************************
    *
    *  shutdown subclass
    *
    *****************************************/

    shutdown();

    /*****************************************
    *
    *  kafka
    *
    *****************************************/
    
    //
    //  requestConsumer
    //

    if (requestConsumer != null)
      {
        requestConsumer.wakeup();
        while (true)
          {
            try
              {
                submitRequestWorkerThread.join();
                break;
              }
            catch (InterruptedException e)
              {
              }
          }
        requestConsumer.close();
      }
    
    //
    //  routingConsumer
    //

    if (routingConsumer != null)
      {
        routingConsumer.wakeup();
        while (true)
          {
            try
              {
                receiveCorrelatorUpdateWorkerThread.join();
                break;
              }
            catch (InterruptedException e)
              {
              }
          }
        routingConsumer.close();
      }

    //
    //  kafkaProducer
    //

    if (kafkaProducer != null)
      {
        while (true)
          {
            try
              {
                submitRequestWorkerThread.join();
                receiveCorrelatorUpdateWorkerThread.join();
                break;
              }
            catch (InterruptedException e)
              {
              }
          }
        kafkaProducer.close();
      }

    /*****************************************
    *
    *  log
    *
    *****************************************/

    log.info("Stopped DeliveryManager " + applicationID);
  }
}
