/****************************************************************************
*
*  DeliveryManager.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
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

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.StringKey;
import com.evolving.nglm.core.StringValue;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.core.WorkItemScheduler;
import com.evolving.nglm.evolution.DeliveryRequest.ActivityType;

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
    /*****************************************
    *
    *  non-final states
    *
    *****************************************/

    Pending("pending"),
    FailedRetry("failedretry"),

    /*****************************************
    *
    *  final states
    *
    *****************************************/

    Delivered("delivered"),
    Indeterminate("indeterminate"),
    Failed("failed"),
    FailedTimeout("failedtimeout"),
    Acknowledged("acknowledged"),
    Reschedule("reschedule"),
    BlockedByContactPolicy("blockedbycontactpolicy"),

    /*****************************************
    *
    *  structure
    *
    *****************************************/

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
  private List<String> requestTopics;
  private String responseTopic;
  private String internalTopic;
  private String routingTopic;
  private int deliveryRatePerMinute;
  private DeliveryGuarantee deliveryGuarantee;
  private int retries;
  private int acknowledgementTimeoutSeconds;
  private int correlatorUpdateTimeoutSeconds;

  //
  //  derived configuration
  //

  private int millisecondsPerDelivery;
  private int maxOutstandingRequests;

  //
  //  status
  //

  private volatile ManagerStatus managerStatus = ManagerStatus.Created;
  private volatile boolean deliveryManagerRunning = false;
  private volatile boolean receiveCorrelatorUpdateWorkerRunning = false;
  private volatile boolean timeoutWorkerRunning = false;
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
  private List<DeliveryRequest> waitingForRetry = new LinkedList<DeliveryRequest>();
  private Map<String,DeliveryRequest> waitingForAcknowledgement = new HashMap<String,DeliveryRequest>();
  private Map<String,DeliveryRequest> waitingForCorrelatorUpdate = new HashMap<String,DeliveryRequest>();
  private LinkedBlockingQueue<DeliveryRequest> scheduledRequests = new LinkedBlockingQueue<DeliveryRequest>();
  private WorkItemScheduler<DeliveryRequest> scheduler = new WorkItemScheduler<DeliveryRequest>(scheduledRequests,"DeliveryManager-Scheduler");

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
  private Thread timeoutWorkerThread = null;

  //
  //  internal processing
  //
  private ContactPolicyProcessor contactPolicyProcessor;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getDeliveryManagerKey() { return deliveryManagerKey; }
  public List<String> getRequestTopics() { return requestTopics; }
  public String getResponseTopic() { return responseTopic; }
  public String getInternalTopic() { return internalTopic; }
  public String getRoutingTopic() { return routingTopic; }
  public int getDeliveryRatePerMinute() { return deliveryRatePerMinute; }
  public DeliveryGuarantee getDeliveryGuarantee() { return deliveryGuarantee; }
  public int getRetries() { return retries; }
  public int getAcknowledgementTimeoutSeconds() { return acknowledgementTimeoutSeconds; }
  public int getCorrelatorUpdateTimeoutSeconds() { return correlatorUpdateTimeoutSeconds; }

  //
  //  derived
  //

  private String getDefaultRequestTopic() { return requestTopics.get(0); }

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
            processContactPolicyForRequest(result);
            //if DeliveryStatus is BlockedByContactPolicy make result null and the next request from queue will be taken
            if(result.getDeliveryStatus() == DeliveryStatus.BlockedByContactPolicy) result = null;
          }
        catch (InterruptedException e)
          {
          }
      }
    return (result != null) ? result.copy() : null;
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

    NGLMRuntime.initialize(true);

    /*****************************************
    *
    *  validate -- TEMPORARY, DeliveryManagers dervied from DeliveryManager.java doe NOT support priority
    *
    *****************************************/

    if (deliveryManagerDeclaration == null) throw new RuntimeException("invalid delivery manager (no such delivery manager)");
    if (deliveryManagerDeclaration.getRequestTopics().size() == 0) throw new RuntimeException("invalid delivery manager (no request topic)");

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
    this.requestTopics = deliveryManagerDeclaration.getRequestTopics();
    this.responseTopic = deliveryManagerDeclaration.getResponseTopic();
    this.internalTopic = deliveryManagerDeclaration.getInternalTopic();
    this.routingTopic = deliveryManagerDeclaration.getRoutingTopic();
    this.deliveryRatePerMinute = deliveryManagerDeclaration.getDeliveryRatePerMinute();
    this.deliveryGuarantee = (deliveryManagerDeclaration.getDeliveryGuarantee() != null) ? deliveryManagerDeclaration.getDeliveryGuarantee() : DeliveryGuarantee.AtLeastOnce;
    this.retries = deliveryManagerDeclaration.getRetries();;
    this.acknowledgementTimeoutSeconds = deliveryManagerDeclaration.getAcknowledgementTimeoutSeconds();;
    this.correlatorUpdateTimeoutSeconds = deliveryManagerDeclaration.getCorrelatorUpdateTimeoutSeconds();;

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

    /*****************************************
    *
    *  timeout worker
    *
    *****************************************/

    Runnable timeoutWorker = new Runnable() { @Override public void run() { runTimeoutWorker(); } };
    timeoutWorkerThread = new Thread(timeoutWorker, applicationID + "-TimeoutWorker");
    timeoutWorkerThread.start();

    /*****************************************
     *
     *  contact policy processor
     *
     *****************************************/

    contactPolicyProcessor = new ContactPolicyProcessor("deliveryManager-communicationchannel", deliveryManagerKey);
  }

  /*****************************************
  *
  *  revokeRequestConsumerPartitions
  *
  *****************************************/

  private void revokeRequestConsumerPartitions(Collection<TopicPartition> partitions)
  {
    synchronized (this)
      {
        //
        //  log
        //

        log.info("requestConsumer partitions revoked: {}", partitions);

        //
        //  clear submitRequestQueue
        //

        synchronized (submitRequestQueue)
          {
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
          }

        //
        //  drain waitingForAcknowledgement
        //

        while (waitingForAcknowledgement.size() > 0)
          {
            try
              {
                this.wait();
              }
            catch (InterruptedException e)
              {
                // ignore
              }
          }

        //
        //  deliveryManagerRunning
        //

        this.deliveryManagerRunning = false;
        routingConsumer.wakeup();
        timeoutWorkerThread.interrupt();
        this.notifyAll();

        //
        //  wait for other threads to quiesce
        //

        while (this.receiveCorrelatorUpdateWorkerRunning || this.timeoutWorkerRunning)
          {
            try
              {
                this.wait();
              }
            catch (InterruptedException e)
              {
                // ignore
              }
          }

        //
        //  clear waitingForRestart
        //

        waitingForRestart.clear();

        //
        //  clear waitingForRetry
        //

        waitingForRetry.clear();

        // 
        //  clear scheduler and associated blocking queue
        //

        scheduler.clear();
        scheduledRequests.clear();

        //
        //  clear waitingForCorrelatorUpdate 
        //

        waitingForCorrelatorUpdate.clear();
      }
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

    /*****************************************
    *
    *  invariant
    *
    *****************************************/

    synchronized (this)
      {
        if (deliveryManagerRunning) throw new RuntimeException("deliveryManagerRunning");
        if (receiveCorrelatorUpdateWorkerRunning) throw new RuntimeException("receiveCorrelatorUpdateWorkerRunning");
        if (timeoutWorkerRunning) throw new RuntimeException("timeoutWorkerRunning");
        if (waitingForRestart.size() > 0) throw new RuntimeException("waitingForRestart size: " + waitingForRestart.size());
        if (waitingForRetry.size() > 0) throw new RuntimeException("waitingForRetry size: " + waitingForRetry.size());
        if (waitingForAcknowledgement.size() > 0) throw new RuntimeException("waitingForAcknowledgement size: " + waitingForAcknowledgement.size());
        if (waitingForCorrelatorUpdate.size() > 0) throw new RuntimeException("waitingForCorrelatorUpdate size: " + waitingForCorrelatorUpdate.size());
        if (submitRequestQueue.size() > 0) throw new RuntimeException("submitRequestQueue size: " + submitRequestQueue.size());
        if (scheduler.size() > 0) throw new RuntimeException("scheduler size: " + scheduler.size());
        if (scheduledRequests.size() > 0) throw new RuntimeException("scheduledRequests size: " + scheduledRequests.size());
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
    //  initialize consumedOffsets
    //

    boolean consumedAllAvailable = false;
    Map<TopicPartition,Long> consumedOffsets = new HashMap<TopicPartition,Long>();
    for (TopicPartition topicPartition : progressConsumer.assignment())
      {
        consumedOffsets.put(topicPartition, progressConsumer.position(topicPartition) - 1L);
      }

    //
    //  read
    //

    SortedMap<String,DeliveryRequest> restartRequests = new TreeMap<String,DeliveryRequest>();
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
            Long consumedOffsetForPartition = consumedOffsets.get(topicPartition);
            if (consumedOffsetForPartition == null)
              {
                consumedOffsetForPartition = progressConsumer.position(topicPartition) - 1L;
                consumedOffsets.put(topicPartition, consumedOffsetForPartition);
              }
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
                //
                //  register correlator
                //

                if(log.isDebugEnabled()) log.debug("waitingForCorrelatorUpdate.put (restart): {} {}", deliveryRequest.getCorrelator(), deliveryRequest);
                waitingForCorrelatorUpdate.put(deliveryRequest.getCorrelator(), deliveryRequest);

                //
                //  schedule timeout
                //

                Date timeout = RLMDateUtils.addSeconds(SystemTime.getCurrentTime(), correlatorUpdateTimeoutSeconds);
                deliveryRequest.setTimeout(timeout);
                scheduler.schedule(timeout, deliveryRequest);
              }
            else if (deliveryGuarantee == DeliveryGuarantee.AtLeastOnce)
              {
                waitingForRestart.add(deliveryRequest);
              }
            else
              {
                waitingForAcknowledgement.put(deliveryRequest.getDeliveryRequestID(), deliveryRequest);
                deliveryRequest.setDeliveryStatus(DeliveryStatus.Indeterminate);
                deliveryRequest.setDeliveryDate(SystemTime.getCurrentTime());
                completeRequest(deliveryRequest);
              }
          }
      }

    /*****************************************
    *
    *  deliveryManagerRunning
    *
    *****************************************/

    //
    //  deliveryManagerRunning
    //

    synchronized (this)
      {
        this.deliveryManagerRunning = true;
        this.receiveCorrelatorUpdateWorkerRunning = true;
        this.timeoutWorkerRunning = true;
        this.notifyAll();
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

                revokeRequestConsumerPartitions(requestConsumerAssignment);

                //
                //  unsubscribe from requestTopics
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
            *  subscribe to request topic (note: assignRequestConsumerPartitions will be called inside the next call to poll)
            *
            *****************************************/

            ConsumerRebalanceListener listener = new ConsumerRebalanceListener()
            {
              @Override public void onPartitionsRevoked(Collection<TopicPartition> partitions) {  revokeRequestConsumerPartitions(partitions); }
              @Override public void onPartitionsAssigned(Collection<TopicPartition> partitions) { assignRequestConsumerPartitions(partitions); }
            };
            requestConsumer.subscribe(requestTopics, listener);
            requestConsumerAssignment = new HashSet<TopicPartition>(requestConsumer.assignment());
          }

        /****************************************
        *
        *  poll for requests
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

        //
        //  copy and clear waitingForRestart
        //

        List<DeliveryRequest> restartRequests = new ArrayList<DeliveryRequest>();
        synchronized (this)
          {
            restartRequests.addAll(waitingForRestart);
            waitingForRestart.clear();
          }

        //
        //  submit
        //

        for (DeliveryRequest deliveryRequest : restartRequests)
          {
            //
            //  running?
            //

            if (! managerStatus.isDeliveringRequests()) break;

            //
            //  deliver
            //

            submitDeliveryRequest(deliveryRequest, true, false);
          }

        /*****************************************
        *
        *  submit retry requests
        *
        ****************************************/

        //
        //  copy and clear waitingForRetry
        //

        List<DeliveryRequest> retryRequests = new ArrayList<DeliveryRequest>();
        synchronized (this)
          {
            retryRequests.addAll(waitingForRetry);
            waitingForRetry.clear();
          }

        //
        //  submit
        //

        for (DeliveryRequest deliveryRequest : retryRequests)
          {
            //
            //  running?
            //

            if (! managerStatus.isDeliveringRequests()) break;

            //
            //  deliver
            //

            submitDeliveryRequest(deliveryRequest, false, true);
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

            submitDeliveryRequest(deliveryRequest, false, false, requestRecord.topic(), requestRecord.offset());
          }
      }
  }

  /*****************************************
  *
  *  submitDeliveryRequest
  *
  *****************************************/

  private void submitDeliveryRequest(DeliveryRequest deliveryRequest, boolean restart, boolean retry, String deliveryRequestTopic, Long deliveryRequestOffset)
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

    NGLMRuntime.registerSystemTimeDependency(this);
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

    if(log.isDebugEnabled()) log.debug("submitDeliveryRequest: {}", deliveryRequest);

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

    /****************************************
    *
    *  commit request topic offsets
    *
    ****************************************/

    if (! restart && ! retry)
      {
        while (true)
          {
            try
              {
                requestConsumer.commitSync(Collections.<TopicPartition,OffsetAndMetadata>singletonMap(new TopicPartition(deliveryRequestTopic, deliveryRequest.getDeliveryPartition()), new OffsetAndMetadata(deliveryRequestOffset + 1)));
                break;
              }
            catch (WakeupException e)
              {
              }
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
        //  waitingForAcknowledgement
        //

        waitingForAcknowledgement.put(deliveryRequest.getDeliveryRequestID(), deliveryRequest);

        //
        //  schedule timeout
        //

        Date timeout = RLMDateUtils.addSeconds(SystemTime.getCurrentTime(), acknowledgementTimeoutSeconds);
        deliveryRequest.setTimeout(timeout);
        scheduler.schedule(timeout, deliveryRequest);
      }

    /****************************************
    *
    *  submit -- normal
    *
    ****************************************/

    submitRequestQueue.add(deliveryRequest);
  }

  /*****************************************
  *
  *  submitDeliveryRequestTopic
  *
  *****************************************/

  private void submitDeliveryRequest(DeliveryRequest deliveryRequest, boolean restart, boolean retry)
  {
    submitDeliveryRequest(deliveryRequest, restart, retry, null, null);
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

        if(log.isDebugEnabled()) log.debug("processUpdateRequest: {}", deliveryRequest);

        /*****************************************
        *
        *  validate -- correlator
        *
        *****************************************/

        if (deliveryRequest.getCorrelator() == null)
          {
            throw new RuntimeException("updateRequest with no correlator");
          }

        /*****************************************
        *
        *  copy
        *
        *****************************************/

        deliveryRequest = deliveryRequest.copy();

        /*****************************************
        *
        *  request outstanding?
        *
        *****************************************/

        synchronized (this)
          {
            /*****************************************
            *
            *  in waitingForAcknowledgement?
            *
            *****************************************/

            if (! waitingForAcknowledgement.containsKey(deliveryRequest.getDeliveryRequestID()))
              {
                log.info("processUpdateRequest stale: {}", deliveryRequest);
                return;
              }

            /*****************************************
            *
            *  clear timeout
            *
            *****************************************/

            DeliveryRequest deliveryRequestOnScheduler = waitingForAcknowledgement.get(deliveryRequest.getDeliveryRequestID());
            if(deliveryRequestOnScheduler != null)
              {
                deliveryRequestOnScheduler.setTimeout(null);
              }
            deliveryRequest.setTimeout(null);
          }

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

            if(log.isDebugEnabled()) log.debug("waitingForCorrelatorUpdate.put (processUpdateRequest): {} {}", deliveryRequest.getCorrelator(), deliveryRequest);
            waitingForCorrelatorUpdate.put(deliveryRequest.getCorrelator(), deliveryRequest);

            //
            //  schedule timeout
            //

            Date timeout = RLMDateUtils.addSeconds(SystemTime.getCurrentTime(), correlatorUpdateTimeoutSeconds);
            deliveryRequest.setTimeout(timeout);
            scheduler.schedule(timeout, deliveryRequest);
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

        if(log.isDebugEnabled()) log.debug("processCompleteRequest: {}", deliveryRequest);

        /*****************************************
        *
        *  copy
        *
        *****************************************/

        deliveryRequest = deliveryRequest.copy();

        /*****************************************
        *
        *  state
        *
        *****************************************/

        synchronized (this)
          {
            //
            //  waiting?
            //

            DeliveryRequest deliveryRequestOnScheduler = null;
            boolean waiting = false;
            boolean rescheduled = (deliveryRequest.getDeliveryStatus().equals(DeliveryStatus.Reschedule));
            if (waitingForAcknowledgement.containsKey(deliveryRequest.getDeliveryRequestID()))
              {
                deliveryRequestOnScheduler = waitingForAcknowledgement.get(deliveryRequest.getDeliveryRequestID());
                waiting = true;
              }
            else if (deliveryRequest.getCorrelator() != null && waitingForCorrelatorUpdate.containsKey(deliveryRequest.getCorrelator()))
              {
                deliveryRequestOnScheduler = waitingForCorrelatorUpdate.get(deliveryRequest.getCorrelator());
                waiting = true;
              }

            //
            //  stale?
            //

            if (! waiting && ! rescheduled)
              {
                log.info("processCompleteRequest stale: {}", deliveryRequest);
                return;
              }

            //
            //  clear timeout
            //

            if(deliveryRequestOnScheduler != null) {
              deliveryRequestOnScheduler.setTimeout(null);
            }
            deliveryRequest.setTimeout(null);

            //
            //  remove from waiting (if necessary)
            //

            waitingForAcknowledgement.remove(deliveryRequest.getDeliveryRequestID());
            if (deliveryRequest.getCorrelator() != null) waitingForCorrelatorUpdate.remove(deliveryRequest.getCorrelator());
            this.notifyAll();
          }

        /*****************************************
        *
        *  ensure invariants
        *
        *****************************************/

        if (deliveryRequest.getDeliveryStatus() == null) deliveryRequest.setDeliveryStatus(DeliveryStatus.Indeterminate);
        if (deliveryRequest.getDeliveryDate() == null && ! deliveryRequest.getDeliveryStatus().equals(DeliveryStatus.Reschedule)) deliveryRequest.setDeliveryDate(SystemTime.getCurrentTime());

        /*****************************************
        *
        *  retry required?
        *
        *****************************************/

        boolean retryRequired = false;
        switch (deliveryRequest.getDeliveryStatus())
          {
            case FailedRetry:
              if (deliveryRequest.getRetries() < retries)
                {
                  retryRequired = true;
                }
              else
                {
                  deliveryRequest.setDeliveryStatus(DeliveryStatus.Failed);
                }
              break;
            case FailedTimeout:
              if (deliveryRequest.getRetries() < retries)
                {
                  retryRequired = true;
                }
              break;
          }

        /*****************************************
        *
        *  retry (if necessary)
        *
        *****************************************/

        if (retryRequired)
          {
            /*****************************************
            *
            *  setup for retry
            *
            *****************************************/

            deliveryRequest.setRetries(deliveryRequest.getRetries() + 1);
            deliveryRequest.setCorrelator(null);

            /*****************************************
            *
            *  update progress
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
            *  retry
            *
            *****************************************/

            synchronized (this)
              {
                waitingForRetry.add(deliveryRequest);
                requestConsumer.wakeup();
              }
          }

        /*****************************************
        *
        *  final response
        *
        *****************************************/

        if (! retryRequired)
          {
            /****************************************
            *
            *  write response message
            *
            ****************************************/

            while (managerStatus.isProcessingResponses())
              {
                try
                  {
                    StringKey key = deliveryRequest.getOriginatingRequest() ? new StringKey(deliveryRequest.getSubscriberID()) : new StringKey(deliveryRequest.getDeliveryRequestID());
                    kafkaProducer.send(new ProducerRecord<byte[], byte[]>(responseTopic, stringKeySerde.serializer().serialize(responseTopic, key), requestSerde.serializer().serialize(responseTopic, deliveryRequest))).get();
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
            if(log.isDebugEnabled()) log.debug("DeliveryManager processSubmitCorrelatorUpdate (" + correlator + ", "+correlatorUpdate+") ...");
            kafkaProducer.send(new ProducerRecord<byte[], byte[]>(routingTopic, stringKeySerde.serializer().serialize(routingTopic, new StringKey(correlator)), stringValueSerde.serializer().serialize(routingTopic, new StringValue(correlatorUpdate.toString())))).get();
            if(log.isDebugEnabled()) log.debug("DeliveryManager processSubmitCorrelatorUpdate (" + correlator + ", "+correlatorUpdate+") done");
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
            /*****************************************
            *
            *  suspend if deliveryManager is not running
            *
            *****************************************/

            if (! deliveryManagerRunning)
              {
                //
                //  mark as not running
                //

                receiveCorrelatorUpdateWorkerRunning = false;
                this.notifyAll();

                //
                //  wait
                //

                while (managerStatus.isProcessingResponses() && ! receiveCorrelatorUpdateWorkerRunning)
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

                //
                //  seek back to beginning of routing topic  
                //

                routingConsumer.seekToBeginning(routingConsumer.assignment());
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

            if(log.isDebugEnabled()) log.debug("receiveCorrelatorUpdate: {} {} {}", correlator, correlatorUpdate, deliveryRequest);

            //
            //  update (if necessary)
            //

            if (deliveryRequest != null)
              {
                processCorrelatorUpdate(deliveryRequest, correlatorUpdate);
              }
          }
      }
  }

  /*****************************************
  *
  *  runTimeoutWorker
  *
  *****************************************/

  private void runTimeoutWorker()
  {
    while (managerStatus.isProcessingResponses())
      {
        /*****************************************
        *
        *  wait for requestConsumer rebalance/restart
        *
        *****************************************/

        synchronized (this)
          {
            /*****************************************
            *
            *  suspend if deliveryManager is not running
            *
            *****************************************/

            if (! deliveryManagerRunning)
              {
                //
                //  mark as not running
                //

                timeoutWorkerRunning = false;
                this.notifyAll();

                //
                //  wait
                //

                while (managerStatus.isProcessingResponses() && ! timeoutWorkerRunning)
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
          }

        /*****************************************
        *
        *  wait for next timeout
        *
        *****************************************/

        DeliveryRequest deliveryRequest = null;
        try
          {
            deliveryRequest = scheduledRequests.take();
          }
        catch (InterruptedException e)
          {
            deliveryRequest = null;
          }

        /*****************************************
        *
        *  process -- timeout?
        *
        *****************************************/

        if (deliveryRequest != null && deliveryRequest.getTimeout() != null && deliveryRequest.getTimeout().compareTo(SystemTime.getCurrentTime()) <= 0)
          {
            deliveryRequest.setDeliveryStatus(DeliveryStatus.FailedTimeout);
            completeRequest(deliveryRequest);
          }
      }
  }

  /*****************************************
   *
   *  processContactPolicyForRequest is used to validate the request based on contact policy rule
   *  If request is blocked by contact policy the completion of request is made inside of this method
   * @param request represent the delivery request to be evalued against contact policy rules.
   *                If is blocked by contact policy request status will be changed in BlockedByContactPolicy
   *
   *****************************************/

  private void processContactPolicyForRequest(DeliveryRequest request)
  {
    //if request is not type of any of contact policy affected request types do nothing
    if(request instanceof SMSNotificationManager.SMSNotificationManagerRequest || request instanceof PushNotificationManager.PushNotificationManagerRequest || request instanceof MailNotificationManager.MailNotificationManagerRequest)
      {
        RESTAPIGenericReturnCodes returnCode = RESTAPIGenericReturnCodes.UNKNOWN;
        boolean blockedByContactPolicy = false;
        try
          {
            blockedByContactPolicy = contactPolicyProcessor.ensureContactPolicy(request);
            if (blockedByContactPolicy)
              {
                request.setDeliveryStatus(DeliveryStatus.Failed);
                returnCode = RESTAPIGenericReturnCodes.BLOCKED_BY_CONTACT_POLICY;
              }
          }
        catch (Exception ex)
          {
            blockedByContactPolicy = true;
            request.setDeliveryStatus(DeliveryStatus.Failed);
            returnCode = RESTAPIGenericReturnCodes.CONTACT_POLICY_EVALUATION_ERROR;
            log.warn("Processing contact policy for " + request.getSubscriberID() + " failed", ex);
          }
        //because the return code is defined at each notification request type need to identify type cast and call setReturnCode
        if (blockedByContactPolicy)
          {
            if (request instanceof SMSNotificationManager.SMSNotificationManagerRequest)
              {
                ((SMSNotificationManager.SMSNotificationManagerRequest) request).setReturnCode(returnCode.getGenericResponseCode());
                ((SMSNotificationManager.SMSNotificationManagerRequest) request).setReturnCodeDetails(returnCode.toString());
              } else if (request instanceof PushNotificationManager.PushNotificationManagerRequest)
              {
                ((PushNotificationManager.PushNotificationManagerRequest) request).setReturnCode(returnCode.getGenericResponseCode());
                ((PushNotificationManager.PushNotificationManagerRequest) request).setReturnCodeDetails(returnCode.toString());
              } else if (request instanceof MailNotificationManager.MailNotificationManagerRequest)
              {
                ((MailNotificationManager.MailNotificationManagerRequest) request).setReturnCode(returnCode.getGenericResponseCode());
                ((MailNotificationManager.MailNotificationManagerRequest) request).setReturnCodeDetails(returnCode.toString());
              }
            completeRequest(request);
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
    *  revokeRequestConsumerPartitions
    *
    *****************************************/

    switch (managerStatus)
      {
        case Stopping:
          revokeRequestConsumerPartitions(requestConsumerAssignment);
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
                timeoutWorkerThread.join();
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
