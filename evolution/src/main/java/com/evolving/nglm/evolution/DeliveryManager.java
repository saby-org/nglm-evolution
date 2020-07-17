package com.evolving.nglm.evolution;

import com.evolving.nglm.core.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
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
    CheckBalanceLowerThan("checkbalancelowerthan"),
    CheckBalanceGreaterThan("checkbalancegreaterthan"),
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
    public static DeliveryStatus fromExternalRepresentation(String externalRepresentation) { for (DeliveryStatus enumeratedValue : DeliveryStatus.values()) { if (enumeratedValue.getExternalRepresentation().equals(externalRepresentation)) return enumeratedValue; }
    return Unknown; }
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

  //very very hacky, we need one instance of the subclass, we will populate this as soon as we got one
  private volatile static DeliveryRequest hackyDeliveryRequestInstance;
  private static CountDownLatch hackyDeliveryRequestInstanceReady = new CountDownLatch(1);

  private List<String> requestTopics;
  private String responseTopic;
  private String routingTopic;
  private int deliveryRatePerMinute;
  private int correlatorUpdateTimeoutSeconds;
  private long correlatorCleanerFrequencyMilliSeconds;
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
  private Date lastSubmitDate = NGLMRuntime.BEGINNING_OF_TIME;

  //
  //  serdes
  //

  private ConnectSerde<StringKey> stringKeySerde = StringKey.serde();

  //
  //  transactions
  //

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

  //
  //  threads
  //

  private Thread submitRequestWorkerThread = null;
  private Thread receiveCorrelatorUpdateWorkerThread = null;
  private Timer correlatorCleanerThread = null;

  //
  //  internal processing
  //
  private ContactPolicyProcessor contactPolicyProcessor = null;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getDeliveryManagerKey() { return deliveryManagerKey; }
  public List<String> getRequestTopics() { return requestTopics; }
  public String getResponseTopic() { return responseTopic; }
  public String getRoutingTopic() { return routingTopic; }
  public int getDeliveryRatePerMinute() { return deliveryRatePerMinute; }
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
        managerStatus = ManagerStatus.Delivering;
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
            if(Deployment.getEnableContactPolicyProcessing())
              {
                //if DeliveryStatus is BlockedByContactPolicy make result null and the next request from queue will be taken
                if (processRequestBlockedByContactPolicy(result)) result = null;
              }
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
    this.routingTopic = deliveryManagerDeclaration.getRoutingTopic();
    this.deliveryRatePerMinute = deliveryManagerDeclaration.getDeliveryRatePerMinute();
    this.correlatorUpdateTimeoutSeconds = deliveryManagerDeclaration.getCorrelatorUpdateTimeoutSeconds();
    this.correlatorCleanerFrequencyMilliSeconds = deliveryManagerDeclaration.getCorrelatorCleanerFrequencyMilliSeconds();

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
    routingConsumerProperties.put("group.id", applicationID + "-routing");
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
    *  correlatorCleaner worker
    *
    *****************************************/
    correlatorCleanerThread = new Timer(applicationID + "-CorrelatorCleaner",true);
    correlatorCleanerThread.scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        long startTime = System.currentTimeMillis();
        log.info("start execution");
        int removed=0;
        try{
          if(waitingForCorrelatorUpdate!=null){
            Set<String> toRemove = new HashSet<>();
            // concurrent scan non locking (no clue if this is risky)
            waitingForCorrelatorUpdate.entrySet().forEach(entry->{if(entry.getValue().getTimeout().after(SystemTime.getCurrentTime())) toRemove.add(entry.getKey());});
            synchronized (this){
              for(String key:toRemove){
                waitingForCorrelatorUpdate.remove(key);
                removed++;
              }
            }
          }
        }catch (Exception ex){
          log.error("error during cleaning",ex);
        }
        log.info("finish execution removing {} entries in {} ms",removed,System.currentTimeMillis()-startTime);
      }
    }, correlatorCleanerFrequencyMilliSeconds, correlatorCleanerFrequencyMilliSeconds);

    /*****************************************
     *
     *  contact policy processor
     *
     *****************************************/

    // skip if not needed
    if(deliveryManagerDeclaration.getDeliveryType().equals(CommodityDeliveryManager.COMMODITY_DELIVERY_MANAGER_NAME)){
      log.info("skipping contactPolicyProcessor init for "+deliveryManagerDeclaration.getDeliveryType());
    }else{
      contactPolicyProcessor = new ContactPolicyProcessor("deliveryManager-communicationchannel", deliveryManagerKey);
    }
  }


  /*****************************************
  *
  *  runSubmitRequestWorker
  *
  *****************************************/

  private void runSubmitRequestWorker()
  {

    // wait start
    synchronized (this){
      while(!managerStatus.isDeliveringRequests()){
        try {
          this.wait();
        } catch (InterruptedException e){}
      }
    }
    log.info("runSubmitRequestWorker: starting delivery");

    ConsumerRebalanceListener listener = new ConsumerRebalanceListener()
    {
      @Override public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        synchronized (this) {
          log.info("requestConsumer partitions revoked: {}", partitions);
          int nbWaiting = waitingForAcknowledgement.size();
          waitingForAcknowledgement.clear();
          log.info("requestConsumer partitions {} pending in memory request removed, {} request waiting for acknowledgement removed",submitRequestQueue.drainTo(new ArrayList<>(),nbWaiting));
        }
      }
      @Override public void onPartitionsAssigned(Collection<TopicPartition> partitions) { log.info("requestConsumer partitions assigned: {}", partitions); }
    };

    requestConsumer.subscribe(requestTopics,listener);
    while (managerStatus.isRunning())
    {
      /****************************************
       *
       *  poll for requests
       *
       ****************************************/

      long lastPollTime=System.currentTimeMillis();
      ConsumerRecords<byte[], byte[]> requestRecords;
      try
      {
        requestRecords = requestConsumer.poll(5000);
      }
      catch (WakeupException e)
      {
        requestRecords = ConsumerRecords.<byte[], byte[]>empty();
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
        setHackyDeliveryRequestInstanceIfNeeded(deliveryRequest);

        //
        //  partition
        //

        deliveryRequest.setDeliveryPartition(requestRecord.partition());

        //
        //  process
        //

        submitDeliveryRequest(deliveryRequest, false, false, requestRecord.topic(), requestRecord.offset());
      }
      if (managerStatus.isDeliveringRequests()){
        try{
          requestConsumer.commitSync();
        }catch (CommitFailedException ex){
          long lastPoll_ms=System.currentTimeMillis()-lastPollTime;
          log.info("CommitFailedException catched, can be normal rebalancing or poll time interval too long, last was {}ms ago",lastPoll_ms);
        }
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

    // timeout in correlator waiting memory queue
    Date timeout = RLMDateUtils.addSeconds(SystemTime.getCurrentTime(), getCorrelatorUpdateTimeoutSeconds());
    deliveryRequest.setTimeout(timeout);

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

          }

        /*****************************************
        *
        *  commit
        *
        *****************************************/

        // forward the deliveryRequest to the instance that will received correlator response
        kafkaProducer.send(new ProducerRecord<byte[], byte[]>(routingTopic, stringKeySerde.serializer().serialize(routingTopic, new StringKey(deliveryRequest.getCorrelator())), requestSerde.optionalSerializer().serialize(routingTopic, deliveryRequest)));

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
            boolean rescheduled = (deliveryRequest.getDeliveryStatus() != null && deliveryRequest.getDeliveryStatus().equals(DeliveryStatus.Reschedule));
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

        StringKey key = deliveryRequest.getOriginatingRequest() ? new StringKey(deliveryRequest.getSubscriberID()) : new StringKey(deliveryRequest.getDeliveryRequestID());
        kafkaProducer.send(new ProducerRecord<byte[], byte[]>(responseTopic, stringKeySerde.serializer().serialize(responseTopic, key), requestSerde.serializer().serialize(responseTopic, deliveryRequest)));

      }
  }

  /*****************************************
   *
   *  processSubmitCorrelatorUpdate  (note:  runs on subclass thread "blocking" until complete)
   *
   *****************************************/

  private static final String CORRELATOR_UPDATE_KEY = "correlatorUpdate";
  private void processSubmitCorrelatorUpdate(String correlator, JSONObject correlatorUpdate)
  {
    // construct a fake delivery request containing only correlatorUpdate to forward to the right instance
    if(hackyDeliveryRequestInstance==null){
      try {
        log.info("DeliveryManager processSubmitCorrelatorUpdate need to wait to deserialize at least one object for a hack ...");
        hackyDeliveryRequestInstanceReady.await();// need to wait we got the hacky instance....
        log.info("DeliveryManager processSubmitCorrelatorUpdate hacky object instance ready");
      } catch (InterruptedException e) {}
    }
    DeliveryRequest deliveryRequestForCorrelatorUpdate = hackyDeliveryRequestInstance.copy();
    deliveryRequestForCorrelatorUpdate.getDiplomaticBriefcase().put(CORRELATOR_UPDATE_KEY,correlatorUpdate.toJSONString());
    kafkaProducer.send(new ProducerRecord<byte[], byte[]>(routingTopic, stringKeySerde.serializer().serialize(routingTopic, new StringKey(correlator)), requestSerde.serializer().serialize(routingTopic, deliveryRequestForCorrelatorUpdate)));
  }

  /*****************************************
   *
   *  revokeRoutingConsumerPartitions
   *
   *****************************************/

  private void revokeRoutingConsumerPartitions(Collection<TopicPartition> partitions)
  {
    synchronized (this)
    {
      log.info("routingConsumer partitions revoked: {}", partitions);
      waitingForCorrelatorUpdate.clear();
    }
  }

  /*****************************************
   *
   *  assignRoutingConsumerPartitions
   *
   *****************************************/

  private void assignRoutingConsumerPartitions(Collection<TopicPartition> partitions)
  {
    log.info("routingConsumer partitions assigned: {}", partitions);
    synchronized (this) {
      if (waitingForCorrelatorUpdate.size() > 0) throw new RuntimeException("waitingForCorrelatorUpdate size: " + waitingForCorrelatorUpdate.size());
    }

    /*****************************************
    *
    *  read routing to (re-) populate data structures
    *
    *****************************************/

    Properties routingProgressConsumerProperties = new Properties();
    routingProgressConsumerProperties.put("bootstrap.servers", bootstrapServers);
    routingProgressConsumerProperties.put("auto.offset.reset", "earliest");
    routingProgressConsumerProperties.put("enable.auto.commit", "false");
    routingProgressConsumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    routingProgressConsumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    KafkaConsumer<byte[], byte[]> routingProgressConsumer = new KafkaConsumer<>(routingProgressConsumerProperties);

    //
    //  assign topic partitions
    //

    Set<TopicPartition> routingProgressConsumerPartitions = new HashSet<TopicPartition>();
    for (TopicPartition topicPartition : partitions)
    {
      routingProgressConsumerPartitions.add(new TopicPartition(routingTopic, topicPartition.partition()));
    }
    routingProgressConsumer.assign(routingProgressConsumerPartitions);

    //
    //  initialize consumedOffsets
    //

    boolean consumedAllAvailable = false;
    Map<TopicPartition,Long> consumedOffsets = new HashMap<TopicPartition,Long>();
    for (TopicPartition topicPartition : routingProgressConsumer.assignment())
    {
      consumedOffsets.put(topicPartition, routingProgressConsumer.position(topicPartition) - 1L);
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

      ConsumerRecords<byte[], byte[]> progressRecords = (routingProgressConsumer.assignment().size() > 0) ? routingProgressConsumer.poll(5000) : ConsumerRecords.<byte[], byte[]>empty();

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

        String correlator = stringKeySerde.deserializer().deserialize(topicPartition.topic(), progressRecord.key()).getKey();
        DeliveryRequest deliveryRequest;
        try{
          deliveryRequest = requestSerde.optionalDeserializer().deserialize(topicPartition.topic(), progressRecord.value());
        }catch (ClassCastException ex){
          log.info("DeliveryManager assignRoutingConsumerPartitions: old routing format message, skipping");
          continue;
        }
        if (deliveryRequest != null && deliveryRequest.getDiplomaticBriefcase().get(CORRELATOR_UPDATE_KEY)==null && deliveryRequest.getTimeout()!=null && deliveryRequest.getTimeout().after(SystemTime.getCurrentTime()) )
        {
          setHackyDeliveryRequestInstanceIfNeeded(deliveryRequest);
          restartRequests.put(correlator, deliveryRequest);
        }
        else
        {
          restartRequests.remove(correlator);
        }

        //
        //  offsets
        //

        consumedOffsets.put(topicPartition, progressRecord.offset());
      }

      //
      //  consumed all available?
      //

      Map<TopicPartition,Long> availableOffsets = routingProgressConsumer.endOffsets(routingProgressConsumerPartitions);
      consumedAllAvailable = true;
      for (TopicPartition topicPartition : availableOffsets.keySet())
      {
        Long availableOffsetForPartition = availableOffsets.get(topicPartition);
        Long consumedOffsetForPartition = consumedOffsets.get(topicPartition);
        if (consumedOffsetForPartition == null)
        {
          consumedOffsetForPartition = routingProgressConsumer.position(topicPartition) - 1L;
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

    routingProgressConsumer.close();

    /****************************************
     *
     *  restart
     *
     ****************************************/

    synchronized (this)
    {
      for (DeliveryRequest deliveryRequest : restartRequests.values())
      {

        if(log.isDebugEnabled()) log.debug("waitingForCorrelatorUpdate.put (restart): {} {}", deliveryRequest.getCorrelator(), deliveryRequest);
        waitingForCorrelatorUpdate.put(deliveryRequest.getCorrelator(), deliveryRequest);

      }
      log.debug("waitingForCorrelatorUpdate.put (restart): waitingForCorrelatorUpdate rebuilt with size {}", waitingForCorrelatorUpdate.size());
    }

  }

  /*****************************************
  *
  *  runReceiveCorrelatorWorker
  *
  *****************************************/

  private void runReceiveCorrelatorWorker()
  {
    /*****************************************
    *
    *  subscribe to routing topic (note: assignRoutingConsumerPartitions will be called inside the next call to poll)
    *
    *****************************************/

    ConsumerRebalanceListener listener = new ConsumerRebalanceListener()
    {
      @Override public void onPartitionsRevoked(Collection<TopicPartition> partitions) {  revokeRoutingConsumerPartitions(partitions); }
      @Override public void onPartitionsAssigned(Collection<TopicPartition> partitions) { assignRoutingConsumerPartitions(partitions); }
    };
    routingConsumer.subscribe(Arrays.asList(routingTopic), listener);

    // wait start
    synchronized (this){
      while(!managerStatus.isDeliveringRequests()){
        try {
          this.wait();
        } catch (InterruptedException e){}
      }
    }
    log.info("runReceiveCorrelatorWorker: starting delivery");

    while (managerStatus.isProcessingResponses())
    {

      /****************************************
      *
      *  poll for correlatorUpdates
      *
      ****************************************/

      long lastPollTime=System.currentTimeMillis();
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
        //  deserialize
        //

        String correlator = stringKeySerde.deserializer().deserialize(routingTopic, correlatorUpdateRecord.key()).getKey();
        DeliveryRequest deliveryRequest;
        try{
          deliveryRequest = requestSerde.deserializer().deserialize(routingTopic, correlatorUpdateRecord.value());
        }catch (ClassCastException ex){
          log.info("DeliveryManager correlatorUpdate: old routing format message, skipping");
          continue;
        }
        setHackyDeliveryRequestInstanceIfNeeded(deliveryRequest);
        String correlatorUpdateJSON = deliveryRequest.getDiplomaticBriefcase().get(CORRELATOR_UPDATE_KEY);

        if(correlatorUpdateJSON==null)
        {
          // this is the case we received a request that will wait for a correlator response

          if(deliveryRequest.getCorrelator()==null)
          {
            log.warn("DeliveryManager correlatorUpdate: received request for correlator response without correlator !");
            continue;
          }
          if(!deliveryRequest.getCorrelator().equals(correlator))
          {
            log.warn("DeliveryManager correlatorUpdate: received badly routed correlator {} {} !", deliveryRequest.getCorrelator(), correlator);
            continue;
          }

          synchronized (this)
          {
            waitingForCorrelatorUpdate.put(correlator,deliveryRequest);
            if(log.isDebugEnabled()) log.debug("DeliveryManager correlatorUpdate : adding waitingForCorrelatorUpdate {}, new waitingForCorrelatorUpdate size {}",correlator,waitingForCorrelatorUpdate.size());
          }

        }
        else
        {
          // this is the case we received a correlator response for a request we do have

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

          if (correlatorUpdate != null)
          {
            synchronized (this)
            {
              deliveryRequest = waitingForCorrelatorUpdate.get(correlator);
            }
          }

          if(deliveryRequest==null)
          {
            log.info("receiveCorrelatorUpdate for a request we do not store {}",correlator);
            continue;
          }

          //
          //  log (debug)
          //

          if(log.isDebugEnabled()) log.debug("receiveCorrelatorUpdate: {} {} {}", correlator, correlatorUpdate, deliveryRequest);

          //
          //  update
          //

          processCorrelatorUpdate(deliveryRequest, correlatorUpdate);

        }

      }
      try{
        routingConsumer.commitSync();
      }catch (CommitFailedException ex){
        long lastPoll_ms=System.currentTimeMillis()-lastPollTime;
        log.info("CommitFailedException catched, can be normal rebalancing or poll time interval too long, last was {}ms ago",lastPoll_ms);
      }
    }
  }

  // this is to set our hacky instance the right way
  private void setHackyDeliveryRequestInstanceIfNeeded(DeliveryRequest deliveryRequest){
    if(hackyDeliveryRequestInstance==null){
      synchronized (hackyDeliveryRequestInstanceReady){
        if(hackyDeliveryRequestInstance==null){
          hackyDeliveryRequestInstance=deliveryRequest.copy();
          hackyDeliveryRequestInstance.setCorrelator(null);
          hackyDeliveryRequestInstance.setDiplomaticBriefcase(new HashMap<>());
          hackyDeliveryRequestInstanceReady.countDown();
        }
      }
    }
  }

  /*****************************************
   *
   *  processContactPolicyForRequest is used to validate the request based on contact policy rule
   *  delivery status and any information needed for blocking by contact policy is processed inside of method
   * @param request represent the delivery request to be evalued against contact policy rules.
   * @return boolean If request in blocked by an contact policy rule return true
   *
   *****************************************/

  private boolean processRequestBlockedByContactPolicy(DeliveryRequest request)
  {
    boolean blockedByContactPolicy = false;
    //if request is not type of any of contact policy affected request types do nothing
    if(request instanceof SMSNotificationManager.SMSNotificationManagerRequest || request instanceof PushNotificationManager.PushNotificationManagerRequest || request instanceof MailNotificationManager.MailNotificationManagerRequest)
      {
        RESTAPIGenericReturnCodes returnCode = RESTAPIGenericReturnCodes.UNKNOWN;
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
    return blockedByContactPolicy;
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
