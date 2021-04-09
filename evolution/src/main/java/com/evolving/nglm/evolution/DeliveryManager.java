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
    BonusNotFound("bonusnotfound"),
    InsufficientBalance("insufficientbalance"),
    CheckBalanceLowerThan("checkbalancelowerthan"),
    CheckBalanceGreaterThan("checkbalancegreaterthan"),
    CheckBalanceEqualsTo("checkbalanceequalsto"),
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
  //  ManagerStatus
  //

  public enum ManagerStatus
  {
    Created,
    Delivering,
    Stopping;
    public boolean isCreated() { return this==Created;}
    public boolean isRunning() { return this==Delivering; }
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
  
  public static final String TARGETED = "targeted-";
  public static final String ORIGIN = "origin-";

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

  private DeliveryManagerDeclaration deliveryManagerDeclaration;

  private int deliveryRatePerMinute;
  private int correlatorUpdateTimeoutSeconds;
  private long correlatorCleanerFrequencyMilliSeconds;
  //
  //  derived configuration
  //

  private int millisecondsPerDelivery;
  private int maxOutstandingRequests;
  private int oneLoopConfiguredProcessTimeMs;

  //
  //  status
  //

  private volatile ManagerStatus managerStatus = ManagerStatus.Created;
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
  private static ContactPolicyProcessor contactPolicyProcessor = null;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getDeliveryManagerKey() { return deliveryManagerKey; }
  public int getDeliveryRatePerMinute() { return deliveryRatePerMinute; }
  public int getCorrelatorUpdateTimeoutSeconds() { return correlatorUpdateTimeoutSeconds; }

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
  *  nextRequest
  *
  *****************************************/

  protected DeliveryRequest nextRequest()
  {
    DeliveryRequest result = null;
    while (result == null)
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

  protected DeliveryManager(String applicationID, String deliveryManagerKey, String bootstrapServers, ConnectSerde<? extends DeliveryRequest> requestSerde, DeliveryManagerDeclaration deliveryManagerDeclaration, int workerThreadNumber)
  {
    this(applicationID, applicationID, deliveryManagerKey, bootstrapServers, requestSerde, deliveryManagerDeclaration, workerThreadNumber);
  }
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  protected DeliveryManager(String applicationID, String groupIdForRouting, String deliveryManagerKey, String bootstrapServers, ConnectSerde<? extends DeliveryRequest> requestSerde, DeliveryManagerDeclaration deliveryManagerDeclaration, int workerThreadNumber)
  {
    /*****************************************
    *
    *  runtime
    *
    *****************************************/

    NGLMRuntime.initialize(true);

    if (deliveryManagerDeclaration == null) throw new RuntimeException("invalid delivery manager (no such delivery manager)");
    if (deliveryManagerDeclaration.getRequestTopics().size() == 0) throw new RuntimeException("invalid delivery manager (no request topic)");

    /*****************************************
    *
    *  configuration
    *
    *****************************************/

    this.applicationID = applicationID;
    this.deliveryManagerKey = deliveryManagerKey;
    this.bootstrapServers = bootstrapServers;
    this.requestSerde = (ConnectSerde<DeliveryRequest>) requestSerde;
    this.deliveryManagerDeclaration = deliveryManagerDeclaration;
    this.deliveryRatePerMinute = deliveryManagerDeclaration.getDeliveryRatePerMinute();
    this.correlatorUpdateTimeoutSeconds = deliveryManagerDeclaration.getCorrelatorUpdateTimeoutSeconds();
    this.correlatorCleanerFrequencyMilliSeconds = deliveryManagerDeclaration.getCorrelatorCleanerFrequencyMilliSeconds();

    /*****************************************
    *
    *  calculated
    *
    *****************************************/

    millisecondsPerDelivery = (int) Math.ceil(1.0 / ((double) getDeliveryRatePerMinute() / (60.0 * 1000.0)));
    maxOutstandingRequests = workerThreadNumber;// this put a cap to how much request could be duplicated in case of hard failure
    if(deliveryManagerDeclaration.getMaxBufferedRequests()>0) maxOutstandingRequests=deliveryManagerDeclaration.getMaxBufferedRequests();// could be forced for extreme cases
    // this is, per conf, how much time it should takes to process "one loop" of maxOutstandingRequests at deliveryRatePerMinute throughput configuration
    oneLoopConfiguredProcessTimeMs = maxOutstandingRequests*millisecondsPerDelivery;

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
    requestConsumerProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxOutstandingRequests);// for low tps, needed otherwise will just keep rebalancing
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
    routingConsumerProperties.put("group.id", groupIdForRouting + "-routing");
    log.info("MK creating routing consumer with group = " + groupIdForRouting + "-routing");
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

    if(deliveryManagerDeclaration.getRoutingTopic()!=null){
      Runnable receiveCorrelatorUpdateWorker = new Runnable() { @Override public void run() { runReceiveCorrelatorWorker(); } };
      receiveCorrelatorUpdateWorkerThread = new Thread(receiveCorrelatorUpdateWorker, applicationID + "-ReceiveCorrelatorWorker");
      receiveCorrelatorUpdateWorkerThread.start();
    }else{
      log.info("no routing topic declaration, no correlator update consumer");
    }

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
        int nbTimeOut=0;
        try{
          if(waitingForCorrelatorUpdate!=null){
            List<DeliveryRequest> toSendBackTimeout = new ArrayList<>();
            // concurrent scan non locking (no clue if this is risky)
            waitingForCorrelatorUpdate.entrySet().forEach(entry->{if(entry.getValue().getTimeout().before(SystemTime.getCurrentTime())) toSendBackTimeout.add(entry.getValue());});
            for(DeliveryRequest deliveryRequest:toSendBackTimeout){
              deliveryRequest.setDeliveryStatus(DeliveryStatus.FailedTimeout);
              // note there is no synchronisation so the request response might have been sent in the mean time, which should be OK and just throw then an info log
              completeRequest(deliveryRequest);
              nbTimeOut++;
            }
          }
        }catch (Exception ex){
          log.error("correlatorCleanerThread : error during cleaning",ex);
        }
        log.info("correlatorCleanerThread : finish execution, sent {} timeout request in {} ms, over {} total in memory requests",nbTimeOut,System.currentTimeMillis()-startTime,waitingForCorrelatorUpdate.size());
      }
    }, correlatorCleanerFrequencyMilliSeconds, correlatorCleanerFrequencyMilliSeconds);

    /*****************************************
     *
     *  contact policy processor
     *
     *****************************************/

    // skip if not needed TODO: move to DeliveryManagerForNotifications if not common
    if(deliveryManagerDeclaration.getDeliveryType().equals(CommodityDeliveryManager.COMMODITY_DELIVERY_TYPE)){
      log.info("skipping contactPolicyProcessor init for "+deliveryManagerDeclaration.getDeliveryType());
    }else{
      if(contactPolicyProcessor==null){
        synchronized (this){
          if(contactPolicyProcessor==null){
            contactPolicyProcessor = new ContactPolicyProcessor();
          }
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

    // wait start
    synchronized (this){
      while(managerStatus.isCreated()){
        try {
          this.wait();
        } catch (InterruptedException e){}
      }
    }
    log.info("runSubmitRequestWorker : starting {} delivery from {} inputs",deliveryManagerDeclaration.getDeliveryType(),deliveryManagerDeclaration.getRequestTopicsList());

    ConsumerRebalanceListener listener = new ConsumerRebalanceListener()
    {
      @Override public void onPartitionsRevoked(Collection<TopicPartition> revokedPartitions) {
        synchronized (this) {
          log.info("requestConsumer.onPartitionsRevoked() : partitions revoked: {}", revokedPartitions);
          // just log some info, but we actually just do nothing here
          // this onPartitionsRevoked can only happen during the "poll()"
          // and if happening, no records should be returned by this "poll()"
          // so previous records have already been "commitSync" to kafka
          // so new deliveryManager will not process them
          long nbWaiting = waitingForAcknowledgement.values().stream().filter(dr->revokedPartitions.contains(dr.getTopicPartition())).count();
          log.info("requestConsumer.onPartitionsRevoked() : {} requests pending in memory for those partitions, will still be done by this instance",nbWaiting);
        }
      }
      @Override public void onPartitionsAssigned(Collection<TopicPartition> partitions) { log.info("requestConsumer.onPartitionsAssigned() partitions assigned: {}", partitions); }
    };

    requestConsumer.subscribe(deliveryManagerDeclaration.getRequestTopicsList(),listener);
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
        requestRecords = requestConsumer.poll(1000);
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
        //  topicPartition
        //

        TopicPartition topicPartition = new TopicPartition(requestRecord.topic(), requestRecord.partition());

        //
        //  parse
        //

        DeliveryRequest deliveryRequest = requestSerde.deserializer().deserialize(topicPartition.topic(), requestRecord.value());
        deliveryRequest.setTopicPartition(topicPartition);
        setHackyDeliveryRequestInstanceIfNeeded(deliveryRequest);

        //
        //  process
        //

        submitDeliveryRequest(deliveryRequest);
      }

      if (requestRecords.count()>0){
        try{
          // if we need at AtLeastOnce, need to wait processing to finish and then only commit progress
          if(deliveryManagerDeclaration.getDeliveryGuarantee()==DeliveryManagerDeclaration.DeliveryGuarantee.AtLeastOnce){
            waitForAllInMemoryJobs();
          }
          requestConsumer.commitSync();
        }catch (CommitFailedException ex){
          long lastPoll_ms=System.currentTimeMillis()-lastPollTime;
          log.warn("runSubmitRequestWorker : CommitFailedException caught, last poll was {}ms ago",lastPoll_ms);
        }
      }

    }
  }

  /*****************************************
  *
  *  submitDeliveryRequest
  *
  *****************************************/

  private void submitDeliveryRequest(DeliveryRequest deliveryRequest)
  {

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

        while (waitingForAcknowledgement.size() >= maxOutstandingRequests)
          {
            try
              {
                this.wait();
              }
            catch (InterruptedException e)
              {
              }
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
    while (now.before(nextSubmitDate))
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

    /*****************************************
    *
    *  log (debug)
    *
    *****************************************/

    if(log.isDebugEnabled()) log.debug("submitDeliveryRequest : {}", deliveryRequest);

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
  *  processUpdateRequest
  *
  *****************************************/

  private void processUpdateRequest(DeliveryRequest deliveryRequest)
  {
      /*****************************************
      *
      *  log (debug)
      *
      *****************************************/

      if(log.isDebugEnabled()) log.debug("processUpdateRequest : {}", deliveryRequest);

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
              log.warn("processUpdateRequest : request not hold {}", deliveryRequest);
              return;
            }

        }

      /*****************************************
      *
      *  commit
      *
      *****************************************/

      // forward the deliveryRequest to the instance that will received correlator response
      if(deliveryManagerDeclaration.getRoutingTopic()!=null){
        String routingTopic = deliveryManagerDeclaration.getRoutingTopic().getName();
        kafkaProducer.send(new ProducerRecord<byte[], byte[]>(routingTopic, stringKeySerde.serializer().serialize(routingTopic, new StringKey(deliveryRequest.getCorrelator())), requestSerde.optionalSerializer().serialize(routingTopic, deliveryRequest)));
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
        }
  }

  /*****************************************
  *
  *  processCompleteRequest
  *
  *****************************************/

  private void processCompleteRequest(DeliveryRequest deliveryRequest)
  {

      /*****************************************
      *
      *  log (debug)
      *
      *****************************************/

      if(log.isDebugEnabled()) log.debug("processCompleteRequest : {}", deliveryRequest);

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
              log.info("processCompleteRequest :  request not hold  {}", deliveryRequest);
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

      String responseTopic = deliveryManagerDeclaration.getResponseTopic(deliveryRequest.getDeliveryPriority());
      if(deliveryRequest.getOriginatingRequest())
        {
          // response to be sent indexed by subscriber ID
          if(deliveryRequest.getOriginatingSubscriberID() != null) {
            // EVPRO-178
            // means this request has been made by originatingSubscriberID on behalf of current subscsriberID
            // so need to send a response to:
            // - originatingSubscriberID to unlock the current state of its Journey: SubscriberID = originatingSubscriberID and originatingSubscriberID becomes targeted-<effectivelyTargeted>
            // - targeted subscriberID (i.e. the one which effectively got, by example, a SMS..., for delivery history: subscriberID = currentSubscriberID and originatingSubscriberID origin-<originating>
            String originating = deliveryRequest.getOriginatingSubscriberID();
            String targeted = deliveryRequest.getSubscriberID();

            // send the response to the originating:
            deliveryRequest.setSubscriberID(originating);
            deliveryRequest.setOriginatingSubscriberID(TARGETED + targeted);
            StringKey key = new StringKey(originating);
            kafkaProducer.send(new ProducerRecord<byte[], byte[]>(responseTopic, stringKeySerde.serializer().serialize(responseTopic, key), requestSerde.serializer().serialize(responseTopic, deliveryRequest)));

            // send the response to the targetted
            deliveryRequest.setSubscriberID(targeted);
            deliveryRequest.setOriginatingSubscriberID(ORIGIN + originating);
            key = new StringKey(targeted);
            kafkaProducer.send(new ProducerRecord<byte[], byte[]>(responseTopic, stringKeySerde.serializer().serialize(responseTopic, key), requestSerde.serializer().serialize(responseTopic, deliveryRequest)));

          }
        else
          {
            // normal case
            StringKey key = new StringKey(deliveryRequest.getSubscriberID());
            kafkaProducer.send(new ProducerRecord<byte[], byte[]>(responseTopic, stringKeySerde.serializer().serialize(responseTopic, key), requestSerde.serializer().serialize(responseTopic, deliveryRequest)));
          }
        }
      else
        {
          // index by requestID
          StringKey key = new StringKey(deliveryRequest.getDeliveryRequestID());
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
        log.warn("processSubmitCorrelatorUpdate : need to wait to deserialize at least one object for a hack ...");
        hackyDeliveryRequestInstanceReady.await();// need to wait we got the hacky instance....
        log.info("processSubmitCorrelatorUpdate : hacky object instance ready");
      } catch (InterruptedException e) {}
    }
    DeliveryRequest deliveryRequestForCorrelatorUpdate = hackyDeliveryRequestInstance.copy();
    deliveryRequestForCorrelatorUpdate.getDiplomaticBriefcase().put(CORRELATOR_UPDATE_KEY,correlatorUpdate.toJSONString());
    if(deliveryManagerDeclaration.getRoutingTopic()!=null){
      String routingTopic = deliveryManagerDeclaration.getRoutingTopic().getName();
      kafkaProducer.send(new ProducerRecord<byte[], byte[]>(routingTopic, stringKeySerde.serializer().serialize(routingTopic, new StringKey(correlator)), requestSerde.serializer().serialize(routingTopic, deliveryRequestForCorrelatorUpdate)));
    }else{
      log.error("processSubmitCorrelatorUpdate : on declaration without routing topic!");
    }

  }

  private void revokeRoutingConsumerPartitions(Collection<TopicPartition> revokedPartitions) {
    synchronized (this) {
      log.info("routingConsumer.revokeRoutingConsumerPartitions() : partitions revoked {}", revokedPartitions);
      int oldSize = waitingForCorrelatorUpdate.size();
      // we can clean up those, this is the new instance that will now received the update
      waitingForCorrelatorUpdate.values().removeIf(deliveryRequest -> revokedPartitions.contains(deliveryRequest.getTopicPartition()));
      log.info("routingConsumer.revokeRoutingConsumerPartitions() : cleaning waitingForCorrelatorUpdate done, old size {}, new size {}", oldSize, waitingForCorrelatorUpdate.size());
    }
  }

  private void assignRoutingConsumerPartitions(Collection<TopicPartition> partitions)
  {
    log.info("routingConsumer.assignRoutingConsumerPartitions() : partitions assigned {}", partitions);

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

    Set<TopicPartition> routingProgressConsumerPartitions = new HashSet<>();
    for (TopicPartition topicPartition : partitions)
    {
      routingProgressConsumerPartitions.add(new TopicPartition(topicPartition.topic(), topicPartition.partition()));
    }
    routingProgressConsumer.assign(routingProgressConsumerPartitions);

    //
    //  initialize consumedOffsets
    //

    boolean consumedAllAvailable = false;
    Map<TopicPartition,Long> consumedOffsets = new HashMap<>();
    Map<TopicPartition,Long> lastOffsetsToConsume = new HashMap<>();
    for (TopicPartition topicPartition : routingProgressConsumer.assignment())
    {
      consumedOffsets.put(topicPartition, routingProgressConsumer.position(topicPartition)-1);
      lastOffsetsToConsume.put(topicPartition, routingConsumer.position(topicPartition)-1);
    }

    log.info("routingConsumer.assignRoutingConsumerPartitions() : will reload all from "+consumedOffsets+" till : "+lastOffsetsToConsume);

    //
    //  read
    //

    SortedMap<String,DeliveryRequest> restartRequests = new TreeMap<>();
    do
    {
      //
      // poll
      //

      ConsumerRecords<byte[], byte[]> progressRecords = (routingProgressConsumer.assignment().size() > 0) ? routingProgressConsumer.poll(100) : ConsumerRecords.empty();

      //
      //  process
      //

      for (ConsumerRecord<byte[], byte[]> progressRecord : progressRecords)
      {
        //
        //  topicPartition
        //

        TopicPartition topicPartition = new TopicPartition(progressRecord.topic(), progressRecord.partition());

        // skip if routingConsumer not yet consumed it
        if(progressRecord.offset()>lastOffsetsToConsume.get(topicPartition)){
          if(log.isDebugEnabled()) log.debug("skipping offset "+topicPartition+":"+progressRecord.offset()+", because "+lastOffsetsToConsume.get(topicPartition));
          continue;
        }

        //
        //  parse
        //

        String correlator = stringKeySerde.deserializer().deserialize(topicPartition.topic(), progressRecord.key()).getKey();
        DeliveryRequest deliveryRequest = null;
        try{
          deliveryRequest = requestSerde.optionalDeserializer().deserialize(topicPartition.topic(), progressRecord.value());
          setHackyDeliveryRequestInstanceIfNeeded(deliveryRequest);
          deliveryRequest.setTopicPartition(topicPartition);
        }catch (ClassCastException ex){
          log.info("routingConsumer.assignRoutingConsumerPartitions() : old routing format message, skipping");
        }
        if (deliveryRequest != null && deliveryRequest.getDiplomaticBriefcase().get(CORRELATOR_UPDATE_KEY)==null && deliveryRequest.getTimeout()!=null && deliveryRequest.getTimeout().after(SystemTime.getCurrentTime()) )
        {
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

      consumedAllAvailable = true;
      for (TopicPartition topicPartition : lastOffsetsToConsume.keySet())
      {
        Long lastOffsetsToConsumeForPartition = lastOffsetsToConsume.get(topicPartition);
        Long consumedOffsetForPartition = consumedOffsets.get(topicPartition);
        if (consumedOffsetForPartition == null)
        {
          consumedOffsetForPartition = routingProgressConsumer.position(topicPartition)-1;
          consumedOffsets.put(topicPartition, consumedOffsetForPartition);
        }
        if (consumedOffsetForPartition < lastOffsetsToConsumeForPartition)
        {
          log.info("routingConsumer.assignRoutingConsumerPartitions() : "+topicPartition.topic()+":"+topicPartition.partition()+", still not reached end ("+consumedOffsetForPartition+" vs "+lastOffsetsToConsumeForPartition+")");
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

        if(log.isDebugEnabled()) log.debug("routingConsumer.assignRoutingConsumerPartitions() : (restart) {} {}", deliveryRequest.getCorrelator(), deliveryRequest);
        waitingForCorrelatorUpdate.put(deliveryRequest.getCorrelator(), deliveryRequest);

      }
      log.info("routingConsumer.assignRoutingConsumerPartitions() : waitingForCorrelatorUpdate rebuilt with size {}", waitingForCorrelatorUpdate.size());
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
    routingConsumer.subscribe(Collections.singleton(deliveryManagerDeclaration.getRoutingTopic().getName()), listener);

    // wait start
    synchronized (this){
      while(managerStatus.isCreated()){
        try {
          this.wait();
        } catch (InterruptedException e){}
      }
    }
    log.info("runReceiveCorrelatorWorker : starting delivery");

    while (managerStatus.isRunning())
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
        correlatorUpdateRecords = routingConsumer.poll(1000);
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

        String correlator = stringKeySerde.deserializer().deserialize(correlatorUpdateRecord.topic(), correlatorUpdateRecord.key()).getKey();
        DeliveryRequest deliveryRequest;
        try{
          deliveryRequest = requestSerde.deserializer().deserialize(correlatorUpdateRecord.topic(), correlatorUpdateRecord.value());
          deliveryRequest.setTopicPartition(new TopicPartition(correlatorUpdateRecord.topic(),correlatorUpdateRecord.partition()));
        }catch (ClassCastException ex){
          log.info("runReceiveCorrelatorWorker : old routing format message, skipping");
          continue;
        }
        setHackyDeliveryRequestInstanceIfNeeded(deliveryRequest);
        String correlatorUpdateJSON = deliveryRequest.getDiplomaticBriefcase().get(CORRELATOR_UPDATE_KEY);

        if(correlatorUpdateJSON==null)
        {
          // this is the case we received a request that will wait for a correlator response

          if(deliveryRequest.getCorrelator()==null)
          {
            log.warn("runReceiveCorrelatorWorker : received request for correlator response without correlator !");
            continue;
          }
          if(!deliveryRequest.getCorrelator().equals(correlator))
          {
            log.warn("runReceiveCorrelatorWorker : received badly routed correlator {} {} !", deliveryRequest.getCorrelator(), correlator);
            continue;
          }

          synchronized (this)
          {
            waitingForCorrelatorUpdate.put(correlator,deliveryRequest);
            if(log.isDebugEnabled()) log.debug("runReceiveCorrelatorWorker : adding waitingForCorrelatorUpdate {}, new waitingForCorrelatorUpdate size {}",correlator,waitingForCorrelatorUpdate.size());
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
            log.error("runReceiveCorrelatorWorker : exception processing correlatorUpdate", e);
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
            log.info("runReceiveCorrelatorWorker : received update for a request we do not store {}",correlator);
            continue;
          }

          //
          //  log (debug)
          //

          if(log.isDebugEnabled()) log.debug("runReceiveCorrelatorWorker : {} , {} , {}", correlator, correlatorUpdate, deliveryRequest);

          //
          //  update
          //

          processCorrelatorUpdate(deliveryRequest, correlatorUpdate);

        }

      }
      if(correlatorUpdateRecords.count()>0){
        try{
          routingConsumer.commitSync();
        }catch (CommitFailedException ex){
          long lastPoll_ms=System.currentTimeMillis()-lastPollTime;
          log.warn("runReceiveCorrelatorWorker : CommitFailedException caught, last poll was {}ms ago",lastPoll_ms);
        }
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
    if(request instanceof INotificationRequest)
      {
        INotificationRequest notifRequest = (INotificationRequest) request;
        RESTAPIGenericReturnCodes returnCode = RESTAPIGenericReturnCodes.UNKNOWN;
        try
          {
            blockedByContactPolicy = contactPolicyProcessor.ensureContactPolicy(notifRequest);
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
            log.warn("processRequestBlockedByContactPolicy : Processing contact policy for " + request.getSubscriberID() + " failed", ex);
          }
        if (blockedByContactPolicy)
          {
            notifRequest.setReturnCode(returnCode.getGenericResponseCode());
            notifRequest.setReturnCodeDetails(returnCode.toString());
            completeRequest(request);
          }
      }
    return blockedByContactPolicy;
  }

  private void waitForAllInMemoryJobs(){
    synchronized (this){
      // no jobs, return
      if(submitRequestQueue.size()==0 && waitingForAcknowledgement.size()==0){
        return;
      }
      // to log if we wait too much, cause either there is a lack of thread, either the configured throughput is too big compare to the delivery processor capacity
      Long startTime = SystemTime.currentTimeMillis();
      Date warningBigWait = RLMDateUtils.addMilliseconds(SystemTime.getCurrentTime(),oneLoopConfiguredProcessTimeMs*5);
      boolean warned = false;
      while(true){
      	long timeToWait = warningBigWait.getTime()-SystemTime.getCurrentTime().getTime();
      	if(timeToWait<=0) timeToWait=500;
        try { wait(timeToWait); } catch (InterruptedException e) {}
        if(SystemTime.getCurrentTime().after(warningBigWait)){
          log.warn("waitForAllInMemoryJobs() : we are waiting since "+(SystemTime.currentTimeMillis()-startTime)+" ms to complete in memory jobs, check consistency of your thread/throughput configuration (still remaining "+submitRequestQueue.size()+", "+waitingForAcknowledgement.size()+")");
          warned=true;
        }
        if(submitRequestQueue.size()==0 && waitingForAcknowledgement.size()==0){
          if(warned) log.info("waitForAllInMemoryJobs() : all memory jobs done in "+(SystemTime.currentTimeMillis()-startTime)+" ms");
          return;
        }
      }

    }
  }

  private static class ShutdownHook implements NGLMRuntime.NGLMShutdownHook {

    private DeliveryManager deliveryManager;

    private ShutdownHook(DeliveryManager deliveryManager) {
      this.deliveryManager = deliveryManager;
    }

    @Override public void shutdown(boolean normalShutdown) { deliveryManager.shutdownDeliveryManager(normalShutdown); }
  }

  private void shutdownDeliveryManager(boolean normalShutdown){

	log.info("DeliveryManager.shutdownDeliveryManager("+normalShutdown+") : stopping "+applicationID);

	synchronized (this){
		managerStatus = ManagerStatus.Stopping;
	}

	// first stop all new requests
	log.info("DeliveryManager.shutdownDeliveryManager() : stopping submitRequestWorkerThread");
	if(requestConsumer!=null){
      while(true){
        try {
          submitRequestWorkerThread.join();
          break;
        } catch (InterruptedException e) {}
      }
      requestConsumer.close();
    }
    log.info("DeliveryManager.shutdownDeliveryManager() : submitRequestWorkerThread stopped");

	// now wait for in memory jobs to complete
    log.info("DeliveryManager.shutdownDeliveryManager() : will wait all in memory jobs to complete");
    waitForAllInMemoryJobs();
	if(submitRequestQueue.size()>0 || waitingForAcknowledgement.size()>0){
	  log.warn("DeliveryManager.shutdownDeliveryManager() : there is a bug here, please report it, still {} pending jobs ({})", submitRequestQueue.size(), waitingForAcknowledgement.size());
    }else{
      log.info("DeliveryManager.shutdownDeliveryManager() : all in memory request done");
    }

	// stopping now receive correlator worker thread
    log.info("DeliveryManager.shutdownDeliveryManager() : stopping receiveCorrelatorUpdateWorkerThread");
    if(routingConsumer!=null){
      while(true){
        try {
          receiveCorrelatorUpdateWorkerThread.join();
          break;
        } catch (InterruptedException e) { e.printStackTrace(); }
      }
      routingConsumer.close();
    }
    log.info("DeliveryManager.shutdownDeliveryManager() : receiveCorrelatorUpdateWorkerThread stopped");

    shutdown(); //sub classes implementation
    kafkaProducer.flush(); // flush what we got at least now (sub classes might still have few to send if shutdown call not perfectly handled)
    // a dirty hack probably mainly for CommodityDeliveryManager and PurchaseFulfillmentManager to avoid too much complexity there (not perfectly handled shutdown call there...)
    try { Thread.sleep(4000); } catch (InterruptedException e) { log.warn("DeliveryManager.shutdownDeliveryManager() : issue while peacefully sleeping",e); }
    kafkaProducer.close();// finally close it

	log.info("DeliveryManager.shutdownDeliveryManager() : stopped "+applicationID);
  }
}
