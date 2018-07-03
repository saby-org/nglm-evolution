/****************************************************************************
*
*  DeliveryManager.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.KStreamsUniqueKeyServer;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.StringKey;

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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public abstract class DeliveryManager
{
  /*****************************************
  *
  *  static
  *
  *****************************************/
  
  protected static KStreamsUniqueKeyServer uniqueKeyServer = new KStreamsUniqueKeyServer();

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
  //  DeliveryPriority
  //
  
  public enum DeliveryPriority
  {
    HighPriority,
    NormalPriority,
    LowPriority;
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
    public boolean isRunning() { return EnumSet.of(Delivering,SuspendRequested,Suspended,ResumeRequested).contains(this); }
    public boolean isSuspending() { return EnumSet.of(SuspendRequested).contains(this); }
    public boolean isSuspended() { return EnumSet.of(SuspendRequested,Suspended,ResumeRequested).contains(this); }
    public boolean isWaitingToDeliverRequests() { return EnumSet.of(Suspended).contains(this); }
    public boolean isDeliveringRequests() { return EnumSet.of(Delivering).contains(this); }
    public boolean isProcessingResponses() { return EnumSet.of(Delivering,SuspendRequested,Suspended,ResumeRequested,Stopping).contains(this); }
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
  private String bootstrapServers;
  private ConnectSerde<DeliveryRequest> requestSerde;
  private String requestTopic;
  private String responseTopic;
  private String internalTopic;
  private int deliveryRatePerMinute;
  private DeliveryGuarantee deliveryGuarantee;

  //
  //  data
  //

  private volatile ManagerStatus managerStatus = ManagerStatus.Created;
  private int millisecondsPerDelivery;
  private int maxOutstandingRequests;
  private Date lastSubmitDate = NGLMRuntime.BEGINNING_OF_TIME;
  private Map<String,DeliveryRequest> inProgress = new HashMap<String,DeliveryRequest>();
  private BlockingQueue<DeliveryRequest> requestQueue = new LinkedBlockingQueue<DeliveryRequest>();
  private BlockingQueue<DeliveryRequest> responseQueue = new LinkedBlockingQueue<DeliveryRequest>();
  private ConnectSerde<StringKey> stringKeySerde = StringKey.serde();
  private KafkaConsumer<byte[], byte[]> requestConsumer = null;
  private KafkaProducer<byte[], byte[]> kafkaProducer = null;
  private Thread requestWorkerThread = null;
  private Thread responseWorkerThread = null;
  
  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getRequestTopic() { return requestTopic; }
  public String getResponseTopic() { return responseTopic; }
  public String getInternalTopic() { return internalTopic; }
  public int getDeliveryRatePerMinute() { return deliveryRatePerMinute; }
  public DeliveryGuarantee getDeliveryGuarantee() { return deliveryGuarantee; }

  //
  //  isProcessing
  //

  protected boolean isProcessing() { return managerStatus.isProcessingResponses(); }

  //
  //  startDelivery
  //

  protected void startDelivery()
  {
    log.info("Starting DeliveryManager " + applicationID);
    synchronized (this)
      {
        managerStatus = ManagerStatus.ResumeRequested;
        this.notifyAll();
      }
  }

  //
  //  suspendDelivery
  //

  protected void suspendDelivery()
  {
    log.info("Suspending DeliveryManager " + applicationID);
    synchronized (this)
      {
        managerStatus = ManagerStatus.SuspendRequested;
        this.notifyAll();
      }
  }

  //
  //  nextRequest
  //

  protected DeliveryRequest nextRequest()
  {
    DeliveryRequest result = null;
    while (isProcessing() && result == null)
      {
        try
          {
            result = requestQueue.take();
          }
        catch (InterruptedException e)
          {
          }
      }
    return result;
  }

  //
  //  markProcessed
  //

  protected void markProcessed(DeliveryRequest deliveryRequest)
  {
    if (managerStatus.isProcessingResponses())
      {
        responseQueue.add(deliveryRequest);
      }
  }

  /*****************************************
  *
  *  abstract
  *
  *****************************************/

  //
  //  with default implementation
  //

  protected void shutdown() { }
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  protected DeliveryManager(String applicationID, String bootstrapServers, ConnectSerde<? extends DeliveryRequest> requestSerde, DeliveryManagerDeclaration deliveryManagerDeclaration)
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
    this.bootstrapServers = bootstrapServers;
    this.requestSerde = (ConnectSerde<DeliveryRequest>) requestSerde;
    this.requestTopic = deliveryManagerDeclaration.getRequestTopic();
    this.responseTopic = deliveryManagerDeclaration.getResponseTopic();
    this.internalTopic = deliveryManagerDeclaration.getInternalTopic();
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
    requestConsumerProperties.put("group.id", applicationID);
    requestConsumerProperties.put("auto.offset.reset", "earliest");
    requestConsumerProperties.put("enable.auto.commit", "false");
    requestConsumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    requestConsumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    requestConsumer = new KafkaConsumer<>(requestConsumerProperties);
    
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
    *  request worker
    *
    *****************************************/

    Runnable requestWorker = new Runnable() { @Override public void run() { runRequestWorker(); } };
    requestWorkerThread = new Thread(requestWorker, applicationID + "-RequestWorker");
    requestWorkerThread.start();
    
    /*****************************************
    *
    *  response worker
    *
    *****************************************/

    Runnable responseWorker = new Runnable() { @Override public void run() { runResponseWorker(); } };
    responseWorkerThread = new Thread(responseWorker, applicationID + "-ResponseWorker");
    responseWorkerThread.start();
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
        while (managerStatus.isProcessingResponses() && inProgress.size() > 0)
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
        if (inProgress.size() > 0) throw new RuntimeException("inProgress size: " + inProgress.size());
        if (requestQueue.size() > 0) throw new RuntimeException("requestQueue size: " + requestQueue.size());
        if (responseQueue.size() > 0) throw new RuntimeException("responseQueue size: " + responseQueue.size());
      }

    /*****************************************
    *
    *  read progress to populate inProgress
    *
    *****************************************/

    //
    // set up progressConsumer
    //

    Properties progressConsumerProperties = new Properties();
    progressConsumerProperties.put("bootstrap.servers", bootstrapServers);
    progressConsumerProperties.put("group.id", applicationID);
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

    Map<String,DeliveryRequest> restartRequests = new HashMap<String,DeliveryRequest>();
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
              {
                deliveryRequest.setDeliveryPartition(topicPartition.partition());
                restartRequests.put(deliveryRequestID, deliveryRequest);
              }
            else
              {
                restartRequests.remove(deliveryRequestID);
              }

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
    while (managerStatus.isDeliveringRequests() && ! consumedAllAvailable);

    //
    //  close the consumer
    //

    progressConsumer.close();

    /*****************************************
    *
    *  ensure delivering
    *
    *****************************************/

    if (! managerStatus.isDeliveringRequests()) return;

    /****************************************
    *
    *  restart
    *
    ****************************************/

    switch (deliveryGuarantee)
      {
        case AtLeastOnce:
          for (DeliveryRequest deliveryRequest : restartRequests.values())
            {
              submitDeliveryRequest(deliveryRequest, true, null);
            }
          break;

        case AtMostOnce:
          synchronized (this)
            {
              for (DeliveryRequest deliveryRequest : restartRequests.values())
                {
                  deliveryRequest.setDeliveryStatus(DeliveryStatus.Indeterminate);
                  deliveryRequest.setDeliveryDate(SystemTime.getCurrentTime());
                  inProgress.put(deliveryRequest.getDeliveryRequestID(), deliveryRequest);
                  responseQueue.add(deliveryRequest);
                }
            }
          break;
      }
  }
  
  /*****************************************
  *
  *  runRequestWorker
  *
  *****************************************/

  private void runRequestWorker()
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
            *  log
            *
            *****************************************/

            if (resetRequired)
              {
                log.info("Resetting DeliveryManager (requestWorker) " + applicationID);
              }

            /*****************************************
            *
            *  unsubscribe from requestTopic (if necessary)
            *
            *****************************************/

            if (resetRequired)
              {
                requestConsumer.unsubscribe();
              }

            /*****************************************
            *
            *  reset (if required)
            *
            *****************************************/
            
            if (resetRequired)
              {
                //
                //  synchronized
                //

                synchronized (this)
                  {
                    //
                    //  clear requestQueue
                    //

                    List<DeliveryRequest> unprocessedDeliveryRequests = new ArrayList<DeliveryRequest>();
                    requestQueue.drainTo(unprocessedDeliveryRequests);
                    for (DeliveryRequest unprocessedDeliveryRequest : unprocessedDeliveryRequests)
                      {
                        switch (deliveryGuarantee)
                          {
                            case AtMostOnce:
                              unprocessedDeliveryRequest.setDeliveryStatus(DeliveryStatus.Failed);
                              unprocessedDeliveryRequest.setDeliveryDate(SystemTime.getCurrentTime());
                              responseQueue.add(unprocessedDeliveryRequest);
                              break;
                          }
                      }

                    //
                    //  wait for responseQueue to drain
                    //

                    while (managerStatus.isProcessingResponses() && responseQueue.size() > 0)
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
                    //  clear responseQueue
                    //

                    responseQueue.clear();

                    //
                    //  clear inProgress
                    //

                    inProgress.clear();

                    //
                    //  mark suspended
                    //

                    switch (managerStatus)
                      {
                        case SuspendRequested:
                          managerStatus = ManagerStatus.Suspended;
                          break;
                      }
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
              @Override public void onPartitionsRevoked(Collection<TopicPartition> partitions) { drainOutstandingRequests(partitions); }
              @Override public void onPartitionsAssigned(Collection<TopicPartition> partitions) { restart(partitions); }
            };
            requestConsumer.subscribe(Arrays.asList(requestTopic), listener);
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

        if (inProgress.containsKey(deliveryRequest.getDeliveryRequestID()))
          {
            return;
          }
        
        //
        //  maxOutstandingRequests
        //
        
        while (managerStatus.isDeliveringRequests() && inProgress.size() >= maxOutstandingRequests)
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

        //
        //  inProgress
        //
        
        inProgress.put(deliveryRequest.getDeliveryRequestID(), deliveryRequest);
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

    /****************************************
    *
    *  write to progress topic
    *
    ****************************************/

    if (! restart)
      {
        kafkaProducer.send(new ProducerRecord<byte[], byte[]>(internalTopic, stringKeySerde.serializer().serialize(internalTopic, new StringKey(deliveryRequest.getDeliveryRequestID())), requestSerde.optionalSerializer().serialize(internalTopic, deliveryRequest)));
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
        requestQueue.add(deliveryRequest);
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
        responseQueue.add(deliveryRequest);
      }
  }
  
  /*****************************************
  *
  *  runResponseWorker
  *
  *****************************************/

  private void runResponseWorker()
  {
    while (managerStatus.isProcessingResponses())
      {
        /****************************************
        *
        *  get request to process
        *
        ****************************************/
        
        DeliveryRequest deliveryRequest = null;
        while (managerStatus.isProcessingResponses() && deliveryRequest == null)
          {
            try
              {
                deliveryRequest = responseQueue.take();
              }
            catch (InterruptedException e)
              {
              }
          }

        /*****************************************
        *
        *  ensure processing
        *
        *****************************************/

        if (! managerStatus.isProcessingResponses()) continue;

        /*****************************************
        *
        *  ensure inProgress
        *
        *****************************************/

        synchronized (this)
          {
            if (! inProgress.containsKey(deliveryRequest.getDeliveryRequestID())) continue;
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

        kafkaProducer.send(new ProducerRecord<byte[], byte[]>(responseTopic, stringKeySerde.serializer().serialize(responseTopic, new StringKey(deliveryRequest.getSubscriberID())), requestSerde.serializer().serialize(responseTopic, deliveryRequest)));

        /****************************************
        *
        *  update progress
        *
        ****************************************/

        kafkaProducer.send(new ProducerRecord<byte[], byte[]>(internalTopic, stringKeySerde.serializer().serialize(internalTopic, new StringKey(deliveryRequest.getDeliveryRequestID())), requestSerde.optionalSerializer().serialize(internalTopic, null)));
        
        /****************************************
        *
        *  update inProgress
        *
        ****************************************/

        synchronized (this)
          {
            inProgress.remove(deliveryRequest.getDeliveryRequestID());
            this.notifyAll();
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
        requestWorkerThread.interrupt();
        responseWorkerThread.interrupt();
        this.notifyAll();
      }

    /*****************************************
    *
    *  drainOutstandingRequests
    *
    *****************************************/

    switch (managerStatus)
      {
        case Delivering:
        case Stopping:
          drainOutstandingRequests(requestConsumer.assignment());
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
        requestWorkerThread.interrupt();
        responseWorkerThread.interrupt();
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
                requestWorkerThread.join();
                break;
              }
            catch (InterruptedException e)
              {
              }
          }
        requestConsumer.close();
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
                responseWorkerThread.join();
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

