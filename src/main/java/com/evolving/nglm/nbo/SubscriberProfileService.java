/****************************************************************************
*
*  SubscriberProfileService.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.core.ServerException;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.StringKey;

import com.rii.utilities.SystemTime;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonDeserializer;

import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.exceptions.JedisException;

import com.fasterxml.jackson.databind.JsonNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.PatternSyntaxException;

public class SubscriberProfileService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(SubscriberProfileService.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  //
  //  redis instance
  //

  private static final String redisInstance = "subscriberprofile";
  private static final int redisIndex = 0;

  //
  //  data
  //

  private String bootstrapServers;
  private SubscriberUpdateListener subscriberUpdateListener;
  private BlockingQueue<SubscriberProfile> listenerQueue = new LinkedBlockingQueue<SubscriberProfile>();
  private JedisSentinelPool jedisSentinelPool;
  private BinaryJedis jedis;
  private KafkaConsumer<byte[], byte[]> consumer;

  //
  //  serdes
  //
  
  private ConnectSerde<StringKey> stringKeySerde = StringKey.serde();
  private ConnectSerde<SubscriberProfile> subscriberProfileSerde = SubscriberProfile.serde();

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public SubscriberProfileService(String bootstrapServers, String groupID, String subscriberUpdateTopic, String redisSentinels, SubscriberUpdateListener subscriberUpdateListener)
  {
    /*****************************************
    *
    *  configuration
    *
    *****************************************/

    this.bootstrapServers = bootstrapServers;
    this.subscriberUpdateListener = subscriberUpdateListener;

    /*****************************************
    *
    *  redis
    *
    *****************************************/

    //
    //  instantiate
    //  

    Set<String> sentinels = new HashSet<String>(Arrays.asList(redisSentinels.split("\\s*,\\s*")));
    this.jedisSentinelPool = new JedisSentinelPool(redisInstance, sentinels);
    this.jedis = null;
    
    /*****************************************
    *
    *  kafka
    *
    *****************************************/

    //
    //  set up consumer
    //

    Properties consumerProperties = new Properties();
    consumerProperties.put("bootstrap.servers", bootstrapServers);
    consumerProperties.put("group.id", groupID);
    consumerProperties.put("auto.offset.reset", "earliest");
    consumerProperties.put("enable.auto.commit", "false");
    consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    this.consumer = new KafkaConsumer<>(consumerProperties);

    //
    //  subscribe to the subscriberUpdate topic
    //

    this.consumer.subscribe(Arrays.asList(subscriberUpdateTopic));
  }

  //
  //  constructor
  //

  public SubscriberProfileService(String bootstrapServers, String groupID, String subscriberUpdateTopic, String redisServer)
  {
    this(bootstrapServers, groupID, subscriberUpdateTopic, redisServer, (SubscriberUpdateListener) null);
  }

  /*****************************************
  *
  *  start
  *
  *****************************************/

  public void start()
  {
    /*****************************************
    *
    *  listener
    *
    *****************************************/

    if (subscriberUpdateListener != null)
      {
        Runnable listener = new Runnable() { @Override public void run() { runListener(); } };
        Thread listenerThread = new Thread(listener, "SubscriberUpdateListener");
        listenerThread.start();
      }

    /*****************************************
    *
    *  consumer
    *
    *****************************************/

    Runnable subscriberUpdateReader = new Runnable() { @Override public void run() { readSubscriberUpdates(consumer); } };
    Thread subscriberUpdateReaderThread = new Thread(subscriberUpdateReader, "SubscriberUpdateReader");
    subscriberUpdateReaderThread.start();
  }

  /*****************************************
  *
  *  readSubscriberUpdates
  *
  *****************************************/

  private void readSubscriberUpdates(KafkaConsumer<byte[], byte[]> consumer)
  {
    boolean readInitialTopicRecords = false;
    boolean consumedAllAvailable = false;
    Map<TopicPartition,Long> consumedOffsets = new HashMap<TopicPartition,Long>();
    do
      {
        //
        // poll
        //

        ConsumerRecords<byte[], byte[]> subscriberUpdateRecords = consumer.poll(5000);

        //
        //  process
        //

        for (ConsumerRecord<byte[], byte[]> subscriberUpdateRecord : subscriberUpdateRecords)
          {
            //
            //  parse
            //

            String subscriberID =  stringKeySerde.deserializer().deserialize(subscriberUpdateRecord.topic(), subscriberUpdateRecord.key()).getKey();
            SubscriberProfile subscriberUpdate = subscriberProfileSerde.deserializer().deserialize(subscriberUpdateRecord.topic(), subscriberUpdateRecord.value());
            if (subscriberUpdateListener != null) listenerQueue.add(subscriberUpdate);
            
            //
            //  offsets
            //

            consumedOffsets.put(new TopicPartition(subscriberUpdateRecord.topic(), subscriberUpdateRecord.partition()), subscriberUpdateRecord.offset());
          }

        //
        //  commit
        //

        consumer.commitSync();

        //
        //  consumed all available?
        //

        Set<TopicPartition> assignedPartitions = consumer.assignment();
        Map<TopicPartition,Long> availableOffsets = consumer.endOffsets(assignedPartitions);
        consumedAllAvailable = true;
        for (TopicPartition partition : availableOffsets.keySet())
          {
            Long availableOffsetForPartition = availableOffsets.get(partition);
            Long consumedOffsetForPartition = (consumedOffsets.get(partition) != null) ? consumedOffsets.get(partition) : -1L;
            if (consumedOffsetForPartition < availableOffsetForPartition-1)
              {
                consumedAllAvailable = false;
                break;
              }
          }
      }
    while (! consumedAllAvailable || ! readInitialTopicRecords);
  }

  /*****************************************
  *
  *  runListener
  *
  *****************************************/

  private void runListener()
  {
    while (true)
      {
        try
          {
            SubscriberProfile subscriberUpdate = listenerQueue.take();
            if (subscriberUpdate.getSubscriberStatusUpdated())
              {
                subscriberUpdateListener.subscriberStatusUpdated(subscriberUpdate);
              }
          }
        catch (InterruptedException e)
          {
            //
            // ignore
            //
          }
        catch (Throwable e)
          {
            StringWriter stackTraceWriter = new StringWriter();
            e.printStackTrace(new PrintWriter(stackTraceWriter, true));
            log.warn("Exception processing listener: {}", stackTraceWriter.toString());
          }
      }
  }

  /*****************************************
  *
  *  getSubscriberProfiles
  *
  *****************************************/

  public Map<String,SubscriberProfile> getSubscriberProfiles(Set<String> subscriberIDs) throws SubscriberProfileServiceException
  {
    /****************************************
    *
    *  binary keys
    *
    ****************************************/

    List<byte[]> binarySubscriberIDs = new ArrayList<byte[]>();
    for (String subscriberID : subscriberIDs)
      {
        binarySubscriberIDs.add(subscriberID.getBytes(StandardCharsets.UTF_8));
      }

    /****************************************
    *
    *  retrieve from redis
    *
    ****************************************/
    
    List<byte[]> encodedSubscriberProfiles = null;
    try
      {
        //
        //  ensure jedis connection
        //

        if (jedis == null)
          {
            jedis = jedisSentinelPool.getResource();
            jedis.select(redisIndex);
          }

        //
        //  read
        //

        encodedSubscriberProfiles = jedis.mget(binarySubscriberIDs.toArray(new byte[0][0]));
      }
    catch (JedisException e)
      {
        //
        //  log
        //

        log.error("JEDIS error");
        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.error(stackTraceWriter.toString());

        //
        //  close
        //

        if (jedis != null)
          {
            jedis.close();
            jedis = null;
          }

        //
        //  abort
        //

        throw new SubscriberProfileServiceException(e);
      }

    /****************************************
    *
    *  instantiate result map with subscriber profiles
    *
    ****************************************/

    Map<String,SubscriberProfile> result = new HashMap<String,SubscriberProfile>();
    for (int i = 0; i < binarySubscriberIDs.size(); i++)
      {
        //
        //  subscriber information
        //
        
        String subscriberID = new String(binarySubscriberIDs.get(i), StandardCharsets.UTF_8);
        byte[] encodedSubscriberProfile = encodedSubscriberProfiles.get(i);

        //
        //  decode subscriber profile
        //
        
        SubscriberProfile subscriberProfile;
        if (encodedSubscriberProfile != null)
          subscriberProfile = SubscriberProfile.serde().deserializer().deserialize(Deployment.getSubscriberProfileChangeLogTopic(), encodedSubscriberProfile);
        else
          subscriberProfile = null;

        //
        //  add
        //
        
        result.put(subscriberID, subscriberProfile);
      }

    //
    //  return
    //

    return result;
  }

  /*****************************************
  *
  *  getSubscriberProfile
  *
  *****************************************/

  public SubscriberProfile getSubscriberProfile(String subscriberID) throws SubscriberProfileServiceException
  {
    Map<String,SubscriberProfile> result = getSubscriberProfiles(Collections.<String>singleton(subscriberID));
    return result.get(subscriberID);
  }
  
  /*****************************************
  *
  *  interface SubscriberUpdateListener
  *
  *****************************************/

  public interface SubscriberUpdateListener
  {
    public void subscriberStatusUpdated(SubscriberProfile subscriberProfile);
  }

  /*****************************************
  *
  *  class SubscriberProfileServiceException
  *
  *****************************************/

  public class SubscriberProfileServiceException extends Exception
  {
    public SubscriberProfileServiceException(Throwable t) { super(t); }
  }

  /*****************************************
  *
  *  example main
  *
  *****************************************/

  public static void main(String[] args) throws Exception
  {
    /*****************************************
    *
    *  setup
    *
    *****************************************/

    //
    //  NGLMRuntime
    //

    NGLMRuntime.initialize();

    //
    //  arguments
    //
    
    Set<String> subscriberIDs = new HashSet<String>();
    for (int i=0; i<args.length; i++)
      {
        subscriberIDs.add(args[i]);
      }

    //
    //  instantiate offer service
    //

    OfferService offerService = new OfferService(Deployment.getBrokerServers(), "example-offerservice-001", Deployment.getOfferTopic(), false);
    offerService.start();

    //
    //  instantiate scoring-strategy service
    //

    ScoringStrategyService scoringStrategyService = new ScoringStrategyService(Deployment.getBrokerServers(), "example-scoringstrategyservice-001", Deployment.getScoringStrategyTopic(), false);
    scoringStrategyService.start();

    //
    //  instantiate subscriberGroupEpochReader
    //
    
    ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader = ReferenceDataReader.<String,SubscriberGroupEpoch>startReader("example", "example-subscriberGroupReader-001", Deployment.getBrokerServers(), Deployment.getSubscriberGroupEpochTopic(), SubscriberGroupEpoch::unpack);

    //
    //  instantiate subscriber profile service
    //

    SubscriberUpdateListener subscriberUpdateListener = new SubscriberUpdateListener()
    {
      @Override public void subscriberStatusUpdated(SubscriberProfile subscriberUpdate) { System.out.println("subscriberUpdate: " + subscriberUpdate.getSubscriberID() + ", " + subscriberUpdate.getSubscriberStatus()); }
    };
    SubscriberProfileService subscriberProfileService = new SubscriberProfileService(Deployment.getBrokerServers(), "example-subscriberprofileservice-001", Deployment.getSubscriberUpdateTopic(), Deployment.getRedisSentinels(), subscriberUpdateListener);
    subscriberProfileService.start();
    
    /*****************************************
    *
    *  json converter (for toString)
    *
    *****************************************/

    JsonConverter converter = new JsonConverter();
    Map<String, Object> converterConfigs = new HashMap<String, Object>();
    converterConfigs.put("schemas.enable","false");
    converter.configure(converterConfigs, false);
    JsonDeserializer deserializer = new JsonDeserializer();
    deserializer.configure(Collections.<String, Object>emptyMap(), false);

    /*****************************************
    *
    *  main loop
    *
    *****************************************/

    while (true)
      {
        try
          {
            //
            //  retrieve subscribers
            //
            
            Map<String,SubscriberProfile> subscriberProfiles = subscriberProfileService.getSubscriberProfiles(subscriberIDs);

            //
            //  evaluate
            //


            Date now = SystemTime.getCurrentTime();
            for (String subscriberID : subscriberProfiles.keySet())
              {
                SubscriberProfile subscriberProfile = subscriberProfiles.get(subscriberID);
                if (subscriberProfile != null)
                  {
                    //
                    //  evaluationRequest
                    //

                    SubscriberEvaluationRequest evaluationRequest = new SubscriberEvaluationRequest(subscriberProfile, subscriberGroupEpochReader, now);

                    //
                    //  print subscriberProfile
                    //

                    System.out.println(subscriberProfile.getSubscriberID() + ": " + subscriberProfile.getMSISDN() + ", " + subscriberProfile.getContractID() + ", " + subscriberProfile.getAccountTypeID() + ", " + subscriberProfile.getRatePlan() + ", " + subscriberProfile.getActivationDate() + ", " + subscriberProfile.getSubscriberStatus() + ", " + subscriberProfile.getStatusChangeDate() + ", " + subscriberProfile.getPreviousSubscriberStatus() + ", " + subscriberProfile.getLastRechargeDate() + ", " + subscriberProfile.getRatePlanChangeDate() + ", " + subscriberProfile.getMainBalanceValue() + ", " + subscriberProfile.getLanguage() + ", " + subscriberProfile.getRegion() + ", " + subscriberProfile.getHistoryTotalChargePrevious14Days(now) + ", " + subscriberProfile.getHistoryRechargeCountPrevious14Days(now) + ", " + subscriberProfile.getHistoryRechargeChargePrevious14Days(now) + ", " + subscriberProfile.getHistoryMOCallChargePrevious14Days(now) + ", " + subscriberProfile.getHistoryMOCallCountPrevious14Days(now) + ", " + subscriberProfile.getHistoryMOCallDurationPrevious14Days(now) + ", " + subscriberProfile.getHistoryMTCallCountPrevious14Days(now) + ", " + subscriberProfile.getHistoryMTCallDurationPrevious14Days(now) + ", " + subscriberProfile.getHistoryMTCallIntCountPrevious14Days(now) + ", " + subscriberProfile.getHistoryMTCallIntDurationPrevious14Days(now) + ", " + subscriberProfile.getHistoryMOCallIntChargePrevious14Days(now) + ", " + subscriberProfile.getHistoryMOCallIntCountPrevious14Days(now) + ", " + subscriberProfile.getHistoryMOCallIntDurationPrevious14Days(now) + ", " + subscriberProfile.getHistoryMOSMSChargePrevious14Days(now) + ", " + subscriberProfile.getHistoryMOSMSCountPrevious14Days(now) + ", " + subscriberProfile.getHistoryDataVolumePrevious14Days(now) + ", " + subscriberProfile.getHistoryDataBundleChargePrevious14Days(now) + ", " + subscriberProfile.getControlGroup(subscriberGroupEpochReader)  + ", " + subscriberProfile.getUniversalControlGroup(subscriberGroupEpochReader));

                    //
                    //  active offers
                    //

                    for (Offer offer : offerService.getActiveOffers(now))
                      {
                        boolean qualifiedOffer = offer.evaluateProfileCriteria(evaluationRequest);
                        System.out.println("  offer: " + offer.getOfferID() + " " + (qualifiedOffer ? "true" : "false"));
                      }

                    //
                    //  active scoring strategies
                    //

                    for (ScoringStrategy scoringStrategy : scoringStrategyService.getActiveScoringStrategies(now))
                      {
                        ScoringGroup scoringGroup = scoringStrategy.evaluateScoringGroups(evaluationRequest);
                        JsonNode scoringGroupNode = deserializer.deserialize(null, converter.fromConnectData(null, ScoringGroup.schema(),  ScoringGroup.pack(scoringGroup)));
                        System.out.println("  scoringStrategy: " + scoringStrategy.getScoringStrategyID() + " " + scoringGroupNode.toString());
                      }

                    //
                    //  traceDetails
                    //

                    for (String traceDetail : evaluationRequest.getTraceDetails())
                      {
                        System.out.println("  trace: " + traceDetail);
                      }
                  }
                else
                  {
                    System.out.println(subscriberID + ": not found");
                  }
              }

            //
            //  sleep
            //
            
            System.out.println("sleeping 10 seconds ...");
            Thread.sleep(10*1000L);
          }
        catch (SubscriberProfileServiceException e)
          {
            StringWriter stackTraceWriter = new StringWriter();
            e.printStackTrace(new PrintWriter(stackTraceWriter, true));
            System.out.println(stackTraceWriter.toString());
            System.out.println("sleeping 1 second ...");
            Thread.sleep(1*1000L);
          }
      }
  }
}
