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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class SubscriberProfileService
{
  /*****************************************
  *
  *  enum
  *
  *****************************************/

  //
  //  CompressionType
  //

  public enum CompressionType
  {
    None("none", 0),
    GZip("gzip", 1),
    Unknown("(unknown)", 99);
    private String stringRepresentation;
    private int externalRepresentation;
    private CompressionType(String stringRepresentation, int externalRepresentation) { this.stringRepresentation = stringRepresentation; this.externalRepresentation = externalRepresentation; }
    public String getStringRepresentation() { return stringRepresentation; }
    public int getExternalRepresentation() { return externalRepresentation; }
    public static CompressionType fromStringRepresentation(String stringRepresentation) { for (CompressionType enumeratedValue : CompressionType.values()) { if (enumeratedValue.getStringRepresentation().equalsIgnoreCase(stringRepresentation)) return enumeratedValue; } return Unknown; }
    public static CompressionType fromExternalRepresentation(int externalRepresentation) { for (CompressionType enumeratedValue : CompressionType.values()) { if (enumeratedValue.getExternalRepresentation() ==externalRepresentation) return enumeratedValue; } return Unknown; }
  }

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
  *  constants
  *
  *****************************************/

  public static final byte SubscriberProfileCompressionEpoch = 0;

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
  private ConnectSerde<SubscriberProfile> subscriberProfileSerde = SubscriberProfile.getSubscriberProfileSerde();

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
            subscriberUpdateListener.evolutionSubscriberStatusUpdated(subscriberUpdate);
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
    
    List<byte[]> rawSubscriberProfiles = null;
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

        rawSubscriberProfiles = jedis.mget(binarySubscriberIDs.toArray(new byte[0][0]));
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

    //
    //  uncompress
    //

    List<byte[]> encodedSubscriberProfiles = new ArrayList<byte[]>();
    for (byte [] rawProfile : rawSubscriberProfiles)
      {
        if (rawProfile != null)
          {
            byte [] profile = uncompressSubscriberProfile(rawProfile, Deployment.getSubscriberProfileCompressionType());
            encodedSubscriberProfiles.add(profile);
          }
        else
          {
            encodedSubscriberProfiles.add(null);
          }
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
        if (encodedSubscriberProfile != null && encodedSubscriberProfile.length > 0)
          subscriberProfile = subscriberProfileSerde.deserializer().deserialize(Deployment.getSubscriberProfileRegistrySubject(), encodedSubscriberProfile);
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
    public void evolutionSubscriberStatusUpdated(SubscriberProfile subscriberProfile);
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
    //  instantiate subscriberGroupEpochReader
    //
    
    ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader = ReferenceDataReader.<String,SubscriberGroupEpoch>startReader("example", "example-subscriberGroupReader-001", Deployment.getBrokerServers(), Deployment.getSubscriberGroupEpochTopic(), SubscriberGroupEpoch::unpack);

    //
    //  instantiate subscriber profile service
    //

    SubscriberUpdateListener subscriberUpdateListener = new SubscriberUpdateListener()
    {
      @Override public void evolutionSubscriberStatusUpdated(SubscriberProfile subscriberUpdate) { System.out.println("subscriberUpdate: " + subscriberUpdate.getSubscriberID() + ", " + subscriberUpdate.getEvolutionSubscriberStatus()); }
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

                    System.out.println(subscriberProfile.getSubscriberID() + ": " + subscriberProfile.toString(subscriberGroupEpochReader));

                    //
                    //  active offers
                    //

                    for (Offer offer : offerService.getActiveOffers(now))
                      {
                        boolean qualifiedOffer = offer.evaluateProfileCriteria(evaluationRequest);
                        System.out.println("  offer: " + offer.getOfferID() + " " + (qualifiedOffer ? "true" : "false"));
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

  /***********************************************************************
  *
  *  compression support for profile
  *
  ***********************************************************************/

  /*****************************************
  *
  *  compressSubscriberProfile
  *
  *****************************************/

  public static byte [] compressSubscriberProfile(byte [] data, CompressionType compressionType)
  {
    //
    //  sanity
    //

    if (SubscriberProfileService.SubscriberProfileCompressionEpoch != 0) throw new ServerRuntimeException("unsupported compression epoch");

    //
    //  compress (if indicated)
    //
    
    byte[] payload;
    switch (compressionType)
      {
        case None:
          payload = data;
          break;
        case GZip:
          payload = compress_gzip(data);
          break;
        default:
          throw new RuntimeException("unsupported compression type");
      }

    //
    //  prepare result
    //

    byte[] result = new byte[payload.length+1];

    //
    //  compression epoch
    //

    result[0] = SubscriberProfileCompressionEpoch;

    //
    //  payload
    //

    System.arraycopy(payload, 0, result, 1, payload.length);

    //
    //  return
    //

    return result;
  }

  /*****************************************
  *
  *  uncompressSubscriberProfile
  *
  *****************************************/

  public static byte [] uncompressSubscriberProfile(byte [] compressedData, CompressionType compressionType)
  {
    /****************************************
    *
    *  check epoch
    *
    ****************************************/
    
    int epoch = compressedData[0];
    if (epoch != 0) throw new ServerRuntimeException("unsupported compression epoch");

    /****************************************
    *
    *  uncompress according to provided algorithm
    *
    ****************************************/

    //
    // extract payload
    // 

    byte [] rawPayload = new byte[compressedData.length-1];
    System.arraycopy(compressedData, 1, rawPayload, 0, compressedData.length-1);

    //
    //  uncompress
    //
    
    byte [] payload;
    switch (compressionType)
      {
        case None:
          payload = rawPayload;
          break;
        case GZip:
          payload = uncompress_gzip(rawPayload);
          break;
        default:
          throw new RuntimeException("unsupported compression type");
      }

    //
    //  return
    //

    return payload;
  }

  /*****************************************
  *
  *  compress_gzip
  *
  *****************************************/

  public static byte[] compress_gzip(byte [] data)
  {
    int len = data.length;
    byte [] compressedData;
    try
      {
        //
        //  length (to make the uncompress easier)
        //

        ByteArrayOutputStream bos = new ByteArrayOutputStream(4 + len);
        byte[] lengthBytes = integerToBytes(len);
        bos.write(lengthBytes);

        //
        //  payload
        //

        GZIPOutputStream gzip = new GZIPOutputStream(bos);
        gzip.write(data);

        //
        // result
        //

        gzip.close();
        compressedData = bos.toByteArray();
        bos.close();
      }
    catch (IOException ioe)
      {
        throw new ServerRuntimeException("compress", ioe);
      }

    return compressedData;
  }

  /*****************************************
  *
  *  uncompress_gzip
  *
  *****************************************/

  public static byte[] uncompress_gzip(byte [] compressedData)
  {
    //
    // sanity
    //
    
    if (compressedData == null) return null;

    //
    //  uncompress
    //
    
    byte [] uncompressedData;
    try
      {
        ByteArrayInputStream bis = new ByteArrayInputStream(compressedData);

        //
        //  extract length
        //

        byte [] lengthBytes = new byte[4];
        bis.read(lengthBytes, 0, 4);
        int dataLength = bytesToInteger(lengthBytes);

        //
        //  extract payload
        //

        uncompressedData = new byte[dataLength];
        GZIPInputStream gis = new GZIPInputStream(bis);
        int bytesRead = 0;
        int pos = 0;
        while (pos < dataLength)
          {
            bytesRead = gis.read(uncompressedData, pos, dataLength-pos);
            pos = pos + bytesRead;                
          }

        //
        //  close
        //
        
        gis.close();
        bis.close();
      }
    catch (IOException ioe)
      {
        throw new ServerRuntimeException("uncompress", ioe);
      }

    return uncompressedData;
  }

  /*****************************************
  *
  *  integerToBytes
  *
  *****************************************/

  static byte[] integerToBytes(int value)
  {
    byte [] result = new byte[4];
    result[0] = (byte) (value >> 24);
    result[1] = (byte) (value >> 16);
    result[2] = (byte) (value >> 8);
    result[3] = (byte) (value);
    return result;
  }

  /*****************************************
  *
  *  bytesToInteger
  *
  *****************************************/

  static int bytesToInteger(byte[] data)
  {
    return ((0x000000FF & data[0]) << 24) + ((0x000000FF & data[0]) << 16) + ((0x000000FF & data[2]) << 8) + ((0x000000FF) & data[3]);
  }
}
