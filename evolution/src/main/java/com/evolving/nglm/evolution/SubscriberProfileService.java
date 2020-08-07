/****************************************************************************
*
*  SubscriberProfileService.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.ServerException;
import com.evolving.nglm.core.StringKey;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.SubscriberProfile.CompressionType;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonDeserializer;

import org.json.simple.JSONObject;

import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.exceptions.JedisException;

import com.fasterxml.jackson.databind.JsonNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.PatternSyntaxException;

public abstract class SubscriberProfileService
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

  protected volatile boolean stopRequested = false;

  //
  //  serdes
  //
  
  protected ConnectSerde<SubscriberProfile> subscriberProfileSerde = SubscriberProfile.getSubscriberProfileSerde();
  
  /*****************************************
  *
  *  start/stop
  *
  *****************************************/

  public void start() {}
  public void stop() {}

  /*****************************************
  *
  *  getSubscriberProfile
  *
  *****************************************/

  //
  //  getSubscriberProfile (optionally w/ history)
  //

  public abstract SubscriberProfile getSubscriberProfile(String subscriberID, boolean includeExtendedSubscriberProfile, boolean includeHistory) throws SubscriberProfileServiceException;

  //
  //  getSubscriberProfile (no history)
  //

  public SubscriberProfile getSubscriberProfile(String subscriberID) throws SubscriberProfileServiceException { return getSubscriberProfile(subscriberID, false, false); }
  
  /*****************************************
  *
  *  class SubscriberProfileServiceException
  *
  *****************************************/

  public class SubscriberProfileServiceException extends Exception
  {
    public SubscriberProfileServiceException(String message) { super(message); }
    public SubscriberProfileServiceException(Throwable t) { super(t); }
  }

  /*****************************************
  *
  *  class EngineSubscriberProfileService
  *
  *****************************************/

  public static class EngineSubscriberProfileService extends SubscriberProfileService
  {
    /*****************************************
    *
    *  data
    *
    *****************************************/

    private HttpClient httpClient;
    private List<String> subscriberProfileEndpoints;

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public EngineSubscriberProfileService(String subscriberProfileEndpoints)
    {
      /*****************************************
      *
      *  subscriberProfileEndpoints
      *
      *****************************************/

      this.subscriberProfileEndpoints = new LinkedList<String>(Arrays.asList(subscriberProfileEndpoints.split("\\s*,\\s*")));

      /*****************************************
      *
      *  http
      *
      *****************************************/

      //
      //  default connections
      //

      PoolingHttpClientConnectionManager httpClientConnectionManager = new PoolingHttpClientConnectionManager();
      httpClientConnectionManager.setDefaultMaxPerRoute(Deployment.getEvolutionEngineStreamThreads());
      httpClientConnectionManager.setMaxTotal(Deployment.getEvolutionEngineInstanceNumbers()*Deployment.getEvolutionEngineStreamThreads());

      //
      //  httpClient
      //

      HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
      httpClientBuilder.setConnectionManager(httpClientConnectionManager);
      this.httpClient = httpClientBuilder.build();
    }

    //
    //  legacy historical constructor
    //
    
    public EngineSubscriberProfileService(String bootstrapServers, String groupID, String subscriberUpdateTopic, String subscriberProfileEndpoints)
    {
      this(subscriberProfileEndpoints);
    }
    
    /*****************************************
    *
    *  getSubscriberProfile
    *
    *****************************************/

    @Override public SubscriberProfile getSubscriberProfile(String subscriberID, boolean includeExtendedSubscriberProfile, boolean includeHistory) throws SubscriberProfileServiceException
    {
      /****************************************
      *
      *  processing?
      *
      ****************************************/

      if (stopRequested) throw new SubscriberProfileServiceException("service closed");

      /*****************************************
      *
      *  timeout
      *
      *****************************************/

      Date now = SystemTime.getCurrentTime();
      Date timeout = RLMDateUtils.addSeconds(now, 10);

      /*****************************************
      *
      *  request
      *
      *****************************************/

      HashMap<String,Object> request = new HashMap<String,Object>();
      request.put("apiVersion", 1);
      request.put("subscriberID", subscriberID);
      request.put("includeExtendedSubscriberProfile", includeExtendedSubscriberProfile);
      request.put("includeHistory", includeHistory);
      JSONObject requestJSON = JSONUtilities.encodeObject(request);

      /*****************************************
      *
      *  shuffle endpoints
      *
      *****************************************/

      List<String> allEndpoints = new LinkedList<String>(subscriberProfileEndpoints);
      Collections.shuffle(allEndpoints, ThreadLocalRandom.current());
      Iterator<String> endpoints = allEndpoints.iterator();

      /*****************************************
      *
      *  attempt call to endpoint
      *
      *****************************************/

      HttpResponse httpResponse = null;
      while (httpResponse == null && endpoints.hasNext())
        {
          /*****************************************
          *
          *   httpPost
          *
          *****************************************/

          String endpoint = endpoints.next();
          long waitTime = timeout.getTime() - now.getTime();
          HttpPost httpPost = new HttpPost("http://" + endpoint + "/nglm-evolutionengine/getSubscriberProfile");
          httpPost.setEntity(new StringEntity(requestJSON.toString(), ContentType.create("application/json")));
          httpPost.setConfig(RequestConfig.custom().setConnectTimeout((int) (waitTime > 0 ? waitTime : 1)).build());

          /*****************************************
          *
          *  submit
          *
          *****************************************/

          httpResponse = null;
          try
            {
              if (waitTime > 0) httpResponse = httpClient.execute(httpPost);
            }
          catch (IOException e)
            {
              //
              //  log
              //

              StringWriter stackTraceWriter = new StringWriter();
              e.printStackTrace(new PrintWriter(stackTraceWriter, true));
              log.error("Exception processing REST api: {}", stackTraceWriter.toString());
            }

          //
          //  success?
          //

          if (httpResponse == null || httpResponse.getStatusLine() == null || httpResponse.getStatusLine().getStatusCode() != 200 || httpResponse.getEntity() == null)
            {
              log.info("retrieveSubscriberProfile failed: {}", httpResponse);
              httpResponse = null;
            }
        }

      //
      //  success?
      //

      if (httpResponse == null) throw new SubscriberProfileServiceException("getSubscriberProfile failed");
      
      /*****************************************
      *
      *  read response
      *
      *****************************************/

      int responseCode;
      byte[] responseBody;
      InputStream responseStream = null;
      try
        {
          //
          //  stream
          //

          responseStream = httpResponse.getEntity().getContent();

          //
          //  header
          //

          byte[] rawResponseHeader = new byte[6];
          int totalBytesRead = 0;
          while (totalBytesRead < 6)
            {
              int bytesRead = responseStream.read(rawResponseHeader, totalBytesRead, 6-totalBytesRead);
              if (bytesRead == -1) break;
              totalBytesRead += bytesRead;
            }
          if (totalBytesRead < 6) throw new SubscriberProfileServiceException("getSubscriberProfile failed (bad header)");

          //
          //  parse response header
          //

          ByteBuffer apiResponseHeader = ByteBuffer.allocate(6);
          apiResponseHeader.put(rawResponseHeader);
          int version = apiResponseHeader.get(0);
          responseCode = apiResponseHeader.get(1);
          int responseBodyLength = (responseCode == 0) ? apiResponseHeader.getInt(2) : 0;

          //
          //  body
          //

          responseBody = new byte[responseBodyLength];
          totalBytesRead = 0;
          while (totalBytesRead < responseBodyLength)
            {
              int bytesRead = responseStream.read(responseBody, totalBytesRead, responseBodyLength-totalBytesRead);
              if (bytesRead == -1) break;
              totalBytesRead += bytesRead;
            }
          if (totalBytesRead < responseBodyLength) throw new SubscriberProfileServiceException("getSubscriberProfile failed (bad body)");
        }
      catch (IOException e)
        {
          //
          //  log
          //

          StringWriter stackTraceWriter = new StringWriter();
          e.printStackTrace(new PrintWriter(stackTraceWriter, true));
          log.error("Exception processing REST api: {}", stackTraceWriter.toString());

          //
          //  error
          //

          responseCode = 2;
          responseBody = null;
        }
      finally
        {
          if (responseStream != null) try { responseStream.close(); } catch (IOException e) { }
        }
      
      /*****************************************
      *
      *  result
      *
      *****************************************/

      //
      //  cases
      //

      SubscriberProfile result = null;
      switch (responseCode)
        {
          case 0:
            result = SubscriberProfile.getSubscriberProfileSerde().deserializer().deserialize(Deployment.getSubscriberProfileRegistrySubject(), responseBody);
            break;

          case 1:
            result = null;
            break;

          default:
            throw new SubscriberProfileServiceException("retrieveSubscriberProfile response code: " + responseCode);
        }

      //
      //  return
      //

      if(log.isTraceEnabled()) log.trace("retrieveSubscriberProfile returning : {}",result);

      return result;
    }
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
    //  instantiate service
    //
    
    SubscriberProfileService subscriberProfileService = new EngineSubscriberProfileService(Deployment.getSubscriberProfileEndpoints());

    //
    //  start
    //

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
            Date now = SystemTime.getCurrentTime();
            for (String subscriberID : subscriberIDs)
              {
                SubscriberProfile subscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, true, true);
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
}
