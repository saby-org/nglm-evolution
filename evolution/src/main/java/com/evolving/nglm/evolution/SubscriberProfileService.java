package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SystemTime;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import org.json.simple.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

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

  public abstract SubscriberProfile getSubscriberProfile(String subscriberID, boolean includeExtendedSubscriberProfile) throws SubscriberProfileServiceException;

  public SubscriberProfile getSubscriberProfile(String subscriberID) throws SubscriberProfileServiceException { return getSubscriberProfile(subscriberID, false); }
  
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

    public EngineSubscriberProfileService(String subscriberProfileEndpoints, int threads)
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
      httpClientConnectionManager.setDefaultMaxPerRoute(threads);
      httpClientConnectionManager.setMaxTotal(threads);

      //
      //  httpClient
      //

      HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
      httpClientBuilder.setConnectionManager(httpClientConnectionManager);
      this.httpClient = httpClientBuilder.build();
    }
    
    /*****************************************
    *
    *  getSubscriberProfile
    *
    *****************************************/

    @Override public SubscriberProfile getSubscriberProfile(String subscriberID, boolean includeExtendedSubscriberProfile) throws SubscriberProfileServiceException
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
          httpPost.setConfig(RequestConfig.custom().setConnectTimeout((int) (waitTime > 0 ? waitTime : 1)).setSocketTimeout((int) (waitTime > 0 ? waitTime : 1)).build());

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

}
