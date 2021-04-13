/****************************************************************************
*
*  SubscriberIDService.java
*
****************************************************************************/

package com.evolving.nglm.core;

import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisException;

import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SubscriberIDService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(SubscriberIDService.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  //
  //  redis instance
  //

  private static final String redisInstance = "subscriberids";
  private static final int redisIndex = 0;

  //
  //  data
  //

  private JedisSentinelPool jedisSentinelPool;

  //
  //  serdes
  //
  
  private ConnectSerde<StringKey> stringKeySerde = StringKey.serde();

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public SubscriberIDService(String redisSentinels, String name)
  {
    /*****************************************
    *
    *  redis
    *
    *****************************************/

    //
    //  instantiate
    //  

    Set<String> sentinels = new HashSet<String>(Arrays.asList(redisSentinels.split("\\s*,\\s*")));
    JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
    jedisPoolConfig.setTestOnBorrow(true);
    jedisPoolConfig.setTestOnReturn(true);
    if (name != null)
      {
        jedisPoolConfig.setJmxNamePrefix("SubscriberIDService");
        jedisPoolConfig.setJmxNameBase(name);
      }
    String password = System.getProperty("redis.password");
    if(password != null && !password.trim().equals("") && !password.trim().equals("none")) {
      log.info("SubscriberIDService() Use Redis Password " + password);
      this.jedisSentinelPool = new JedisSentinelPool(redisInstance, sentinels, jedisPoolConfig, Protocol.DEFAULT_TIMEOUT, Protocol.DEFAULT_TIMEOUT, null, password, 0, null,
          Protocol.DEFAULT_TIMEOUT, Protocol.DEFAULT_TIMEOUT, null, password, null);
    }
    else {
      log.info("SubscriberIDService() No Redis Password");
      this.jedisSentinelPool = new JedisSentinelPool(redisInstance, sentinels, jedisPoolConfig);
    }   
  }

  //
  //  legacy historical constructor
  //
    
  public SubscriberIDService(String redisSentinels) { this(redisSentinels, null); }
  
  /*****************************************
  *
  *  stop
  *
  *****************************************/

  public void stop()
  {
    if (jedisSentinelPool != null) try { jedisSentinelPool.close(); } catch (JedisException e) { }
  }

  //
  //  compatibility
  //
  
  public void close() { stop(); }

  /****************************************
  *
  *  getSubscriberID (list)
  *
  ****************************************/
  
  private Map<String,String> getSubscriberIDs(String alternateIDName, List<String> alternateIDs) throws SubscriberIDServiceException
  {
    /****************************************
    *
    *  redis cache index
    *
    ****************************************/

    if (Deployment.getAlternateIDs().get(alternateIDName) == null)
      {
        throw new SubscriberIDServiceException("unknown alternateID name '" + alternateIDName + "'");
      }
    
    /****************************************
    *
    *  use with sharedIDs not meaningful
    *
    ****************************************/

    if (Deployment.getAlternateIDs().get(alternateIDName).getSharedID())
      {
        throw new SubscriberIDServiceException("service not available for shared id '" + alternateIDName + "'");
      }
    
    /****************************************
    *
    *  binary keys
    *
    ****************************************/

    List<byte[]> binaryAlternateIDs = new ArrayList<byte[]>();
    for (String alternateID : alternateIDs)
      {
        binaryAlternateIDs.add(alternateID.getBytes(StandardCharsets.UTF_8));
      }

    /****************************************
    *
    *  retrieve from redis
    *
    ****************************************/
    
    BinaryJedis jedis = jedisSentinelPool.getResource();
    List<byte[]> binarySubscriberIDs = null;
    try
      {
        jedis.select(Deployment.getAlternateIDs().get(alternateIDName).getRedisCacheIndex());
        binarySubscriberIDs = jedis.mget(binaryAlternateIDs.toArray(new byte[0][0]));
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
        //  abort
        //

        throw new SubscriberIDServiceException(e);
      }
    finally
      {
        try { jedis.close(); } catch (JedisException e1) { }
      }

    /****************************************
    *
    *  instantiate result map with subscriber ids
    *
    ****************************************/

    Map<String,String> result = new HashMap<String,String>();
    Integer tenantID = null;
    for (int i = 0; i < binaryAlternateIDs.size(); i++)
      {
        String alternateID = new String(binaryAlternateIDs.get(i), StandardCharsets.UTF_8);
        String subscriberID = null;        
        if (binarySubscriberIDs.get(i) != null)
          {
            int sizeModulo = binarySubscriberIDs.get(i).length % 8;
            // after multitenancy, the length is 4 + 8* n before it was 2 + 8 * n
            int tmpTenantID = sizeModulo == 4 ? /*after mutlitenancy*/ Shorts.fromByteArray(Arrays.copyOfRange(binarySubscriberIDs.get(i), 0, 2)) : 1;
            short numberOfSubscriberIDs = sizeModulo == 4 ? /* after multitenancy */ Shorts.fromByteArray(Arrays.copyOfRange(binarySubscriberIDs.get(i), 2, 4)) : Shorts.fromByteArray(Arrays.copyOfRange(binarySubscriberIDs.get(i), 0, 2));
            if (numberOfSubscriberIDs > 1) throw new SubscriberIDServiceException("invariant violated - multiple subscriberIDs");
            subscriberID = (numberOfSubscriberIDs == 1) ? (sizeModulo == 4 ? Long.toString(Longs.fromByteArray(Arrays.copyOfRange(binarySubscriberIDs.get(i), 4, 12))) : Long.toString(Longs.fromByteArray(Arrays.copyOfRange(binarySubscriberIDs.get(i), 2, 10))) ) : null;
            if(tenantID == null)
              {
                tenantID = tmpTenantID;
              }
            else if(tenantID.intValue() != tmpTenantID)
              {
                log.warn("Dfferent tenantID for " + subscriberID);
              }            
          }
        result.put(alternateID, subscriberID);
        result.put("tenantID", ""+tenantID);
      }

    //
    //  return
    //

    return result;
  }

  /****************************************
  *
  *  getSubscriberID (singleton)
  *
  ****************************************/
  
  public String getSubscriberID(String alternateIDName, String alternateID) throws SubscriberIDServiceException
  {
    Map<String,String> subscriberIDs = getSubscriberIDs(alternateIDName, Collections.<String>singletonList(alternateID));
    return subscriberIDs.get(alternateID);
  }
    
  /**
   * return null if either subscriber ID or tenantID is null...
   * @param alternateIDName
   * @param alternateID
   * @return
   * @throws SubscriberIDServiceException
   */
  public Pair<String, Integer> getSubscriberIDAndTenantID(String alternateIDName, String alternateID) throws SubscriberIDServiceException {
    Map<String,String> subscriberIDs = getSubscriberIDs(alternateIDName, Collections.<String>singletonList(alternateID));
    String tenantIDString = subscriberIDs.get("tenantID");
    Integer tenantID = null;
    if(tenantIDString != null) 
      { 
        tenantID = Integer.parseInt(tenantIDString);
      }
    String subscriberID = subscriberIDs.get(alternateID);
    if(subscriberID != null && tenantID != null) 
      {
        return new Pair<String, Integer>(subscriberIDs.get(alternateID), tenantID);
      }
    else 
      {
        return null;
      }
  }
  

  // same but blocking call if redis issue
  public String getSubscriberIDBlocking(String alternateIDName, String alternateID) throws SubscriberIDServiceException
  {
    while(true)
    {
      try
      {
        return getSubscriberID(alternateIDName,alternateID);
      }
      catch (SubscriberIDServiceException e)
      {
        if(!(e.getCause() instanceof JedisException)) throw e;
        log.warn("JEDIS error, will retry",e);
        try { Thread.sleep(1000); } catch (InterruptedException e1) { }
      }
    }
  }

  /*****************************************
  *
  *  class SubscriberIDServiceException
  *
  *****************************************/

  public class SubscriberIDServiceException extends Exception
  {
    public SubscriberIDServiceException(String message) { super(message); }
    public SubscriberIDServiceException(Throwable t) { super(t); }
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

    String alternateIDName = args[0];
    List<String> alternateIDs = new ArrayList<String>();
    for (int i=1; i<args.length; i++)
      {
        alternateIDs.add(args[i]);
      }

    //
    //  instantiate subscriber id service
    //

    SubscriberIDService subscriberIDService = new SubscriberIDService(Deployment.getRedisSentinels(), "test-main");

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
            //  retrieve subscriber ids
            //
            
            Map<String,String> ids = subscriberIDService.getSubscriberIDs(alternateIDName, alternateIDs);

            //
            //  output
            //

            System.out.println("resolved identifiers from redis cache:");
            for (String alternateID : ids.keySet())
              {
                System.out.println("  " + alternateID + " - " + ids.get(alternateID));
              }

            //
            //  sleep
            //
            
            System.out.println("sleeping 10 seconds ...");
            Thread.sleep(10*1000L);
          }
        catch (SubscriberIDServiceException e)
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
