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
import java.util.concurrent.atomic.AtomicBoolean;

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

  public SubscriberIDService(String redisSentinels, String name){
    this(redisSentinels,name,false);
  }

  public SubscriberIDService(String redisSentinels, String name, boolean dynamicPool)
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
    if(dynamicPool){
      jedisPoolConfig.setMaxTotal(1000);
      jedisPoolConfig.setMaxIdle(1000);
      jedisPoolConfig.setMinIdle(50);
    }
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
  
  private Map<String/*alternateID*/,Pair<String/*subscriberID*/,Integer/*tenantID*/>> getSubscriberIDs(String alternateIDName, Set<String> alternateIDs) throws SubscriberIDServiceException
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

    Map<String,Pair<String,Integer>> result = new HashMap<>();
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
                log.warn("Different tenantID for " + subscriberID);
              }
            result.put(alternateID, new Pair<>(subscriberID,tenantID));
          }
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
    Pair<String,Integer> subscriberID = getSubscriberIDAndTenantID(alternateIDName,alternateID);
    if(subscriberID==null) return null;
    return subscriberID.getFirstElement();
  }
    
  /**
   * return null if subscriber ID is null, return tenantID 1 if tenantID is null
   * @param alternateIDName
   * @param alternateID
   * @return
   * @throws SubscriberIDServiceException
   */
  public Pair<String, Integer> getSubscriberIDAndTenantID(String alternateIDName, String alternateID) throws SubscriberIDServiceException {
    Map<String,Pair<String,Integer>> subscriberIDs = getSubscriberIDs(alternateIDName, Collections.singleton(alternateID));
    Pair<String,Integer> subscriberID = subscriberIDs.get(alternateID);
    if(subscriberID==null||subscriberID.getFirstElement()==null) return null;//no mapping entry return null
    if(subscriberID.getSecondElement()==null) return new Pair<>(subscriberID.getFirstElement(),1);//no tenantID stored, default to 1 (migration to "multi-tenancy" case)
    return subscriberID;
  }

  // same but blocking call if redis issue
  public Pair<String, Integer> getSubscriberIDAndTenantIDBlocking(String alternateIDName, String alternateID, AtomicBoolean stopRequested) throws SubscriberIDServiceException, InterruptedException
  {
    while(!stopRequested.get())
    {
      try
      {
        return getSubscriberIDAndTenantID(alternateIDName,alternateID);
      }
      catch (SubscriberIDServiceException e)
      {
        if(!(e.getCause() instanceof JedisException)) throw e;
        log.warn("JEDIS error, will retry",e);
        try { Thread.sleep(1000); } catch (InterruptedException e1) { }
      }
    }
    throw new InterruptedException("stopped requested");
  }
  // only subsID
  public String getSubscriberIDBlocking(String alternateIDName, String alternateID, AtomicBoolean stopRequested) throws SubscriberIDServiceException, InterruptedException
  {
    Pair<String,Integer> subscriberIDAndTenantID = getSubscriberIDAndTenantIDBlocking(alternateIDName,alternateID,stopRequested);
    if(subscriberIDAndTenantID==null) return null;
    return subscriberIDAndTenantID.getFirstElement();
  }
  // creating stopRequested on JVM shutdown call
  public String getSubscriberIDBlocking(String alternateIDName, String alternateID) throws SubscriberIDServiceException
  {
    try{
      return getSubscriberIDBlocking(alternateIDName,alternateID,ShutdownHookHolderForBlockingCall.stopRequested);
    }catch (InterruptedException e){
      throw new SubscriberIDServiceException(e);
    }
  }
  // singleton "stopRequested" lazy init
  private static class ShutdownHookHolderForBlockingCall {
    private static AtomicBoolean stopRequested = new AtomicBoolean();
    static {
      NGLMRuntime.addShutdownHook((normalShutdown -> stopRequested.set(true)));
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

}
