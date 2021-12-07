package com.evolving.nglm.core;

import com.evolving.nglm.evolution.redis.UpdateAlternateIDSubscriberIDs;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;
import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisException;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SubscriberIDService implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(SubscriberIDService.class);
  private static final String redisInstance = "subscriberids";

  private JedisSentinelPool jedisSentinelPool;

  public SubscriberIDService(String redisSentinels) { this(redisSentinels, null); }
  public SubscriberIDService(String redisSentinels, String name){this(redisSentinels,name,false);}
  public SubscriberIDService(String redisSentinels, String name, boolean dynamicPool) {
    Set<String> sentinels = new HashSet<String>(Arrays.asList(redisSentinels.split("\\s*,\\s*")));
    JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
    if(dynamicPool){
      jedisPoolConfig.setMaxTotal(1000);
      jedisPoolConfig.setMaxIdle(1000);
      jedisPoolConfig.setMinIdle(50);
    }
    if (name != null) {
      jedisPoolConfig.setJmxNamePrefix("SubscriberIDService");
      jedisPoolConfig.setJmxNameBase(name);
    }
    String password = System.getProperty("redis.password");
    if(password != null && !password.trim().equals("") && !password.trim().equals("none")) {
      log.info("SubscriberIDService() Use Redis Password " + password);
      this.jedisSentinelPool = new JedisSentinelPool(redisInstance, sentinels, jedisPoolConfig, Protocol.DEFAULT_TIMEOUT, Protocol.DEFAULT_TIMEOUT, null, password, 0, null, Protocol.DEFAULT_TIMEOUT, Protocol.DEFAULT_TIMEOUT, null, password, null);
    } else {
      log.info("SubscriberIDService() No Redis Password");
      this.jedisSentinelPool = new JedisSentinelPool(redisInstance, sentinels, jedisPoolConfig);
    }   
  }

  public void stop() {if (jedisSentinelPool != null) try { jedisSentinelPool.close(); } catch (JedisException e) {}}
  @Override public void close() { stop(); }

  private Map<String/*alternateID*/,Pair<Integer/*tenantID*/,List<Long>/*subscriberIDs*/>> getSubscriberIDs(String alternateIDName, Set<String> alternateIDs) throws SubscriberIDServiceException {

    if (Deployment.getAlternateIDs().get(alternateIDName) == null) throw new SubscriberIDServiceException("unknown alternateID name '" + alternateIDName + "'");

    // prepare keys
    List<byte[]> binaryAlternateIDs = new ArrayList<>();
    for (String alternateID : alternateIDs) binaryAlternateIDs.add(alternateID.getBytes(StandardCharsets.UTF_8));

    // get from redis
    List<byte[]> binarySubscriberIDs = null;
    try (BinaryJedis jedis = jedisSentinelPool.getResource()) {
      jedis.select(Deployment.getAlternateIDs().get(alternateIDName).getRedisCacheIndex());
      binarySubscriberIDs = jedis.mget(binaryAlternateIDs.toArray(new byte[0][0]));
    } catch (JedisException e) {
        log.error("JEDIS error",e);
        throw new SubscriberIDServiceException(e);
    }

    // build result
    Map<String,Pair<Integer,List<Long>>> result = new HashMap<>();
    for (int i = 0; i < binaryAlternateIDs.size(); i++) {
      String alternateID = new String(binaryAlternateIDs.get(i), StandardCharsets.UTF_8);
      if (binarySubscriberIDs.get(i) != null) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(binarySubscriberIDs.get(i));

        // 2 cases how subscriberIDs are stored in redis :
        // - before multi tenancy : 2 bytes short nbOfSubscriberIDs | nbOfSubscriberIDs * 8 bytes long subscriberID
        // - after  multi tenancy : 2 bytes short tenantID | 2 bytes short nbOfSubscriberIDs | nbOfSubscriberIDs * 8 bytes long subscriberID

        boolean withTenant = binarySubscriberIDs.get(i).length % 8 == 4;
        Integer tenantID = 1;
        if(withTenant) tenantID = Short.valueOf(byteBuffer.getShort()).intValue();
        short numberOfSubscriberIDs = byteBuffer.getShort();
        List<Long> subscriberIDs = new ArrayList<>();
        for(int j=0;j<numberOfSubscriberIDs;j++) subscriberIDs.add(byteBuffer.getLong());
        result.put(alternateID,new Pair<>(tenantID,subscriberIDs));
      }
    }

    return result;
  }

  // if return false, means none of the entries have been pushed (this is redis msetnx "set if not exists" behavior)
  public boolean putAlternateIDs(AlternateID alternateID, Map<String/*alternateID*/, Pair<Integer/*tenantID*/,Set<Long/*subsriberIDs*/>>> subscribersIDs, boolean fullBatchFailureIfAtLeastOneAlreadyExists, AtomicBoolean stopRequested) throws SubscriberIDServiceException{

    // use with sharedIDs not meaningful
    if (alternateID.getSharedID()) throw new SubscriberIDServiceException("service not available for shared id '" + alternateID.getName() + "'");

    // constructing bytes request to push
    List<byte[]> binaryAlternateIDs = new ArrayList<byte[]>();
    for (Map.Entry<String,Pair<Integer,Set<Long>>> subscriberID : subscribersIDs.entrySet()) {
      String alternateIDValue = subscriberID.getKey();
      Short tenantID = subscriberID.getValue().getFirstElement().shortValue();
      Set<Long> subscriberIDs = subscriberID.getValue().getSecondElement();
      Short nbSubscriberIDs = Integer.valueOf(subscriberIDs.size()).shortValue();

      ByteBuffer byteBuffer = ByteBuffer.allocate( 2 + 2 + (8*nbSubscriberIDs) ); //2 bytes tenantID, 2 bytes nb of subscriberIDs, 8bytes per subscriberID
      byteBuffer.putShort(tenantID);
      byteBuffer.putShort(nbSubscriberIDs);
      for(Long subsID:subscriberIDs) byteBuffer.putLong(subsID);

      binaryAlternateIDs.add(alternateIDValue.getBytes(StandardCharsets.UTF_8));
      binaryAlternateIDs.add(byteBuffer.array());
    }
    byte[][] binarySubscriberIDsArray = binaryAlternateIDs.toArray(new byte[0][0]);

    while(!stopRequested.get()){
      try (BinaryJedis jedis = jedisSentinelPool.getResource()) {
        jedis.select(alternateID.getRedisCacheIndex());
        if(fullBatchFailureIfAtLeastOneAlreadyExists){
          return jedis.msetnx(binarySubscriberIDsArray)!=0;
        }else{
          jedis.mset(binarySubscriberIDsArray);
          return true;//mset never fail
        }
      }
      catch (JedisException e) {
        log.warn("putAlternateIDs redis issue",e);
      }
    }

    return false;

  }

  // value deserialize method
  private static Pair<Integer,Set<Long>> valueFromBytes(byte[] stored){
    if(stored==null) return null;
    boolean withTenant = stored.length % 8 == 4;
    ByteBuffer byteBuffer = ByteBuffer.wrap(stored);
    Integer tenantID = 1;
    if(withTenant) tenantID = Short.valueOf(byteBuffer.getShort()).intValue();
    Short nbSubscriberIDs = byteBuffer.getShort();
    Set<Long> subscriberIDs = new HashSet<>(nbSubscriberIDs);
    for(int i=0;i<nbSubscriberIDs;i++) subscriberIDs.add(byteBuffer.getLong());
    return new Pair<>(tenantID,subscriberIDs);
  }
  // value serialize method
  private static byte[] valueToBytes(Pair<Integer,Set<Long>> value){
    if(value==null || value.getSecondElement()==null || value.getSecondElement().isEmpty()) return null;// should return null if empty subscriberIDs for deleting well the entry
    ByteBuffer byteBuffer = ByteBuffer.allocate(2 + 2 + 8 * value.getSecondElement().size());
    byteBuffer.putShort(value.getFirstElement().shortValue());
    byteBuffer.putShort((short)value.getSecondElement().size());
    for(Long subscriberID:value.getSecondElement()) byteBuffer.putLong(subscriberID);
    return byteBuffer.array();
  }
  // when giving as arg to LUA script, we can not send null value, we use an empty array then
  private static byte[] valueToBytesForLUAArg(Pair<Integer,Set<Long>> value){
    byte[] toRet = valueToBytes(value);
    if(toRet==null) return new byte[0];
    return toRet;
  }


  /**
   * Redis is single threaded, hence running LUA scripts on Redis side can provide needed atomicity
   *
   * luaScriptSetIfEqualsBulk is meant to bulk update alternateID values
   * it does like key.compareAndSet(expectedValue, newValue)
   * if new value is null, key is deleted
   *
   * if all updates succeed, it returns null
   * otherwise, on the first update failing, it aborts and returns the key where it failed
   * (so all request previous to that key or OK, after that none update have been applied)
   */
  private static final byte[] luaScriptSetIfEqualsBulk = (
          "local i = 1;" +
          "local old = ARGV[i];" +
          "while KEYS[i] do" +
          "  if string.len(old) == 0 then old = false end" +//redis.call('GET', KEYS[i]) return value if existing, or false boolean if not existing
          "  if redis.call('GET', KEYS[i]) == old then" +
          "      if string.len(ARGV[i+1]) == 0 then" +
          "        redis.call('DEL', KEYS[i])" +
          "      else" +
          "        redis.call('SET', KEYS[i], ARGV[i+1])" +
          "      end" +
          "  else" +
          "    return KEYS[i]" +
          "  end" +
          "  i = i + 2;" +
          "end"
  ).trim().getBytes(StandardCharsets.UTF_8);

  // this create alternateID/subscriberIDs or update only to add/remove and subscriberID to shared alternateID or delete fully entry
  // this provide atomicty thanks to the LUA script above
  // note that that we first get all batch from redis at once, then check if job is needed, then update if needed
  // so if in the same batch, you send a delete + create for the same key/value which is already existing, it will delete it only (as create will assume there is no job to do as entry was already there before the batch)
  public List<UpdateAlternateIDSubscriberIDs> updateAlternateIDsAddOrRemoveSubscriberID(AlternateID alternateID, List<UpdateAlternateIDSubscriberIDs> requests, AtomicBoolean stopRequest){
    return updateAlternateIDsAddOrRemoveSubscriberID(0,alternateID,requests,stopRequest);
  }
  private List<UpdateAlternateIDSubscriberIDs> updateAlternateIDsAddOrRemoveSubscriberID(int recursiveNb, AlternateID alternateID, List<UpdateAlternateIDSubscriberIDs> updateRequests, AtomicBoolean stopRequest){

    List<UpdateAlternateIDSubscriberIDs> requests = new ArrayList<>(updateRequests);
    List<UpdateAlternateIDSubscriberIDs> stored = new ArrayList<>();

    if(updateRequests==null || updateRequests.isEmpty()) return stored;

    List<byte[]> keys = requests.stream().map(v->v.getAlternateIDValue().getBytes(StandardCharsets.UTF_8)).collect(Collectors.toList());

    while(!stopRequest.get()){
      try(BinaryJedis jedis = jedisSentinelPool.getResource()){
        jedis.select(alternateID.getRedisCacheIndex());
        // first we need what is already stored
        List<byte[]> existingValues = jedis.mget(keys.toArray(new byte[0][0]));

        // then we prepare the update request
        List<byte[]> updateKeys = new ArrayList<>();
        List<byte[]> updateArgs = new ArrayList<>();
        Iterator<UpdateAlternateIDSubscriberIDs> requestsIterator = requests.iterator();
        Iterator<byte[]> keysIterator = keys.listIterator();
        for(byte[] existingValue:existingValues){
          UpdateAlternateIDSubscriberIDs request = requestsIterator.next();
          byte[] binaryKey = keysIterator.next();

          String alternateIDValue = request.getAlternateIDValue();
          Integer requestTenantID = request.getTenantID();
          Long subscriberIDToADD = request.getSubscriberIDToAdd();
          Long subscriberIDToRemove = request.getSubscriberIDToRemove();

          Pair<Integer,Set<Long>> existing = valueFromBytes(existingValue);
          // value already there
          if(existingValue!=null){
            // tenantID mismatch, we warn and just do nothing
            if(!existing.getFirstElement().equals(requestTenantID)){
              log.warn("updateAlternateIDs "+alternateID.getDisplay()+" "+alternateIDValue+" mismatch of tenantID from old "+existing+" vs "+requestTenantID+", doing nothing");
              requestsIterator.remove();
              continue;
            }
            // not sharedID, and not delete request
            if(!alternateID.getSharedID() && subscriberIDToADD!=null){
              // if already stored, nothing to do, but return fine
              if(existing.getSecondElement().contains(subscriberIDToADD)){
                if(log.isDebugEnabled()) log.debug("updateAlternateIDs "+alternateID.getDisplay()+" "+alternateIDValue+" old "+existing+" vs new "+subscriberIDToADD+", already fine doing nothing");
                stored.add(request);
                requestsIterator.remove();
                continue;
              // else log mismatch and doing nothing
              }else{
                log.info("updateAlternateIDs "+alternateID.getDisplay()+" "+alternateIDValue+" can not update non sharedID already existing "+existing+" vs new "+subscriberIDToADD+", doing nothing");
                requestsIterator.remove();
                continue;
              }
            }
            // if delete request
            if(subscriberIDToRemove!=null){
              // nothing to do if none store, but return OK
              if(!existing.getSecondElement().contains(subscriberIDToRemove)){
                if(log.isDebugEnabled()) log.debug("updateAlternateIDs "+alternateID.getDisplay()+" "+alternateIDValue+" subscriberID to remove "+subscriberIDToRemove+" is not stored "+existing+", doing nothing");
                stored.add(request);
                requestsIterator.remove();
                continue;
              }
              // we remove the subscriberID from list
              existing.getSecondElement().remove(subscriberIDToRemove);
            }
            // if add request
            if(subscriberIDToADD!=null){
              // nothing to do if already stored, but return OK
              if(existing.getSecondElement().contains(subscriberIDToADD)){
                if(log.isDebugEnabled()) log.debug("updateAlternateIDs "+alternateID.getDisplay()+" "+alternateIDValue+" subscriberID to add "+subscriberIDToADD+" already stored "+existing+", doing nothing");
                stored.add(request);
                requestsIterator.remove();
                continue;
              }
              // this should be already checked, but to be safe
              if(!alternateID.getSharedID() && !existing.getSecondElement().isEmpty()){
                log.warn("updateAlternateIDs "+alternateID.getDisplay()+" "+alternateIDValue+" subscriberID to add "+subscriberIDToADD+" but not sharedID, already "+existing+" stored");
                requestsIterator.remove();
                continue;
              }
              // we add the subscriberID from list
              existing.getSecondElement().add(subscriberIDToADD);
            }

            updateKeys.add(binaryKey);
            updateArgs.add(existingValue);
            updateArgs.add(valueToBytesForLUAArg(existing));

          }else{
            // not existing and nothing to add, so nothing to do
            if(subscriberIDToADD==null){
              if(log.isDebugEnabled()) log.debug("updateAlternateIDs "+alternateID.getDisplay()+" "+alternateIDValue+" no subscriberID to add, nothing store, doing nothing");
              stored.add(request);
              requestsIterator.remove();
              continue;
            }

            updateKeys.add(binaryKey);
            updateArgs.add(new byte[0]);//can not sent null to redis as arg
            updateArgs.add(valueToBytesForLUAArg(new Pair<>(requestTenantID,Collections.singleton(subscriberIDToADD))));

          }
        }

        if(updateKeys.isEmpty()) return stored;// nothing to do
        // we run the request
        Object evalResult = jedis.eval(luaScriptSetIfEqualsBulk,updateKeys,updateArgs);
        // null response means all OK
        if(evalResult==null) return stored;

        // else, we had a concurrency update at one point, we got back the key of the failure, we need to re-execute from that point
        String failedAt = new String((byte[])evalResult);
        if(log.isDebugEnabled()) log.debug("updateAlternateIDs concurrency issue at "+failedAt+" after recursiveNb "+recursiveNb);
        requestsIterator = requests.iterator();
        while(requestsIterator.hasNext()){
          UpdateAlternateIDSubscriberIDs request = requestsIterator.next();
          if(!request.getAlternateIDValue().equals(failedAt)){
            requestsIterator.remove();
          }else{
            break;
          }
        }
        stored.addAll(updateAlternateIDsAddOrRemoveSubscriberID(recursiveNb+1,alternateID,requests,stopRequest));
        return stored;

      }catch (JedisException e) {
        log.warn("putAlternateIDs redis issue",e);
      }
    }

    return stored;

  }

  // same but blocking call if redis issue
  public Map<String/*alternateID*/,Pair<Integer/*tenantID*/,List<Long>/*subscriberIDs*/>> getSubscriberIDsBlocking(String alternateIDName, Set<String> alternateIDs, AtomicBoolean stopRequested) throws SubscriberIDServiceException, InterruptedException{
    while(!stopRequested.get()) {
      try {
        return getSubscriberIDs(alternateIDName,alternateIDs);
      } catch (SubscriberIDServiceException e) {
        if(!(e.getCause() instanceof JedisException)) throw e;
        log.warn("JEDIS error, will retry",e);
        try { Thread.sleep(1000); } catch (InterruptedException e1) { }
      }
    }
    throw new InterruptedException("stopped requested");
  }
  
  /****************************************
    {
      "apiVersion": 1,
      "straightAlternateIDName": "msisdn",
      "straightAlternateIDValue": "12125550120",
      "subscriberIDs": [
        {
          "reverseAlternateIDs": {
            "contractID": {
              "120": "121"
            },
            "msisdn": {
              "120": "12125550120"
            }
          },
          "subscriberID": "120"
        }
      ],
      
      "licenseCheck": {
        "raisedDate": 1632233423472,
        "level": 0,
        "context": "system",
        "source": "licensemanager",
        "detail": "Expires at Sun Jan 16 00:59:59 CET 2022",
        "type": "license_timelimit"
      }      
    }
  *
  ****************************************/
  
  public JSONObject getRedisSubscriberIDsForPTTTests(String alternateIDName, String alternateIDValue)
  {
    /****************************************
    *
    *  redis cache index
    *
    ****************************************/

    if (Deployment.getAlternateIDs().get(alternateIDName) == null)
      {
        JSONObject result = new JSONObject();
        result.put("error", "unknown alternateID name '" + alternateIDName + "'");
        return result;
      }
    
    /****************************************
    *
    *  use with sharedIDs not meaningful
    *
    ****************************************/

    if (Deployment.getAlternateIDs().get(alternateIDName).getSharedID())
      {
        JSONObject result = new JSONObject();
        result.put("error", "service not available for shared id '" + alternateIDName + "'");
        return result;
      }
    
    // retrieve Subscriber ID from alternate ID
    Map<String, String> subscriberIDsFromOneAlternateID =  getMappingValueAlternateIDToSubcriberIDRedisIndex(alternateIDName, alternateIDValue);
    
    if(subscriberIDsFromOneAlternateID != null)
      {
        JSONObject result = new JSONObject();
        JSONArray subscriberIDs = new JSONArray();
        result.put("straightAlternateIDName", alternateIDName);
        result.put("straightAlternateIDValue", alternateIDValue);
        result.put("subscriberIDs", subscriberIDs);
        
        for(Map.Entry<String, String> current : subscriberIDsFromOneAlternateID.entrySet()) // <msisdn>,<subscriberID>
          {           
            if("tenantID".equals(current.getKey())) { continue; }
            String foundSubscriberID = current.getValue();
            JSONObject alternateIDsMap = new JSONObject();
            //Pair<String, Map<String, Map<String, String>>> oneSubscriberID = new Pair<String, Map<String,Map<String,String>>>(foundSubscriberID, alternateIDsMap);
            for(String alternateID : Deployment.getAlternateIDs().keySet())
              {
                // <SubscriberID> => <alternateIDValue>
                Map<String, String> alternateIdsFromOneSubscriber = getMappingValueSubscriberIDToAlternateIDRedisIndex(alternateID, foundSubscriberID); //
               
                if(alternateIdsFromOneSubscriber != null) 
                  {
                    JSONObject reverseAlternateID = new JSONObject();
                    alternateIDsMap.put(alternateID, reverseAlternateID);
                    JSONObject oneReverse = new JSONObject();
                    oneReverse.putAll(alternateIdsFromOneSubscriber);
                    reverseAlternateID.put("reverse", oneReverse);
                    
                    Map<String, String> straight = getMappingValueAlternateIDToSubcriberIDRedisIndex(alternateID, alternateIdsFromOneSubscriber.get(foundSubscriberID));
                    if(straight != null && straight.size() > 0) {
                      JSONObject straightJSON = new JSONObject();
                      straightJSON.putAll(straight);
                      reverseAlternateID.put("straight", straight);                      
                    }
                  }
              }
            JSONObject arrayElement = new JSONObject();
            arrayElement.put("subscriberID", foundSubscriberID);
            arrayElement.put("alternateIDs", alternateIDsMap);
            subscriberIDs.add(arrayElement);
          }
        return result;        
      }
    else 
      {
        JSONObject result = new JSONObject();
        result.put("error", "no subscriberID for " + alternateIDName +  " " + alternateIDValue);
        return result;
      }
  }

  private Map<String, String> getMappingValueAlternateIDToSubcriberIDRedisIndex(String alternateIDName, String alternateIDValue)
  {
    if(alternateIDValue == null) { return null; }
    
    List<byte[]> binaryAlternateIDs = new ArrayList<byte[]>();
    
    binaryAlternateIDs.add(alternateIDValue.getBytes(StandardCharsets.UTF_8));
    int redisCacheIndex = Deployment.getAlternateIDs().get(alternateIDName).getRedisCacheIndex();
    
    /****************************************
    *
    *  retrieve from redis
    *
    ****************************************/
    
    BinaryJedis jedis = jedisSentinelPool.getResource();
    List<byte[]> retrievedBinaryIDs = null;
    try
      {
        jedis.select(redisCacheIndex);
        retrievedBinaryIDs = jedis.mget(binaryAlternateIDs.toArray(new byte[0][0]));
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

        return null;
      }
    finally
      {
        try { jedis.close(); } catch (JedisException e1) { }
      }

    /****************************************
    *
    *  extract from binaries
    *
    ****************************************/

    Map<String,String> result = new HashMap<String,String>();
    for (int i = 0; i < binaryAlternateIDs.size(); i++)
      {
        String keyInRedis = new String(binaryAlternateIDs.get(i), StandardCharsets.UTF_8);
        
        String valueInRedis = null;        
        if (retrievedBinaryIDs.get(i) != null)
          {
            int sizeModulo = retrievedBinaryIDs.get(i).length % 8;
            // after multitenancy, the length is 4 + 8* n before it was 2 + 8 * n
            int tmpTenantID = sizeModulo == 4 ? /*after mutlitenancy*/ Shorts.fromByteArray(Arrays.copyOfRange(retrievedBinaryIDs.get(i), 0, 2)) : 1;
            short numberOfSubscriberIDs = sizeModulo == 4 ? /* after multitenancy */ Shorts.fromByteArray(Arrays.copyOfRange(retrievedBinaryIDs.get(i), 2, 4)) : Shorts.fromByteArray(Arrays.copyOfRange(retrievedBinaryIDs.get(i), 0, 2));
            if (numberOfSubscriberIDs > 1) throw new RuntimeException("invariant violated - multiple subscriberIDs");
            valueInRedis = (numberOfSubscriberIDs == 1) ? (sizeModulo == 4 ? Long.toString(Longs.fromByteArray(Arrays.copyOfRange(retrievedBinaryIDs.get(i), 4, 12))) : Long.toString(Longs.fromByteArray(Arrays.copyOfRange(retrievedBinaryIDs.get(i), 2, 10))) ) : null;
            result.put(keyInRedis, valueInRedis);
            result.put("tenantID", ""+tmpTenantID);
          }
      }
    return result;
  }
  
  private Map<String, String> getMappingValueSubscriberIDToAlternateIDRedisIndex(String alternateIDName, String subscriberID)
  {
    
    List<byte[]> binaryAlternateIDs = new ArrayList<byte[]>();
    
    binaryAlternateIDs.add(Longs.toByteArray(Long.parseLong(subscriberID)));
    int redisCacheIndex = Deployment.getAlternateIDs().get(alternateIDName).getReverseRedisCacheIndex();

    /****************************************
    *
    *  retrieve from redis
    *
    ****************************************/
    
    BinaryJedis jedis = jedisSentinelPool.getResource();
    List<byte[]> retrievedBinaryIDs = null;
    try
      {
        jedis.select(redisCacheIndex);
        retrievedBinaryIDs = jedis.mget(binaryAlternateIDs.toArray(new byte[0][0]));
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

        return null;
      }
    finally
      {
        try { jedis.close(); } catch (JedisException e1) { }
      }

    /****************************************
    *
    *  extract from binaries
    *
    ****************************************/

    Map<String,String> result = new HashMap<String,String>();
    for (int i = 0; i < binaryAlternateIDs.size(); i++)
      {
        String keyInRedis = "" + Longs.fromByteArray(binaryAlternateIDs.get(i));
        
        String valueInRedis = null;        
        if (retrievedBinaryIDs.get(i) != null)
          {
            valueInRedis = new String(retrievedBinaryIDs.get(i), StandardCharsets.UTF_8);
            result.put(keyInRedis, valueInRedis);
          }
      }
    return result;
  }
  
  public boolean deleteRedisReverseAlternateID(String alternateIDName, String subscriberID)
  {
    List<byte[]> binaryAlternateIDs = new ArrayList<byte[]>();
    
    binaryAlternateIDs.add(Longs.toByteArray(Long.parseLong(subscriberID)));
    int redisCacheIndex = Deployment.getAlternateIDs().get(alternateIDName).getReverseRedisCacheIndex();

    /****************************************
    *
    *  retrieve from redis
    *
    ****************************************/
    
    BinaryJedis jedis = jedisSentinelPool.getResource();
    try
      {
        jedis.select(redisCacheIndex);
        jedis.del(Longs.toByteArray(Long.parseLong(subscriberID)));
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

        return false;
      }
    finally
      {
        try { jedis.close(); } catch (JedisException e1) { }
      }
    return true;
  }

  
  public boolean deleteRedisStraightAlternateID(String alternateIDName, String alternateIDValue)
  {
    if(alternateIDValue == null) { return false; }
    
    List<byte[]> binaryAlternateIDs = new ArrayList<byte[]>();
    
    binaryAlternateIDs.add(alternateIDValue.getBytes(StandardCharsets.UTF_8));
    int redisCacheIndex = Deployment.getAlternateIDs().get(alternateIDName).getRedisCacheIndex();
    
    /****************************************
    *
    *  retrieve from redis
    *
    ****************************************/
    
    BinaryJedis jedis = jedisSentinelPool.getResource();
    try
      {
        jedis.select(redisCacheIndex);
        jedis.del(alternateIDValue.getBytes(StandardCharsets.UTF_8));
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

        return false;
      }
    finally
      {
        try { jedis.close(); } catch (JedisException e1) { }
      }
    return true;
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

  // return null if does not exist
  public Pair<String, Integer> getSubscriberIDAndTenantID(String alternateIDName, String alternateID) throws SubscriberIDServiceException {
    Map<String,Pair<Integer,List<Long>>> subscriberIDs = getSubscriberIDs(alternateIDName, Collections.singleton(alternateID));
    Pair<Integer,List<Long>> subscriberID = subscriberIDs.get(alternateID);
    if(subscriberID==null||subscriberID.getSecondElement()==null||subscriberID.getSecondElement().isEmpty()) return null;//no mapping entry return null
    return new Pair<>(subscriberID.getSecondElement().get(0)+"",subscriberID.getFirstElement());//if several subscriberIDs stored, we are capped to first result at that moment
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
