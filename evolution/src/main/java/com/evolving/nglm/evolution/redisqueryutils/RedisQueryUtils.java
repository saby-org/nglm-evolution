/*****************************************************************************
*
*  GUIManager.java
*
*****************************************************************************/

package com.evolving.nglm.evolution.redisqueryutils;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.Pair;
import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.core.SubscriberIDService;
import com.evolving.nglm.core.SubscriberManager;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.CriterionField;
import com.evolving.nglm.evolution.SubscriberEvaluationRequest;
import com.evolving.nglm.evolution.SubscriberGroupEpoch;
import com.evolving.nglm.evolution.SubscriberProfile;
import com.evolving.nglm.evolution.SubscriberProfileService;
import com.evolving.nglm.evolution.SubscriberProfileService.EngineSubscriberProfileService;

import redis.clients.jedis.JedisSentinelPool;

public class RedisQueryUtils
{
  

  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  private static JedisSentinelPool jedisSentinelPool = null;
  private static String redisInstance = "subscriberids";
  private static Set<String> redisSentinels;
  private static SubscriberIDService subscriberIDService = null;
  private static SubscriberProfileService subscriberProfileService = null;

  /*****************************************
  *
  *  main
  *
  *****************************************/

  public static void main(String[] args) throws Exception
  {
    String alternateIDName = args[0];
    String alternateIDValue = args[1];
   
    subscriberProfileService = new EngineSubscriberProfileService(Deployment.getSubscriberProfileEndpoints(), 10);
    subscriberProfileService.start();
    
    String zk = System.getProperty("zookeeper.connect");

    subscriberIDService = new SubscriberIDService(Deployment.getRedisSentinels(), "AssignSubscriberIDsFileSourceConnector-Connector");
    
    NGLMRuntime.initialize(true);
    
    // retrieve the subscriber profile
    Pair<String, Integer> pair = subscriberIDService.getSubscriberIDAndTenantID(alternateIDName, alternateIDValue);
    if(pair == null)
      {
        System.out.println("Can't find subscriberID " + alternateIDValue + " through " + alternateIDName);
        return;
      }
    
    String subscriberID = pair.getFirstElement();
    SubscriberProfile profile = subscriberProfileService.getSubscriberProfile(subscriberID);
    
    if(profile == null)
      {
        System.out.println("Can't retrieve profile " + subscriberID);
      }
    
    Map<String, String> profileAlternateIDs = buildAlternateIDs(profile, profile.getTenantID());
    profileAlternateIDs.put("subscriberID", subscriberID);
    profileAlternateIDs.put("tenantID", ""+pair.getSecondElement());
   
    for(Map.Entry<String, String> entry : profileAlternateIDs.entrySet())
      {
        System.out.println(entry.getKey() + " : " + entry.getValue());
      }
    
//    /*****************************************
//    *
//    *  initialize jedis client
//    *
//    *****************************************/
//
//    JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
//    jedisPoolConfig.setTestOnBorrow(true);
//    jedisPoolConfig.setTestOnReturn(false);
//    jedisPoolConfig.setMaxTotal(2);
//    jedisPoolConfig.setJmxNamePrefix("SimpleRedisSinkConnector");
//    jedisPoolConfig.setJmxNameBase("AA");
//    
//    redisSentinels = new HashSet<String>();
//    redisSentinels.add(args[0]);
//    
//    String password = System.getProperty("redis.password");
//    if(password != null && !password.trim().equals("") && !password.trim().equals("none")) {
//      System.out.println("SimpleRedisSinkConnector() Use Redis Password " + password);
//      jedisSentinelPool = new JedisSentinelPool(redisInstance, redisSentinels, jedisPoolConfig, Protocol.DEFAULT_TIMEOUT, Protocol.DEFAULT_TIMEOUT, null, password, 0, null,
//          Protocol.DEFAULT_TIMEOUT, Protocol.DEFAULT_TIMEOUT, null, password, null);        
//    }
//    else {
//      System.out.println("SimpleRedisSinkConnector() No Redis Password");
//      jedisSentinelPool = new JedisSentinelPool(redisInstance, redisSentinels, jedisPoolConfig);
//    }  
  }
  
  private static Map<String,String> buildAlternateIDs(SubscriberProfile subscriberProfile, int tenantID) {
    ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader = ReferenceDataReader.<String,SubscriberGroupEpoch>startReader("redisQueryUtils-subscribergroupepoch", Deployment.getBrokerServers(), Deployment.getSubscriberGroupEpochTopic(), SubscriberGroupEpoch::unpack);
    
    Map<String,String> alternateIDs = new LinkedHashMap<>();
    SubscriberEvaluationRequest evaluationRequest = new SubscriberEvaluationRequest(subscriberProfile, subscriberGroupEpochReader, SystemTime.getCurrentTime(), tenantID);

    for (Map.Entry<String,AlternateID> entry:Deployment.getAlternateIDs().entrySet()) {
      AlternateID alternateID = entry.getValue();
      if (alternateID.getProfileCriterionField() == null ) {
        System.out.println("ProfileCriterionField is not given for alternate ID {} - skiping " + entry.getKey());
        continue;
      }
      CriterionField criterionField = Deployment.getProfileCriterionFields().get(alternateID.getProfileCriterionField());
      String criterionFieldValue = (String)criterionField.retrieveNormalized(evaluationRequest);
      System.out.println("adding {} {} for subscriber {} " + entry.getKey() + " " + criterionFieldValue + " " + subscriberProfile.getSubscriberID());
      alternateIDs.put(entry.getKey(), criterionFieldValue);
    }
    return alternateIDs;
  }

}

