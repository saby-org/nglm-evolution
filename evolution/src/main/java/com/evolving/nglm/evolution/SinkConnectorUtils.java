package com.evolving.nglm.evolution;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.AlternateID;
import com.evolving.nglm.evolution.SubscriberProfileService.EngineSubscriberProfileService;
import com.evolving.nglm.evolution.SubscriberProfileService.SubscriberProfileServiceException;

public class SinkConnectorUtils
{
  private static final Logger log = LoggerFactory.getLogger(SinkConnectorUtils.class);

  public static SubscriberProfileService init()
  {
    SubscriberProfileService subscriberProfileService;
    subscriberProfileService = new EngineSubscriberProfileService(Deployment.getSubscriberProfileEndpoints());
    subscriberProfileService.start();
    return subscriberProfileService;
  }
  
  public static void putAlternateIDs(String subscriberID, Map<String,Object> map, SubscriberProfileService subscriberProfileService)
  {
    try
      {
        if (log.isTraceEnabled()) log.trace("subscriberID : " + subscriberID);
        SubscriberProfile subscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, false, false);
        putAlternateIDs(subscriberID, map, subscriberProfile);
      }
    catch (SubscriberProfileServiceException e1)
      {
        log.warn("Cannot resolve " + subscriberID);
      }
    }

  public static void putAlternateIDs(String subscriberID, Map<String,Object> map, SubscriberProfile subscriberProfile)
  {
    boolean needToLog = log.isTraceEnabled();
    if (needToLog) log.trace("subscriberID : " + subscriberID);
    for (AlternateID alternateID : Deployment.getAlternateIDs().values())
      {
        String methodName;
        if ("msisdn".equals(alternateID.getID())) // special case
          {
            methodName = "getMSISDN";
          }
        else
          {
            methodName = "get" + StringUtils.capitalize(alternateID.getID());
          }
        if (needToLog) log.trace("method " + methodName);
        try
        {
          Method method = subscriberProfile.getClass().getMethod(methodName);
          Object alternateIDValue = method.invoke(subscriberProfile);
          if (needToLog) log.trace("adding " + alternateID.getID() + " with " + alternateIDValue);
          map.put(alternateID.getID(), alternateIDValue);
        }
        catch (NoSuchMethodException|SecurityException|IllegalAccessException|IllegalArgumentException|InvocationTargetException e)
        {
          log.warn("Problem retrieving alternateID " + alternateID.getID() + " : " + e.getLocalizedMessage());
        }
      }
  }

  public static void putAlternateIDsString(String subscriberID, Map<String,String> map, SubscriberProfile subscriberProfile)
  {
    boolean needToLog = log.isTraceEnabled();
    if (needToLog) log.trace("subscriberID : " + subscriberID);
    for (AlternateID alternateID : Deployment.getAlternateIDs().values())
      {
        String methodName;
        if ("msisdn".equals(alternateID.getID())) // special case
          {
            methodName = "getMSISDN";
          }
        else
          {
            methodName = "get" + StringUtils.capitalize(alternateID.getID());
          }
        if (needToLog) log.trace("method " + methodName);
        try
        {
          Method method = subscriberProfile.getClass().getMethod(methodName);
          Object alternateIDValue = method.invoke(subscriberProfile);
          String alternateIDStr = "" + alternateIDValue;
          if (needToLog) log.trace("adding " + alternateID.getID() + " with " + alternateIDStr);
          map.put(alternateID.getID(), alternateIDStr);
        }
        catch (NoSuchMethodException|SecurityException|IllegalAccessException|IllegalArgumentException|InvocationTargetException e)
        {
          log.warn("Problem retrieving alternateID " + alternateID.getID() + " : " + e.getLocalizedMessage());
        }
      }
  }

  public static void putAlternateIDs(Map<String,String> alternateIDsMap, Map<String,Object> map)
  {
    for (AlternateID alternateID : Deployment.getAlternateIDs().values())
      {
        String alternateIDValue = alternateIDsMap.get(alternateID.getID());
        if (log.isTraceEnabled()) log.trace("adding " + alternateID.getID() + " with " + alternateIDValue);
        map.put(alternateID.getID(), alternateIDValue);
      }
  }

}
