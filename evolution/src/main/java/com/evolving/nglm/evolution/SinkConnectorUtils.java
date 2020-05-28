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
        log.info("subscriberID : " + subscriberID);
        SubscriberProfile subscriberProfile = subscriberProfileService.getSubscriberProfile(subscriberID, false, false);
        log.info("subscriber profile : " + subscriberProfile);
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
            log.info("method " + methodName);
            try
            {
              Method method = subscriberProfile.getClass().getMethod(methodName);
              Object alternateIDValue = method.invoke(subscriberProfile);
              //if (log.isTraceEnabled())
                log.info("adding " + alternateID.getID() + " with " + alternateIDValue);
              map.put(alternateID.getID(), alternateIDValue);
            }
            catch (NoSuchMethodException|SecurityException|IllegalAccessException|IllegalArgumentException|InvocationTargetException e)
              {
                log.warn("Problem retrieving alternateID " + alternateID.getID() + " : " + e.getLocalizedMessage());
              }
          }
      }
    catch (SubscriberProfileServiceException e1)
      {
        log.warn("Cannot resolve " + subscriberID);
      }
    }
}
