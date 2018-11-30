/****************************************************************************
*
*  ThirdPartyAccessStatistics.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.NGLMMonitoringObject;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.ServerException;

import com.evolving.nglm.core.SystemTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class ThirdPartyAccessStatistics implements ThirdPartyAccessStatisticsMBean, NGLMMonitoringObject
{
  //
  //  Base JMX object name
  //

  public static String BaseJMXObjectName = "com.evolving.nglm.evolution:type=ThirdPartyAccess";

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(ThirdPartyAccessStatistics.class);
  

  //
  //  attributes
  //

  String objectNameForManagement;
  int pingCount;
  int getCustomerCount;
  int getActiveOfferCount;
  int getActiveOffersCount;
  int successfulAPIRequestCount;
  int failedAPIRequestCount;
  int totalAPIRequestCount;

  //
  // Interface: ThirdPartyAccessStatisticsMBean
  //

  public int getPingCount() { return pingCount; }
  public int getGetCustomerCount() { return getCustomerCount; }
  public int getGetActiveOfferCount() { return getActiveOfferCount; }
  public int getGetActiveOffersCount() { return getActiveOffersCount; }
  public int getSuccessfulAPIRequestCount() { return successfulAPIRequestCount; }
  public int getFailedAPIRequestCount() { return failedAPIRequestCount; }
  public int getTotalAPIRequestCount() { return totalAPIRequestCount; }

  //
  // Interface: NGLMMonitoringObject
  //

  public String getObjectNameForManagement() { return objectNameForManagement; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public ThirdPartyAccessStatistics(String serviceName) throws ServerException
  {
    //
    //  counts
    //
    
    this.pingCount = 0;
    this.getCustomerCount = 0;
    this.getActiveOfferCount = 0;
    this.getActiveOffersCount = 0;
    this.successfulAPIRequestCount = 0;
    this.failedAPIRequestCount = 0;
    this.totalAPIRequestCount = 0;

    //
    //  object name
    //

    this.objectNameForManagement = BaseJMXObjectName + ",serviceName=" + serviceName;

    //
    // register
    //

    NGLMRuntime.registerMonitoringObject(this, true);
  }

  /*****************************************
  *
  *  update methods
  *
  *****************************************/
  
  public void updatePingCount(int count) { pingCount = pingCount + count; }
  public void updateGetCustomerCount(int count) { getCustomerCount = getCustomerCount + count; }
  public void updateGetActiveOfferCount(int count) { getActiveOfferCount = getActiveOfferCount + count; }
  public void updateGetActiveOffersCount(int count) { getActiveOffersCount = getActiveOffersCount + count; }
  public void updateSuccessfulAPIRequestCount(int count) { successfulAPIRequestCount = successfulAPIRequestCount + count; }
  public void updateFailedAPIRequestCount(int count) { failedAPIRequestCount = failedAPIRequestCount + count; }
  public void updateTotalAPIRequestCount(int count) { totalAPIRequestCount = totalAPIRequestCount + count; }

  /*****************************************
  *
  *  unregister
  *
  *****************************************/

  public void unregister()
  {
    NGLMRuntime.unregisterMonitoringObject(this);
  }
}

