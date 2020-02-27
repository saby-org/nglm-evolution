/****************************************************************************
*
*  ThirdPartyAccessStatistics.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.NGLMMonitoringObject;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.ServerException;

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
  int getOffersListCount;
  int getLoyaltyProgramListCount;
  int getActiveOfferCount;
  int getActiveOffersCount;
  int validateVoucherCount;
  int redeemVoucherCount;
  int successfulAPIRequestCount;
  int failedAPIRequestCount;
  int totalAPIRequestCount;

  //
  // Interface: ThirdPartyAccessStatisticsMBean
  //

  public int getPingCount() { return pingCount; }
  public int getGetCustomerCount() { return getCustomerCount; }
  public int getGetOffersListCount() { return getOffersListCount; }
  public int getGetLoyaltyProgramListCount() { return getLoyaltyProgramListCount; }
  public int getGetActiveOfferCount() { return getActiveOfferCount; }
  public int getGetActiveOffersCount() { return getActiveOffersCount; }
  public int getValidateVoucherCount() { return validateVoucherCount; }
  public int getRedeemVoucherCount() { return redeemVoucherCount; }
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
    this.getOffersListCount = 0;
    this.getLoyaltyProgramListCount = 0;
    this.getActiveOfferCount = 0;
    this.getActiveOffersCount = 0;
    this.validateVoucherCount = 0;
    this.redeemVoucherCount = 0;
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
  public void updateGetOffersListCount(int count) { getOffersListCount = getOffersListCount + count; }
  public void updateGetLoyaltyProgramListCount(int count) { getLoyaltyProgramListCount = getLoyaltyProgramListCount + count; }
  public void updateGetActiveOfferCount(int count) { getActiveOfferCount = getActiveOfferCount + count; }
  public void updateGetActiveOffersCount(int count) { getActiveOffersCount = getActiveOffersCount + count; }
  public void updateValidateVoucherCount(int count) { validateVoucherCount = validateVoucherCount + count; }
  public void updateRedeemVoucherCount(int count) { redeemVoucherCount = redeemVoucherCount + count; }
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

