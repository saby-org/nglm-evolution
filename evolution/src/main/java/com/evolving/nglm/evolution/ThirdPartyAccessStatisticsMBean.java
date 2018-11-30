/****************************************************************************
*
*  ThirdPartyAccessStatisticsMBean.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Date;

public interface ThirdPartyAccessStatisticsMBean
{
  public int getPingCount();
  public int getGetCustomerCount();
  public int getGetActiveOfferCount();
  public int getGetActiveOffersCount();
  public int getSuccessfulAPIRequestCount();
  public int getFailedAPIRequestCount();
  public int getTotalAPIRequestCount();
}
