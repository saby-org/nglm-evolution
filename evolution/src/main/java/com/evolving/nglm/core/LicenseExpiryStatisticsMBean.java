/****************************************************************************
*
*  LicenseExpiryStatisticsMBean.java
*
****************************************************************************/

package com.evolving.nglm.core;

import java.util.Date;

public interface LicenseExpiryStatisticsMBean
{
  public int getAlarmLevel();
  public Date getExpireDate();
  public double getDaysBeforeExpiry();
  public double getHoursBeforeExpiry();
}
