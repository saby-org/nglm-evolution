/****************************************************************************
*
*  FileSourceTaskRecordStatistics.java
*
****************************************************************************/

package com.evolving.nglm.core;

import com.evolving.nglm.core.Alarm.AlarmLevel;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LicenseExpiryStatistics implements LicenseExpiryStatisticsMBean, NGLMMonitoringObject
{
  //
  //  Base JMX object name
  //

  public static String BaseJMXObjectName = "com.evolving.nglm.core:type=LicenseExpiry";

  //
  //  attributes
  //

  Date expireDate;
  AlarmLevel alarmLevel;
  String objectNameForManagement;

  //
  //  logger
  //

  protected static final Logger log = LoggerFactory.getLogger(LicenseExpiryStatistics.class);

  //
  // Interface: LicenseExpiryStatisticsMBean
  //

  public int getAlarmLevel() { return alarmLevel.getExternalRepresentation(); }
  public Date getExpireDate() { return expireDate; }
  public double getDaysBeforeExpiry()
  {
    long seconds = (expireDate.getTime() - SystemTime.getCurrentTime().getTime()) / 1000;
    return ((double) seconds) / (60*60*24);
  }
  public double getHoursBeforeExpiry()
  {
    
    long seconds = (expireDate.getTime() - SystemTime.getCurrentTime().getTime()) / 1000;
    return ((double) seconds) / (60*60);
  }

  //
  // Interface: NGLMMonitoringObject
  //

  public String getObjectNameForManagement() { return objectNameForManagement; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public LicenseExpiryStatistics(String id, Date expireDate) throws ServerException
  {
    this.alarmLevel = AlarmLevel.Unknown;
    this.expireDate = expireDate;
    this.objectNameForManagement = BaseJMXObjectName + ",id=" + id;

    //
    // register
    //

    log.info("Registering MBEAN {}", this.objectNameForManagement);
    NGLMRuntime.registerMonitoringObject(this, true);
  }

  /*****************************************
  *
  *  updateAlarmLevel
  *
  *****************************************/
      
  synchronized void updateAlarmLevel(AlarmLevel newLevel)
  {
    this.alarmLevel = newLevel;
  }

  synchronized void updateExpireDate(Date newExpireDate)
  {
    this.expireDate = newExpireDate;
  }

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