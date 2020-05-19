/**********************************************
*
*  SystemTime.java
*
*
*  Copyright 2000-2009 RateIntegration, Inc.  All Rights Reserved.
*
**********************************************/

package com.evolving.nglm.core;

import java.util.*;

/**
 * SystemTime is the preferred API for getting the current time and
 * date. Use of this API ensures that an implementation is coherent with the rating engine's
 * view of the current date and time. Direct use of e.g. <tt>new java.util.Date()</tt> or
 * java.lang.System.currentTimeMillis() to get the current time is discouraged.
 */
public class SystemTime
{
  private static Date systemStart = new Date();
  private static Date effectiveSystemStart = systemStart;

  //excludemethodjavadocs-begin
  public static void setEffectiveSystemStartTime(Date effectiveStartTime)
  {
    if (effectiveStartTime.after(getCurrentTime()))
      {
        systemStart = new Date();
        effectiveSystemStart = effectiveStartTime;
      }
  }
  //excludemethodjavadocs-end

  /**
   * Returns the effective start time for the rating engine. Depending on rating engine start parameters, this time may differ
   * from the actual start time of the system.
   * @return The effective start time for the system.
   */
  public static Date getEffectiveSystemStartTime()
  {
    return effectiveSystemStart;
  }

  /**
   * Returns the system's view of the current time.
   * @return A java.util.Date representing the system's view of the current time.
   */

  public static Date getCurrentTime()
  {
    long timeElapsed = System.currentTimeMillis() - systemStart.getTime(); // now is always > systemStart => timeElasped > 0
    Date result = new Date(effectiveSystemStart.getTime() + timeElapsed);
    return result;
  }

  /**
   * Returns the system's view of the current time as a long 
   * @return A long value representing the system's view of the current time expressed as 
   *   milliseconds since the epoch 00:00:00 GMT 1 January 1970.
   */
  public static long currentTimeMillis()
  {
    return SystemTime.getCurrentTime().getTime();
  }

  /**
   * Returns a Calendar based on the system's view of the current time.
   * @return A java.util.Calendar representing the system's view of the current time.
   */
  public static Calendar getCalendar()
  {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(SystemTime.getCurrentTime());
    return calendar;
  }

  /**
   * Returns a GregorianCalendar based on the system's view of the current time.
   * @return A java.util.GregorianCalendar representing the system's view of the current time.
   */
  public static GregorianCalendar getGregorianCalendar()
  {
    GregorianCalendar gregorianCalendar = new GregorianCalendar();
    gregorianCalendar.setTime(SystemTime.getCurrentTime());
    return gregorianCalendar;
  }

  //excludemethodjavadocs-begin

  //
  //  getActualCurrentTime
  //
  //  NOTE: THIS METHOD SHOULD ONLY BE USED IF YOU REALLY WANT THE ACTUAL CURRENT TIME
  //  YOU SHOULD USUALLY USE getCurrentTime()
  //
  
  public static Date getActualCurrentTime()
  {
    return new Date();
  }
  //excludemethodjavadocs-end
  
}
