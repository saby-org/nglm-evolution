/*****************************************************************************
*
*  RLMDateUtils.java
*
*  Copyright 2000-2012 Sixth Sense Media, Inc.  All Rights Reserved.
*
*****************************************************************************/

package com.evolving.nglm.core;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.lang3.time.DateUtils;

public class RLMDateUtils
{
  /*****************************************
  *
  *  utility methods
  *
  *****************************************/

  //
  //  addYears
  //

  public static Date addYears(Date date, int amount, String timeZone)
  {
    Calendar calendar = null;
    try
      {
        calendar = getCalendar(timeZone);
        return addYears(date, amount, calendar);
      }
    finally
      {
        if (calendar != null) returnCalendar(calendar);
      }
  }
      
  //
  //  addYears (internal)
  //

  public static Date addYears(Date date, int amount, Calendar calendar)
  {
    calendar.setTime(date);
    calendar.add(Calendar.YEAR, amount);
    return calendar.getTime();
  }

  //
  //  addMonths
  //

  public static Date addMonths(Date date, int amount, String timeZone)
  {
    Calendar calendar = null;
    try
      {
        calendar = getCalendar(timeZone);
        return addMonths(date, amount, calendar);
      }
    finally
      {
        if (calendar != null) returnCalendar(calendar);
      }
  }
      
  //
  //  addMonths (internal)
  //

  private static Date addMonths(Date date, int amount, Calendar calendar)
  {
    calendar.setTime(date);
    calendar.add(Calendar.MONTH, amount);
    return calendar.getTime();
  }

  //
  //  addWeeks
  //

  public static Date addWeeks(Date date, int amount, String timeZone)
  {
    Calendar calendar = null;
    try
      {
        calendar = getCalendar(timeZone);
        calendar.setTime(date);
        calendar.add(Calendar.DATE, 7*amount);
        return calendar.getTime();
      }
    finally
      {
        if (calendar != null) returnCalendar(calendar);
      }
  }
  
  //
  //  addDays
  //

  public static Date addDays(Date date, int amount, String timeZone)
  {
    Calendar calendar = null;
    try
      {
        calendar = getCalendar(timeZone);
        return addDays(date, amount, calendar);
      }
    finally
      {
        if (calendar != null) returnCalendar(calendar);
      }
  }
      
  //
  //  addDays (internal)
  //

  private static Date addDays(Date date, int amount, Calendar calendar)
  {
    calendar.setTime(date);
    calendar.add(Calendar.DATE, amount);
    return calendar.getTime();
  }
  
  //
  //  addHours
  //

  public static Date addHours(Date date, int amount)
  {
    return DateUtils.addHours(date, amount);
  }
  
  //
  //  addMinutes
  //

  public static Date addMinutes(Date date, int amount)
  {
    return DateUtils.addMinutes(date, amount);
  }

  //
  //  addSeconds
  //

  public static Date addSeconds(Date date, int amount)
  {
    return DateUtils.addSeconds(date, amount);
  }

  //
  //  addMilliseconds
  //

  public static Date addMilliseconds(Date date, int amount)
  {
    return DateUtils.addMilliseconds(date, amount);
  }

  //
  //  daysBetween - partial day counts as 1
  //

  private static Map<List<Object>, Integer> daysBetweenCache = new HashMap<List<Object>, Integer>();
  private static Map<List<Object>, Integer> daysBetweenCacheForUpdate = new LinkedHashMap<List<Object>,Integer>() { @Override protected boolean removeEldestEntry(Map.Entry eldest) { return size() > 1000; } };
  public static int daysBetween(Date firstDay, Date secondDay, String timeZone)
  {
    Calendar helperCalendar = null;
    Calendar first = null;
    Calendar second = null;
    try
      {
        //
        //  check cache, breaking out early if already exists
        //

        List<Object> requestKey = new ArrayList<Object>();
        requestKey.add(firstDay);
        requestKey.add(secondDay);
        requestKey.add(timeZone);
        Integer cachedResult = daysBetweenCache.get(requestKey);
        if (cachedResult != null) return cachedResult.intValue();
        
        //
        //  initialize
        //  

        helperCalendar = getCalendar(timeZone);
        first = getCalendar(timeZone);
        second = getCalendar(timeZone);
        first.setTime(firstDay);
        second.setTime(secondDay);

        //
        //  by thousands
        //

        int result = 0;
        while (addDays(first.getTime(),1000,helperCalendar).before(second.getTime()))
          {
            first.add(Calendar.DATE,1000);
            result += 1000;
          }

        //
        //  by hundreds
        //

        while (addDays(first.getTime(),100,helperCalendar).before(second.getTime()))
          {
            first.add(Calendar.DATE,100);
            result += 100;
          }

        //
        //  by tens
        //

        while (addDays(first.getTime(),10,helperCalendar).before(second.getTime()))
          {
            first.add(Calendar.DATE,10);
            result += 10;
          }

        //
        //  by ones
        //

        while (addDays(first.getTime(),1,helperCalendar).before(second.getTime()))
          {
            first.add(Calendar.DATE,1);
            result += 1;
          }

        //
        //  final
        //

        while (first.getTime().before(second.getTime()))
          {
            first.add(Calendar.DATE,1);
            result += 1;
          }

        //
        //  update cache
        //

        synchronized (daysBetweenCacheForUpdate)
          {
            daysBetweenCacheForUpdate.put(requestKey, result);
            daysBetweenCache = new HashMap<List<Object>, Integer>(daysBetweenCacheForUpdate);
          }

        //
        //  return
        //
        
        return result;
      }
    finally
      {
        if (helperCalendar != null) returnCalendar(helperCalendar);
        if (first != null) returnCalendar(first);
        if (second != null) returnCalendar(second);
      }
  }

  //
  //  monthsBetween - partial month counts as 1
  //

  private static Map<List<Object>, Integer> monthsBetweenCache = new HashMap<List<Object>, Integer>();
  private static Map<List<Object>, Integer> monthsBetweenCacheForUpdate = new LinkedHashMap<List<Object>,Integer>() { @Override protected boolean removeEldestEntry(Map.Entry eldest) { return size() > 1000; } };
  public static int monthsBetween(Date firstDay, Date secondDay, String timeZone)
  {
    Calendar helperCalendar = null;
    Calendar first = null;
    Calendar second = null;
    try
      {
        //
        //  check cache, breaking out early if already exists
        //

        List<Object> requestKey = new ArrayList<Object>();
        requestKey.add(firstDay);
        requestKey.add(secondDay);
        requestKey.add(timeZone);
        Integer cachedResult = monthsBetweenCache.get(requestKey);
        if (cachedResult != null) return cachedResult.intValue();
        
        //
        //  initialize
        //  

        helperCalendar = getCalendar(timeZone);
        first = getCalendar(timeZone);
        second = getCalendar(timeZone);
        first.setTime(firstDay);
        second.setTime(secondDay);

        //
        //  by thousands
        //

        int result = 0;
        while (addMonths(first.getTime(),1000,helperCalendar).before(second.getTime()))
          {
            first.add(Calendar.MONTH,1000);
            result += 1000;
          }

        //
        //  by hundreds
        //

        while (addMonths(first.getTime(),100,helperCalendar).before(second.getTime()))
          {
            first.add(Calendar.MONTH,100);
            result += 100;
          }

        //
        //  by tens
        //

        while (addMonths(first.getTime(),10,helperCalendar).before(second.getTime()))
          {
            first.add(Calendar.MONTH,10);
            result += 10;
          }

        //
        //  by ones
        //

        while (addMonths(first.getTime(),1,helperCalendar).before(second.getTime()))
          {
            first.add(Calendar.MONTH,1);
            result += 1;
          }

        //
        //  final
        //

        while (first.getTime().before(second.getTime()))
          {
            first.add(Calendar.MONTH,1);
            result += 1;
          }

        //
        //  update cache
        //

        synchronized (monthsBetweenCacheForUpdate)
          {
            monthsBetweenCacheForUpdate.put(requestKey, result);
            monthsBetweenCache = new HashMap<List<Object>, Integer>(monthsBetweenCacheForUpdate);
          }

        //
        //  return
        //

        return result;
      }
    finally
      {
        if (helperCalendar != null) returnCalendar(helperCalendar);
        if (first != null) returnCalendar(first);
        if (second != null) returnCalendar(second);
      }
  }

  //
  //  ceiling
  //

  public static Date ceiling(Date date, int field, String timeZone)
  {
    return ceiling(date, field, Calendar.SUNDAY, timeZone);
  }
  
  //
  //  ceiling
  //

  public static Date ceiling(Date date, int field, int firstDayOfWeek, String timeZone)
  {
    Calendar calendar = null;
    try
      {
        calendar = getCalendar(timeZone);
        calendar.setTime(date);
        Calendar result;
        switch (field)
          {
            case Calendar.DAY_OF_WEEK:
              Calendar day = DateUtils.ceiling(calendar,Calendar.DATE);
              while (day.get(Calendar.DAY_OF_WEEK) != firstDayOfWeek) day.add(Calendar.DATE,1);
              result = day;
              break;
            default:
              result = truncate(date, field, timeZone).equals(date) ? calendar : DateUtils.ceiling(calendar, field);
              break;
          }
        return result.getTime();
      }
    finally
      {
        if (calendar != null) returnCalendar(calendar);
      }
  }

  //
  //  truncate
  //

  public static Date truncate(Date date, int field, String timeZone)
  {
    return truncate(date, field, Calendar.SUNDAY, timeZone);
  }

  //
  //  truncate
  //

  public static Date truncate(Date date, int field, int firstDayOfWeek, String timeZone)
  {
    Calendar calendar = null;
    try
      {
        calendar = getCalendar(timeZone);
        calendar.setTime(date);
        Calendar result;
        switch (field)
          {
            case Calendar.DAY_OF_WEEK:
              Calendar day = DateUtils.truncate(calendar,Calendar.DATE);
              while (day.get(Calendar.DAY_OF_WEEK) != firstDayOfWeek) day.add(Calendar.DATE,-1);
              result = day;
              break;
            default:
              result = DateUtils.truncate(calendar, field);
              break;
          }
        return result.getTime();
      }
    finally
      {
        if (calendar != null) returnCalendar(calendar);
      }
  }
  
  //
  //  truncatedCompareTo
  //

  public static int truncatedCompareTo(Date date1, Date date2, int field, String timeZone)
  {
    return truncate(date1,field,timeZone).compareTo(truncate(date2,field,timeZone));
  }
  
  //
  //  truncatedEquals
  //

  public static boolean truncatedEquals(Date date1, Date date2, int field, String timeZone)
  {
    return (truncatedCompareTo(date1, date2, field, timeZone) == 0);
  }

  //
  //  getField
  //

  public static int getField(Date date, int field, String timeZone)
  {
    Calendar calendar = null;
    try
      {
        calendar = getCalendar(timeZone);
        calendar.setTime(date);
        return calendar.get(field);
      }
    finally
      {
        if (calendar != null) returnCalendar(calendar);
      }
  }

  //
  //  setField
  //

  public static Date setField(Date date, int field, int value, String timeZone)
  {
    Calendar calendar = null;
    try
      {
        calendar = getCalendar(timeZone);
        calendar.setTime(date);
        calendar.set(field, value);
        return calendar.getTime();
      }
    finally
      {
        if (calendar != null) returnCalendar(calendar);
      }
  }

  //
  //  lastFullPeriod
  //

  public static Date lastFullPeriod(Date startDate, Date endDate, int field, String timeZone)
  {
    Calendar startDay = null;
    try
      {
        startDay = getCalendar(timeZone);
        startDay.setTime(DateUtils.truncate(startDate, Calendar.DATE));

        Date endDay = DateUtils.truncate(endDate, Calendar.DATE);
    
        int periods=0;
        int increment = (field == Calendar.DAY_OF_WEEK) ? 7 : 1;
        while (startDay.getTime().before(endDay) || startDay.getTime().equals(endDay) )
          {
            startDay.add(Calendar.DATE,increment);
            periods++;
          }
        Date result = DateUtils.addDays(startDate, increment*(periods-1));

        return result;
      }
    finally
      {
        if (startDay != null) returnCalendar(startDay);
      }
  }

  /*****************************************
  *
  *  parseDate
  *
  *****************************************/

  public static Date parseDate(String stringDate, String format, String timeZone)
  {
    Date result = null;
    try
      {
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        dateFormat.setTimeZone(TimeZone.getTimeZone(timeZone));
        if (stringDate != null && stringDate.trim().length() > 0)
          {
            result = dateFormat.parse(stringDate.trim());
          }
      }
    catch (ParseException e)
      {
        throw new ServerRuntimeException("parseDateField", e);
      }
    return result;
  }
  
  /*****************************************
  *
  *  parseDate
  *
  *****************************************/

  public static Date parseDate(String stringDate, String format, String timeZone, boolean lenient)
  {
    Date result = null;
    try
      {
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        dateFormat.setLenient(lenient);
        dateFormat.setTimeZone(TimeZone.getTimeZone(timeZone));
        if (stringDate != null && stringDate.trim().length() > 0)
          {
            result = dateFormat.parse(stringDate.trim());
          }
      }
    catch (ParseException e)
      {
        throw new ServerRuntimeException("parseDateField", e);
      }
    return result;
  }
  
  /*****************************************
  *
  *  calendar pool
  *
  *****************************************/

  //
  //  static pool
  //
  
  private static Map<String, BlockingQueue<Calendar>> calendarPool = new ConcurrentHashMap<String, BlockingQueue<Calendar>>();

  //
  //  getCalendar
  //

  private static Calendar getCalendar(String timeZone)
  {
    BlockingQueue<Calendar> timeZoneCalendarQueue = calendarPool.get(timeZone);
    if (timeZoneCalendarQueue == null)
      {
        timeZoneCalendarQueue = new LinkedBlockingQueue<Calendar>();
        calendarPool.put(timeZone, timeZoneCalendarQueue);
      }
    Calendar result = timeZoneCalendarQueue.poll();
    if (result == null) result = Calendar.getInstance(TimeZone.getTimeZone(timeZone));
    return result;
  }

  //
  //  returnCalendar
  //

  private static void returnCalendar(Calendar calendar)
  {
    String timeZone = calendar.getTimeZone().getID();
    BlockingQueue<Calendar> timeZoneCalendarQueue = calendarPool.get(timeZone);
    if (timeZoneCalendarQueue == null)
      {
        timeZoneCalendarQueue = new LinkedBlockingQueue<Calendar>();
        calendarPool.put(timeZone, timeZoneCalendarQueue);
      }
    timeZoneCalendarQueue.offer(calendar);
  }
}
