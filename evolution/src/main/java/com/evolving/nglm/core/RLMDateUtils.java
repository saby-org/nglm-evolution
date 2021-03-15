/*****************************************************************************
*
*  RLMDateUtils.java
*
*  Copyright 2000-2012 Sixth Sense Media, Inc.  All Rights Reserved.
*
*****************************************************************************/

package com.evolving.nglm.core;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import org.apache.commons.lang3.time.DateUtils;

import com.evolving.nglm.evolution.Deployment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RLMDateUtils
{

  private static final Logger log = LoggerFactory.getLogger(RLMDateUtils.class);

  /*****************************************
  *
  *  display methods
  *
  *****************************************/
  
  // SimpleDateFormat is not threadsafe. 
  // In order to avoid instantiating the same object again an again we use a ThreadLocal static variable
  
  public static final ThreadLocal<DateFormat> TIMESTAMP_FORMAT = ThreadLocal.withInitial(()->{ 
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ"); // TODO EVPRO-99 use systemTimeZone instead of baseTimeZone, is it correct or should it be per tenant ???
    sdf.setTimeZone(TimeZone.getTimeZone(Deployment.getSystemTimeZone()));  
    return sdf;
  });
  
  public static final String printTimestamp(Date date) {
    return TIMESTAMP_FORMAT.get().format(date);
  }
  
  public static final ThreadLocal<DateFormat> DAY_FORMAT = ThreadLocal.withInitial(()->{
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    sdf.setTimeZone(TimeZone.getTimeZone(Deployment.getSystemTimeZone())); // TODO EVPRO-99 use systemTimeZone instead of baseTimeZone, is it correct or should it be per tenant ???
    return sdf;
  });
  
  public static final String printDay(Date date) {
    return DAY_FORMAT.get().format(date);
  }
  
  // IMPORTANTE NOTES : 
  // Here we use YYYY (Week year) because 2021-01-03 must display 2020-53 for instance
  // We also use Locale.FRANCE just to enforce ISO Week system (Monday as first day of week, minimum of 4 days for the first week of year, 1 to 53)
  // DO NOT USE Locale.US as it use non-standard Week system !
  // Always keep Local.FRANCE to enforce ISO week system, even if in another LOCALE (it should not have other impact than that)
  // DO ONLY USE this print for internal purpose as ISO week may not be the convention specified by the tenant.
  public static final ThreadLocal<DateFormat> WEEK_FORMAT = ThreadLocal.withInitial(()->{
    SimpleDateFormat sdf = new SimpleDateFormat("YYYY-'w'ww", Locale.FRANCE);
    sdf.setTimeZone(TimeZone.getTimeZone(Deployment.getSystemTimeZone()));
    return sdf;
  });
  
  public static final String printISOWeek(Date date) {
    return WEEK_FORMAT.get().format(date);
  }
  
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
    return addYears(date, amount, getCalendarInstance1(timeZone));
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
    return addMonths(date, amount, getCalendarInstance1(timeZone));
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
    Calendar calendar = getCalendarInstance1(timeZone);
    calendar.setTime(date);
    calendar.add(Calendar.DATE, 7*amount);
    return calendar.getTime();
  }
  
  //
  //  addDays
  //

  public static Date addDays(Date date, int amount, String timeZone)
  {
    return addDays(date, amount, getCalendarInstance1(timeZone));
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

    helperCalendar = getCalendarInstance1(timeZone);
    first = getCalendarInstance2(timeZone);
    second = getCalendarInstance3(timeZone);
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
  // won't care about time shifting, but way, way less expensive than previous method
  // I tried to keep same behavior than previous method, which is a probably bad, keeping same limitation probably for nothing
  public static int daysBetweenApproximative(Date firstDay, Date secondDay){
    Long first = firstDay.getTime();
    Long second = secondDay.getTime();
    if(second>first){
      second = second + 86399999;//for partial day to count as 1
    }else{
      return 0;
    }
    return (int)( ( second - first /*milliseconds diff*/ ) / ( 1000/*to sec*/ * 86400/*secs per day*/ ) );
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

    helperCalendar = getCalendarInstance1(timeZone);
    first = getCalendarInstance2(timeZone);
    second = getCalendarInstance3(timeZone);
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
    Calendar calendar = getCalendarInstance1(timeZone);
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
    Calendar calendar = getCalendarInstance1(timeZone);
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
    Calendar calendar = getCalendarInstance1(timeZone);
    calendar.setTime(date);
    return calendar.get(field);
  }

  //
  //  setField
  //

  public static Date setField(Date date, int field, int value, String timeZone)
  {
    Calendar calendar = getCalendarInstance1(timeZone);
    calendar.setTime(date);
    calendar.set(field, value);
    return calendar.getTime();
  }

  //
  //  lastFullPeriod
  //

  public static Date lastFullPeriod(Date startDate, Date endDate, int field, String timeZone)
  {
    Calendar startDay = getCalendarInstance1(timeZone);
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
  *  calendars pool
  *
  *****************************************/

  // Calendar, Date, SimpleDateFormat... is deprecated since java8 and java.time package
  // this entire class is there trying to compensate issues with those old objects
  // the solution would be to use java.time package
  // in the mean time, lets continue hacky stuff trying to have decent perf with those old objects

  // used those methods highlight the fact we keep instance per thread :
  private static Calendar getCalendarInstance1(String timeZone){return getCalendar(timeZone,0);}
  private static Calendar getCalendarInstance2(String timeZone){return getCalendar(timeZone,1);}
  private static Calendar getCalendarInstance3(String timeZone){return getCalendar(timeZone,2);}
  // here are the real instances cached :
  private static ThreadLocal<Map<String,Map<Integer,Calendar>>> calendars = ThreadLocal.withInitial(HashMap::new);
  private static Calendar getCalendar(String timeZone, int instanceNumber){
    Map<Integer,Calendar> threadCalendars = calendars.get().get(timeZone);
    if(threadCalendars==null){
      if(log.isDebugEnabled()) log.debug("RLMDateUtils.getCalendar() : no instances for timezone "+timeZone+" for this thread, creating instances pool");
      threadCalendars = new HashMap<>();
      calendars.get().put(timeZone,threadCalendars);
    }
    Calendar calendar = threadCalendars.get(instanceNumber);
    if(calendar==null){
      if(log.isDebugEnabled()) log.debug("RLMDateUtils.getCalendar() : no instance "+instanceNumber+" for timezone "+timeZone+" for this thread, creating Calendar");
      calendar = Calendar.getInstance(TimeZone.getTimeZone(timeZone));
      threadCalendars.put(instanceNumber,calendar);
    }
    return calendar;
  }

}
