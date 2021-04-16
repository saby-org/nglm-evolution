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

import org.apache.commons.lang3.time.DateUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RLMDateUtils
{
  private static final Logger log = LoggerFactory.getLogger(RLMDateUtils.class);

  /*****************************************
  *
  * date parsing & display
  *
  *****************************************/
  public enum DatePattern
  {
    ELASTICSEARCH_UNIVERSAL_TIMESTAMP("yyyy-MM-dd HH:mm:ss.SSSZ"),
    REST_UNIVERSAL_TIMESTAMP_DEFAULT("yyyy-MM-dd'T'HH:mm:ss:SSSXXX"),
    REST_UNIVERSAL_TIMESTAMP_ALTERNATE("yyyy-MM-dd'T'HH:mm:ssXXX"),
    LOCAL_DAY("yyyy-MM-dd");
    private String pattern;
    private DatePattern(String pattern) { this.pattern = pattern; }
    public String get() { return pattern; }
  }
  
  /**
   * WARNING: SimpleDateFormat is not threadsafe. Do not use it in a multi-thread context.
   * WARNING: Avoid calling this method multiple time, do not use it as a one-line format/parse method !!
   * In general, use the format/parse methods provided later
   */
  public static SimpleDateFormat createLocalDateFormat(DatePattern pattern, String timezone) {
    SimpleDateFormat result = new SimpleDateFormat(pattern.get());
    result.setTimeZone(TimeZone.getTimeZone(timezone));
    return result;
  }
  
  /*****************************************
  *
  * local date formats (per time-zone)
  *
  *****************************************/
  // This structure start empty for every thread. SimpleDateFormat will only be instantiated when needed.
  // The main reason for that is, we are in static code and we cannot access Deployment per tenant as 
  // it has not been extracted yet.
  //
  // !REMINDER: SimpleDateFormat is not threadsafe. In order to avoid instantiating the same object
  // again an again we use a ThreadLocal static variable.
  //
  // Map(TimeZone -> Map(Pattern -> Formatter))
  private static final ThreadLocal<Map<String,Map<String,SimpleDateFormat>>> LOCAL_DATE_FORMATS = ThreadLocal.withInitial(()->{
    return new HashMap<String,Map<String,SimpleDateFormat>>();
  });
  
  private static final SimpleDateFormat getLocalDateFormat(DatePattern pattern, String timeZone) {
    Map<String,Map<String,SimpleDateFormat>> allFormats = LOCAL_DATE_FORMATS.get();
    if(allFormats.get(timeZone) == null) {
      allFormats.put(timeZone, new HashMap<String,SimpleDateFormat>());
    }
    Map<String,SimpleDateFormat> timeZoneFormats = allFormats.get(timeZone);
    if(timeZoneFormats.get(pattern.get()) == null) {
      timeZoneFormats.put(pattern.get(), createLocalDateFormat(pattern, timeZone));
    }
    
    return timeZoneFormats.get(pattern.get());
  }

  private static Date parseDate(String stringDate, DatePattern pattern, String timeZone) throws ParseException
  {
    Date result = null;

    SimpleDateFormat dateFormat = getLocalDateFormat(pattern, timeZone);
    if (stringDate != null && stringDate.trim().length() > 0)
      {
        result = dateFormat.parse(stringDate.trim());
      }
    
    return result;
  }
  
  private static String formatDate(Date date, DatePattern pattern, String timeZone)
  {
    SimpleDateFormat dateFormat = getLocalDateFormat(pattern, timeZone);
    return (date != null) ? dateFormat.format(date) : null;
  }

  /*****************************************
  *
  * public date formatters
  *
  *****************************************/
  public static String formatDateDay(Date date, String timeZone) { return formatDate(date, DatePattern.LOCAL_DAY, timeZone); }
  public static String formatDateForREST(Date date, String timeZone) { return formatDate(date, DatePattern.REST_UNIVERSAL_TIMESTAMP_DEFAULT, timeZone); }
  
  /**
   * We will display all dates in the same time-zone in Elasticsearch (the "common" one that act as a default)
   * 
   * REMINDER that display format in Elasticsearch is only for debug purpose.
   * Because readers from Elasticsearch (datacubes, rapports, Grafana) exploit the date object (extracted from long/parsed) and not the display string.
   * The only purpose of this display is to be read by users that investigate directly inside Elasticsearch documents (from Elasticsearch Head plugin for instance).
   */
  private static String formatDateForElasticsearch(Date date, String timeZone) { return formatDate(date, DatePattern.ELASTICSEARCH_UNIVERSAL_TIMESTAMP, timeZone); }
  public static String formatDateForElasticsearchDefault(Date date) { return formatDateForElasticsearch(date, Deployment.getDefault().getTimeZone()); }
  
  /*****************************************
  *
  * public date parsers
  *
  *****************************************/
  // Here we will need a parser that does not depend on Deployment settings (such as time-zone)
  // Mainly to parse dates from Deployment.json (to avoid circular dependency) 
  //
  // /!\ Most of the date format are TIME-ZONE DEPENDENT, but here we will only use universal date formats. 
  // Therefore they can be translated into a date without any time-zone consideration. 
  // It is usually UTC date format, or a date format with a fix offset with UTC (e.g. +0200).
  //
  // Therefore, we do NOT NEED to specify any time-zone in the following SimpleDateFormat 
  //
  // !REMINDER: Usually we should NEVER use SimpleDateFormat without time-zone, but here, because 
  // we will only PARSE string to date, and never display them, we can omit that.
  //
  // !REMINDER: SimpleDateFormat is not threadsafe. In order to avoid instantiating the same object
  // again an again we use a ThreadLocal static variable.
  //
  // This variable is PRIVATE and should never be used outside RLMDateUtils and for anything 
  // else but parsing strings.
  
  private static final ThreadLocal<List<SimpleDateFormat>> REST_UNIVERSAL_DATE_FORMATS = ThreadLocal.withInitial(()->{
    List<SimpleDateFormat> result = new ArrayList<SimpleDateFormat>();
    result.add(new SimpleDateFormat(DatePattern.REST_UNIVERSAL_TIMESTAMP_DEFAULT.get()));
    result.add(new SimpleDateFormat(DatePattern.REST_UNIVERSAL_TIMESTAMP_ALTERNATE.get()));
    return result;
  });
  
  public static final Date parseDateFromREST(String stringDate) throws ParseException {
    List<SimpleDateFormat> formats = REST_UNIVERSAL_DATE_FORMATS.get();
    Date result = null;
    ParseException parseException = null;

    if (stringDate == null || stringDate.trim().length() <= 0) {
      return null;
    }
    String input = stringDate.trim();
    
    for (SimpleDateFormat format : formats)
      {
        try
          {
            result = format.parse(input);
            parseException = null;
          }
        catch (ParseException e)
          {
            result = null;
            parseException = e;
          }
        
        if (result != null) {
          break;
        }
      }
    
    if (parseException != null)
      {
        throw parseException;
      }
    
    return result;
  }
  
  // Here we re-use the default SimpleDateFormat for ES. But we could also do something similar to REST if a circular dependency appear.
  public static Date parseDateFromElasticsearch(String stringDate) throws ParseException { return parseDate(stringDate, DatePattern.ELASTICSEARCH_UNIVERSAL_TIMESTAMP, Deployment.getDefault().getTimeZone()); }

  public static Date parseDateFromDay(String stringDate, String timeZone) throws ParseException { return parseDate(stringDate, DatePattern.LOCAL_DAY, timeZone); }
  
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
    int firstDayOfTheWeekInt = Deployment.getFirstDayOfTheWeek();
    Calendar calendar = getCalendarInstance1(timeZone);
    calendar.setTime(date);
    Calendar result;
    switch (field)
      {
        case Calendar.DAY_OF_WEEK:
          Calendar day = DateUtils.ceiling(calendar,Calendar.DATE);
          while (day.get(Calendar.DAY_OF_WEEK) != firstDayOfTheWeekInt) day.add(Calendar.DATE,1);
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
    int firstDayOfTheWeekInt = Deployment.getFirstDayOfTheWeek();
    Calendar calendar = getCalendarInstance1(timeZone);
    calendar.setTime(date);
    Calendar result;
    switch (field)
      {
        case Calendar.DAY_OF_WEEK:
          Calendar day = DateUtils.truncate(calendar,Calendar.DATE);
          while (day.get(Calendar.DAY_OF_WEEK) != firstDayOfTheWeekInt) day.add(Calendar.DATE,-1);
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
