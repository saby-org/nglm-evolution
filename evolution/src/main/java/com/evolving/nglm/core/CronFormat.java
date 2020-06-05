/*****************************************************************************
*
*  CronFormat.java
*
*  Copyright 2000-2009 RateIntegration, Inc.  All Rights Reserved.
*
*****************************************************************************/

package com.evolving.nglm.core;

import java.util.*;

public class CronFormat
{
  /*
   * cron format string:
   *
   * minute (0-59)
   * hour (0-23)
   * day of the month (1-31) (normalized to 0-30 for computations)
   * month of the year (1-12:JAN-DEC case-insensitive) (normalized to 0-11 for computations)
   * day of the week (0-7:MON-SUN case-insensitive) 1 is always Monday (7 is normalized to 0 for computations)
   */

  //
  //  indexes
  //

  private static final int MIN_INDEX = 0;
  private static final int MINUTE_INDEX = 0;
  private static final int HOUR_INDEX = 1;
  private static final int DAY_INDEX = 2;
  private static final int MONTH_INDEX = 3;
  private static final int YEAR_INDEX = 4;
  private static final int MAX_INDEX = 4;

  //
  //  maps
  //

  private static final Map dayToNumber = new HashMap();
  private static final Map monthToNumber = new HashMap();

  //
  //  dayToNumber
  //

  static
  {
    dayToNumber.put("SUN", new Integer(0));
    dayToNumber.put("MON", new Integer(1));
    dayToNumber.put("TUE", new Integer(2));
    dayToNumber.put("WED", new Integer(3));
    dayToNumber.put("THU", new Integer(4));
    dayToNumber.put("FRI", new Integer(5));
    dayToNumber.put("SAT", new Integer(6));
  }

  //
  //  monthToNumber
  //

  static
  {
    monthToNumber.put("JAN", new Integer(1));
    monthToNumber.put("FEB", new Integer(2));
    monthToNumber.put("MAR", new Integer(3));
    monthToNumber.put("APR", new Integer(4));
    monthToNumber.put("MAY", new Integer(5));
    monthToNumber.put("JUN", new Integer(6));
    monthToNumber.put("JUL", new Integer(7));
    monthToNumber.put("AUG", new Integer(8));
    monthToNumber.put("SEP", new Integer(9));
    monthToNumber.put("OCT", new Integer(10));
    monthToNumber.put("NOV", new Integer(11));
    monthToNumber.put("DEC", new Integer(12));
  }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private boolean daysOfWeekAsterisk = false;
  private boolean daysOfMonthAsterisk = false;
  private boolean hoursAsterisk = false;
  private TreeSet<Integer> daysOfMonth;
  private TreeSet<Integer> daysOfWeek;
  private TreeSet<Integer> minutes;
  private TreeSet<Integer> hours;
  private TreeSet<Integer> months;
  private String cronEntryString;
  private TimeZone timeZone;
  private RangeSpecifications rangeSpecs = new RangeSpecifications();

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public CronFormat(String cronEntryString) throws com.evolving.nglm.core.utilities.UtilitiesException
  {
    this(cronEntryString, TimeZone.getDefault());
  }
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public CronFormat(String cronEntryString, TimeZone timeZone) throws com.evolving.nglm.core.utilities.UtilitiesException
  {
    //
    //  arguments
    //

    this.cronEntryString = cronEntryString;
    this.timeZone = timeZone;

    //
    //  parse cron string
    //

    parse();

    //
    // cannot set days and years here.
    // those ranges are a function of the date that is passed to the next() method.
    //

    rangeSpecs.set(MONTH_INDEX, months);
    rangeSpecs.set(HOUR_INDEX, hours);
    rangeSpecs.set(MINUTE_INDEX, minutes);
  }

  /*****************************************
  *
  *  setTimeZone
  *
  *****************************************/

  public void setTimeZone(TimeZone timeZone)
  {
    this.timeZone = timeZone;
  }
  
  /*****************************************
  *
  *  next
  *
  *****************************************/

  public Date next()
  {
    Date now = SystemTime.getCurrentTime();
    return next(now);
  }

  /*****************************************
  *
  *  next
  *
  *****************************************/

  public Date next(Date date)
  {
    GregorianCalendar calendar = new GregorianCalendar(timeZone);
    calendar.setTime(date);
    calendar.set(Calendar.MILLISECOND,0);

    TreeSet<Integer> years = new TreeSet<Integer>();
    years.add(new Integer(calendar.get(Calendar.YEAR)));
    years.add(new Integer(calendar.get(Calendar.YEAR) + 1));
    rangeSpecs.set(YEAR_INDEX, years);

    Integer [] resultVector = new Integer[5];
    resultVector[YEAR_INDEX] = new Integer(calendar.get(Calendar.YEAR));
    resultVector[MONTH_INDEX] = new Integer(calendar.get(Calendar.MONTH));
    resultVector[DAY_INDEX] = new Integer(calendar.get(Calendar.DAY_OF_MONTH) - 1); // normalize so first day is 0 not 1
    resultVector[HOUR_INDEX] = new Integer(calendar.get(Calendar.HOUR_OF_DAY));
    resultVector[MINUTE_INDEX] = new Integer(calendar.get(Calendar.MINUTE));

    nextRecursive(MAX_INDEX,resultVector,rangeSpecs,false);

    int intyear = resultVector[YEAR_INDEX].intValue();
    int intmonth = resultVector[MONTH_INDEX].intValue();
    int intday = resultVector[DAY_INDEX].intValue() + 1; // convert back to java days that start with 1 not 0
    int inthour = resultVector[HOUR_INDEX].intValue();
    int intminute = resultVector[MINUTE_INDEX].intValue();
    
    calendar.set(intyear,intmonth,intday,inthour,intminute,0);

    GregorianCalendar calendarPreviousHour = new GregorianCalendar(timeZone);
    calendarPreviousHour.set(intyear,intmonth,intday,inthour,intminute,0);
    calendarPreviousHour.add(Calendar.HOUR_OF_DAY,-1);

    //
    //  values to check if we are in a dst transition
    //
    
    GregorianCalendar dst = new GregorianCalendar(timeZone);
    dst.set(intyear,intmonth,intday,inthour,intminute,0);
    dst.set(Calendar.MILLISECOND,0);
    dst.add(Calendar.DAY_OF_MONTH,-1);
    boolean inDSTBefore = dst.get(Calendar.DST_OFFSET) > 0;
    dst.add(Calendar.DAY_OF_MONTH,2);
    boolean inDSTAfter = dst.get(Calendar.DST_OFFSET) > 0;
    
    
    //
    // the following tries to handle daylight saving time issues
    // that java has with its calendar class.
    //
    
    int delta = calendar.get(Calendar.HOUR_OF_DAY);

    boolean transitionDSTtoST = calendarPreviousHour.get(Calendar.DST_OFFSET) > 0 && calendar.get(Calendar.DST_OFFSET) == 0; 

    //
    // checking if we are going from daylight saving to standard time
    // (this is the case where we fall back in time).  If we can subtract
    // one hour and the time is still greater than the date that we
    // sent it (+ a minute) and the hour is in our range spec then we
    // should should roll the time back one hour.
    //

    if ((hoursAsterisk || transitionDSTtoST) && calendarPreviousHour.getTime().getTime() > date.getTime() + 60*1000)
      {
        calendar = calendarPreviousHour;
      }
    return calendar.getTime();
  }

  /*****************************************
  *
  *  previous
  *
  *****************************************/

  public Date previous()
  {
    Date now = SystemTime.getCurrentTime();
    return previous(now);
  }

  /*****************************************
  *
  *  previous
  *
  *****************************************/

  public Date previous(Date date)
  {
    GregorianCalendar calendar = new GregorianCalendar(timeZone);
    calendar.setTime(date);
    calendar.set(Calendar.MILLISECOND,0);

    TreeSet<Integer> years = new TreeSet<Integer>();
    years.add(new Integer(calendar.get(Calendar.YEAR)));
    years.add(new Integer(calendar.get(Calendar.YEAR) - 1));
    rangeSpecs.set(YEAR_INDEX, years);

    //
    //  values to check if we are in a dst transition
    //
    
    GregorianCalendar dst = (GregorianCalendar) calendar.clone();
    boolean inDSTNow = (dst.get(Calendar.DST_OFFSET) > 0);
    dst.set(Calendar.HOUR_OF_DAY,0);
    dst.set(Calendar.MILLISECOND,0);
    boolean inDSTBefore = (dst.get(Calendar.DST_OFFSET) > 0);
    dst.add(Calendar.DAY_OF_MONTH,1);
    boolean inDSTAfter =  (dst.get(Calendar.DST_OFFSET) > 0);

    Integer [] resultVector = new Integer[5];
    resultVector[YEAR_INDEX] = new Integer(calendar.get(Calendar.YEAR));
    resultVector[MONTH_INDEX] = new Integer(calendar.get(Calendar.MONTH));
    resultVector[DAY_INDEX] = new Integer(calendar.get(Calendar.DAY_OF_MONTH) - 1); // normalize so first day is 0 not 1
    if ((!inDSTBefore) && inDSTNow)
      resultVector[HOUR_INDEX] = new Integer(calendar.get(Calendar.HOUR_OF_DAY)-1);
    else if (hoursAsterisk && (!inDSTNow) && inDSTBefore)
      resultVector[HOUR_INDEX] = new Integer(calendar.get(Calendar.HOUR_OF_DAY)+1);
    else
      resultVector[HOUR_INDEX] = new Integer(calendar.get(Calendar.HOUR_OF_DAY));
    resultVector[MINUTE_INDEX] = new Integer(calendar.get(Calendar.MINUTE));

    nextRecursive(MAX_INDEX,resultVector,rangeSpecs,true);

    int intyear = resultVector[YEAR_INDEX].intValue();
    int intmonth = resultVector[MONTH_INDEX].intValue();
    int intday = resultVector[DAY_INDEX].intValue() + 1; // convert back to java days that start with 1 not 0
    int inthour = resultVector[HOUR_INDEX].intValue();
    int intminute = resultVector[MINUTE_INDEX].intValue();
    
    calendar.set(intyear,intmonth,intday,inthour,intminute,0);
    calendar.set(Calendar.MILLISECOND,0);

    GregorianCalendar calendarPreviousHour = new GregorianCalendar(timeZone);
    calendarPreviousHour.set(intyear,intmonth,intday,inthour,intminute,0);
    calendarPreviousHour.set(Calendar.MILLISECOND,0);
    calendarPreviousHour.add(Calendar.HOUR_OF_DAY,-1);

    //
    //  values to check if we are in a dst transition
    //

    dst = new GregorianCalendar(timeZone);
    dst.set(intyear,intmonth,intday,inthour,intminute,0);
    dst.set(Calendar.MILLISECOND,0);
    dst.add(Calendar.DAY_OF_MONTH,-1);
    inDSTBefore = dst.get(Calendar.DST_OFFSET) > 0;
    dst.add(Calendar.DAY_OF_MONTH,2);
    inDSTAfter = dst.get(Calendar.DST_OFFSET) > 0;
    
    //
    // the following tries to handle daylight saving time issues
    // that java has with its calendar class.
    //
    
    int delta = calendar.get(Calendar.HOUR_OF_DAY);

    boolean transitionDSTtoST = (calendarPreviousHour.get(Calendar.DST_OFFSET) > 0) && (calendar.get(Calendar.DST_OFFSET) == 0); 

    //
    // checking if we are going from daylight saving to standard time
    // (this is the case where we fall back in time).
    //
    
    if (transitionDSTtoST && ! hoursAsterisk)
      {
        calendar = calendarPreviousHour;
      }

    if (calendar.getTime().getTime() + 60*1000 > date.getTime())
      {
        calendar.add(Calendar.HOUR_OF_DAY,-1);
      }

    return calendar.getTime();
  }

  /*****************************************
  *
  *  indexToJavaCalendarConstant
  *
  *****************************************/

  private int indexToJavaCalendarConstant(int index)
  {
    switch (index)
      {
        case MINUTE_INDEX: return Calendar.MINUTE;
        case HOUR_INDEX:   return Calendar.HOUR_OF_DAY;
        case DAY_INDEX:    return Calendar.DAY_OF_MONTH;
        case MONTH_INDEX:  return Calendar.MONTH;
        case YEAR_INDEX:   return Calendar.YEAR;
        default:           throw new com.evolving.nglm.core.utilities.UtilitiesRuntimeException("cron index out of range: " + index);
      }
  }

  /*****************************************
  *
  *  nextRecursive
  *
  *****************************************/

  private boolean nextRecursive(int index, Integer [] resultVector, RangeSpecifications rangeSpecs, boolean previous)
  {
    boolean propagateCarry;

    if (index > MAX_INDEX || index < MIN_INDEX) throw new com.evolving.nglm.core.utilities.UtilitiesRuntimeException("cron index out of range: " + index);
        
    SortedSet<Integer> validValues = rangeSpecs.get(index,resultVector);
    Integer initialValue = resultVector[index];
    SortedSet<Integer> tail = (previous) ? validValues.headSet(new Integer(initialValue.intValue() + 1)) : validValues.tailSet(initialValue);
    
    //
    // Exact match for this value, continue recursion searching for the first non-match
    //

    if (tail.contains(initialValue))
      {
        //
        // Detect the base case of the recursion, generate a psuedo carry.
        // This allows an exact match to return the next time, not itself.
        //

        boolean childCarry = (index == MIN_INDEX) ? true : nextRecursive(index - 1, resultVector, rangeSpecs, previous);
        if (childCarry)
          {
            //
            // This value overflows, propagate carry
            //

            if (tail.size() == 1) { propagateCarry = true; }

            //
            // This value does not overflow, carry propagation stops here
            //

            else
              {
                Object [] array = tail.toArray();
                resultVector[index] = previous ? (Integer) array[tail.size()- 2] : (Integer) array[1];
                if (index > MIN_INDEX) { resetLowOrder(index - 1, resultVector, rangeSpecs, previous); }
                propagateCarry = false;
              }
          }

        //
        // No carry from child *and* this value is an exact match, no reason to begin propagating a carry
        //

        else
          {
            propagateCarry = false;
          }
      }

    //
    // Increment without overflow, no carry
    //

    else if (tail.size() > 0)
      {
        resultVector[index] = previous ? (Integer) tail.last() : (Integer) tail.first();
        if (index > MIN_INDEX) { resetLowOrder(index - 1, resultVector, rangeSpecs, previous); }
        propagateCarry = false;
      }

    //
    // No exact match and no tailSet members match so propagate a carry
    //

    else
      {
        propagateCarry = true;
      }
    return propagateCarry;
  }

  /*****************************************
  *
  *  resetLowOrder
  *
  *****************************************/

  private void resetLowOrder(int index, Integer [] resultVector, RangeSpecifications rangeSpecs, boolean previous)
  {
    try
      { 
        for (int i = index; i >= MIN_INDEX; --i)
          {
            SortedSet<Integer> set = rangeSpecs.get(i,resultVector);
            resultVector[i] = previous ? (Integer) set.last() : (Integer) set.first();
          }
      }
    catch (ArrayIndexOutOfBoundsException e)
      {
        throw new com.evolving.nglm.core.utilities.UtilitiesRuntimeException(e.getMessage(), e);
      }
  }

  /*****************************************
  *
  *  cronDayOfWeekToJavaDayOfWeek
  *
  *****************************************/

  private int cronDayOfWeekToJavaDayOfWeek(int cronDayOfWeek)
  {
    //
    // Note both 0 and 7 are Sunday
    //

    switch (cronDayOfWeek)
      {
        case 0: return Calendar.SUNDAY;
        case 1: return Calendar.MONDAY;
        case 2: return Calendar.TUESDAY;
        case 3: return Calendar.WEDNESDAY;
        case 4: return Calendar.THURSDAY;
        case 5: return Calendar.FRIDAY;
        case 6: return Calendar.SATURDAY;
        case 7: return Calendar.SUNDAY;
        default: throw new com.evolving.nglm.core.utilities.UtilitiesRuntimeException("cron index out of range: " + cronDayOfWeek);
      }
  }

  /*****************************************
  *
  *  getFirstDayOfMonthOfDayOfWeek
  *
  *****************************************/

  private int getFirstDayOfMonthOfDayOfWeek(GregorianCalendar calendar, int targetDayOfWeek)
  {
    //
    // Calculate the day that is the first occurrence of a particular day of the week.
    //
    
    GregorianCalendar duplicateCalendar = (GregorianCalendar) calendar.clone();
    int firstOccurrence = -1;

    for (int i = 1; i <= 7; ++i)
      {
        duplicateCalendar.set(Calendar.DAY_OF_MONTH,i);
        int dayOfWeek = duplicateCalendar.get(Calendar.DAY_OF_WEEK);
        if (dayOfWeek == targetDayOfWeek)
          {
            firstOccurrence = i;
            break;
          }
      }

    if (firstOccurrence == -1)
      {
        int month = duplicateCalendar.get(Calendar.MONTH);
        int year = duplicateCalendar.get(Calendar.YEAR);
        throw new com.evolving.nglm.core.utilities.UtilitiesRuntimeException("cron index out of range: " + Integer.toString(targetDayOfWeek)+"-"+Integer.toString(month)+"-"+Integer.toString(year));
      }

    return firstOccurrence;
  }

  /*****************************************
  *
  *  getLastDayOfMonthOfDayOfWeek
  *
  *****************************************/

  private int getLastDayOfMonthOfDayOfWeek(GregorianCalendar calendar, int targetDayOfWeek)
  {
    //
    // Calculate the day that is the last occurrence of a particular day of the week.
    //
    
    GregorianCalendar duplicateCalendar = (GregorianCalendar) calendar.clone();
    int lastOccurrence = -1;

    int greatest = duplicateCalendar.getActualMaximum(Calendar.DAY_OF_MONTH);
    int least = greatest - 6;
    
    for (int i = greatest; i >= least; --i)
      {
        duplicateCalendar.set(Calendar.DAY_OF_MONTH,i);
        int dayOfWeek = duplicateCalendar.get(Calendar.DAY_OF_WEEK);
        if (dayOfWeek == targetDayOfWeek)
          {
            lastOccurrence = i;
            break;
          }
      }

    if (lastOccurrence == -1)
      {
        int month = duplicateCalendar.get(Calendar.MONTH);
        int year = duplicateCalendar.get(Calendar.YEAR);
        throw new com.evolving.nglm.core.utilities.UtilitiesRuntimeException("cron index out of range: " + Integer.toString(targetDayOfWeek)+"-"+Integer.toString(month)+"-"+Integer.toString(year));
      }

    return lastOccurrence;
  }

  /*****************************************
  *
  *  computeDays
  *
  *****************************************/

  private SortedSet<Integer> computeDays(Integer yearInt, Integer monthInt)
  {
    int year = yearInt.intValue();
    int month = monthInt.intValue();

    TreeSet<Integer> days = (TreeSet<Integer>) daysOfMonth.clone();

    GregorianCalendar calendar = new GregorianCalendar(timeZone);

    calendar.set(Calendar.YEAR, year);
    calendar.set(Calendar.MONTH, month);
    calendar.set(Calendar.DAY_OF_MONTH, 1);

    Iterator iterator = daysOfWeek.iterator();
    while (iterator.hasNext())
      {
        Integer cronDayOfWeek = (Integer) iterator.next();
        int javaDayOfWeek = cronDayOfWeekToJavaDayOfWeek(cronDayOfWeek.intValue());

        int first = getFirstDayOfMonthOfDayOfWeek(calendar,javaDayOfWeek);
        int last = getLastDayOfMonthOfDayOfWeek(calendar,javaDayOfWeek);

        //
        //  Sanity check that first and last make sense
        //

        if ((first >= last) || ((first % 7) != (last % 7)))
          {
            throw new com.evolving.nglm.core.utilities.UtilitiesRuntimeException("cron index out of range: " + Integer.toString(cronDayOfWeek)+"-"+Integer.toString(monthInt));
          }

        //
        // Days are specified in the range of 1-31, normalize to 0-30 by subtracting one
        //

        int firstNormalized = first - 1;
        int lastNormalized = last - 1;
        for (int i = firstNormalized; i <= lastNormalized; i += 7)
          {
            days.add(new Integer(i));
          }
      }

    //
    // Deal with end of the month cases.  Round down any day above the largest day of the month to the largest day of the month.
    // Example: Feb 31st -> Feb 28th (or Feb 29th in leap years)
    //

    //
    // Normalize days by subtracting one
    //

    int maxDaysInAnyMonth = calendar.getMaximum(Calendar.DAY_OF_MONTH) - 1; // 31 - 1
    int maxDaysThisMonth  = calendar.getActualMaximum(Calendar.DAY_OF_MONTH) - 1;
    
    for (int i = maxDaysThisMonth + 1; i <= maxDaysInAnyMonth; ++i )
      {
        if (days.contains(new Integer(i))) { days.add(new Integer(maxDaysThisMonth)); }
        days.remove(new Integer(i));
      }
    return days;
  }

  /*****************************************
  *
  *  parse
  *
  *****************************************/

  /*
   *  ws ::= space | tab
   *  cronentry ::= ws* element ws* element ws* element ws* element ws* element ws*
   *  element ::= * | list
   *  list ::= range(,range)*
   *  range ::= number | number-number
   *  number ::= month | day | [0-9]*
   *  month ::= [JAN-DEC]
   *  day ::= [MON-SUN]
   */

  private void parse() throws com.evolving.nglm.core.utilities.UtilitiesException
  {
    StringTokenizer tokenizer = new StringTokenizer(cronEntryString, " \t");
    int tokenCount = tokenizer.countTokens();
    if (tokenCount != 5) throw new com.evolving.nglm.core.utilities.UtilitiesException("cron token number incorrect: " + cronEntryString);

    ParseResult parseResult;

    parseResult = parseElement(tokenizer.nextToken(),0,59,null,0);
    minutes = parseResult.set;

    parseResult = parseElement(tokenizer.nextToken(),0,23,null,0);
    hours = parseResult.set;
    hoursAsterisk = parseResult.asterisk;
    
    parseResult = parseElement(tokenizer.nextToken(),1,31,null,-1);
    daysOfMonth = parseResult.set;
    daysOfMonthAsterisk = parseResult.asterisk;

    parseResult = parseElement(tokenizer.nextToken(),1,12,monthToNumber,-1);
    months = parseResult.set;

    parseResult = parseElement(tokenizer.nextToken(),0,7,dayToNumber,0);
    daysOfWeek = parseResult.set;
    daysOfWeekAsterisk = parseResult.asterisk;

    //
    // Our calculations use only 0-6 for day of the week, if 7 was given normalize it to 0
    //

    Integer integer7 = new Integer(7);
    if (daysOfWeek.contains(integer7))
      {
        daysOfWeek.remove(integer7);
        daysOfWeek.add(new Integer(0));
      }

    if (  daysOfWeekAsterisk && ! daysOfMonthAsterisk) { daysOfWeek  = new TreeSet<Integer>(); }
    if (! daysOfWeekAsterisk &&   daysOfMonthAsterisk) { daysOfMonth = new TreeSet<Integer>(); }
  }

  /*****************************************
  *
  *  parseElement
  *
  *****************************************/

  private ParseResult parseElement(String elementString, int validMin, int validMax, Map mnemonic, int normalize) throws com.evolving.nglm.core.utilities.UtilitiesException
  {
    StringTokenizer tokenizer = new StringTokenizer(elementString,",");
    int tokenCount = tokenizer.countTokens();

    ParseResult parseResult = new ParseResult();

    while (tokenizer.hasMoreTokens())
      {
        String rangeString = tokenizer.nextToken();
        if (tokenCount == 1 && rangeString.equals("*"))
          {
            parseResult.asterisk = true;
            int normalizedMin = validMin + normalize;
            int normalizedMax = validMax + normalize;
            for (int i = normalizedMin; i <= normalizedMax; ++i)
              {
                parseResult.set.add(new Integer(i));
              }
            return parseResult;
          }
        else
          {
            SortedSet<Integer> oneRange = parseRange(rangeString,validMin,validMax,mnemonic,normalize);
            parseResult.set.addAll(oneRange);
          }
      }
    return parseResult;
  }

  /*****************************************
  *
  *  parseRange
  *
  *****************************************/

  private SortedSet<Integer> parseRange(String rangeString, int validMin, int validMax, Map mnemonic, int normalize) throws com.evolving.nglm.core.utilities.UtilitiesException
  {
    StringTokenizer tokenizer = new StringTokenizer(rangeString,"-");    
    int tokenCount = tokenizer.countTokens();
    
    if (tokenCount > 2 || tokenCount < 1) throw new com.evolving.nglm.core.utilities.UtilitiesException("cron range values incorrect: " + rangeString);

    String minString = tokenizer.nextToken();

    int min = parseInt(minString,validMin,validMax,mnemonic,normalize);
    int max;

    if (tokenizer.hasMoreTokens())
      {
        String maxString = tokenizer.nextToken();
        max = parseInt(maxString,validMin,validMax,mnemonic,normalize);
      }
    else
      {
        max = min;
      }

    if (min > max)
      {
        throw new com.evolving.nglm.core.utilities.UtilitiesException("cron min greater than max: " + rangeString);
      }
    else if (min < validMin)
      {
        throw new com.evolving.nglm.core.utilities.UtilitiesException("cron min out of bounds: " + rangeString);
      }
    else if (max > validMax)
      {
        throw new com.evolving.nglm.core.utilities.UtilitiesException("cron max out of bounds: " + rangeString);
      }
    
    int normalizedMin = min + normalize;
    int normalizedMax = max + normalize;

    TreeSet<Integer> range = new TreeSet<Integer>();

    for (int i = normalizedMin; i <= normalizedMax; ++i)
      {
        range.add(new Integer(i));
      }

    return range;
  }

  /*****************************************
  *
  *  parseInt
  *
  *****************************************/

  private int parseInt(String intString, int validMin, int validMax, Map mnemonic, int normalize) throws com.evolving.nglm.core.utilities.UtilitiesException
  {
    Integer integer = null;

    try { integer = new Integer(intString); } catch (NumberFormatException nfe) {}

    if (integer == null && mnemonic != null)
      {
        String upperCase = intString.toUpperCase();
        integer = (Integer) mnemonic.get(upperCase);
      }
    if (integer == null) throw new com.evolving.nglm.core.utilities.UtilitiesException("cron invalid value: " + intString);

    int result = integer.intValue();
    return result;
  }

  /*****************************************
  *
  *  class ParseResult
  *
  *****************************************/

  private class ParseResult
  {
    private TreeSet<Integer> set = new TreeSet<Integer>();
    private boolean asterisk = false;
  }
  
  /*****************************************
  *
  *  class RangeSpecifications
  *
  *****************************************/

  private class RangeSpecifications
  {
    Map sets = new HashMap();

    public SortedSet<Integer> get(int index, Integer [] resultVector)
    {
      SortedSet<Integer> resultSet;
      if (index > MAX_INDEX || index < MIN_INDEX)
        {
          throw new com.evolving.nglm.core.utilities.UtilitiesRuntimeException("cron index out of range: " + index);
        }
      else if (index == DAY_INDEX)
        {
          resultSet = computeDays(resultVector[YEAR_INDEX],resultVector[MONTH_INDEX]);
        }
      else
        {
          resultSet = (SortedSet<Integer>) sets.get(new Integer(index));
        }
      return resultSet;
    }
    
    public void set(int index, SortedSet<Integer> set)
    {
      sets.put(new Integer(index),set);
    }
  }

  /*****************************************
  *
  *  toString
  *
  *****************************************/

  public String toString() { return cronEntryString; }

  /*****************************************
  *
  *  equals
  *
  *****************************************/

  public boolean equals(Object obj)
  {
    boolean result = false;
    if (obj instanceof CronFormat)
      {
        CronFormat cronFormat = (CronFormat) obj;
        result = cronFormat.cronEntryString.equals(this.cronEntryString);
      }
    return result;
  }

  /*****************************************
  *
  *  hashCode
  *
  *****************************************/

  public int hashCode() { return cronEntryString.hashCode(); }
}
