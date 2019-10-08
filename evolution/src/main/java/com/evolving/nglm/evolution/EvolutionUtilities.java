/*****************************************************************************
*
*  EvolutionUtilities.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.evolution.EvolutionUtilities.TimeUnit;

import java.util.Calendar;
import java.util.Date;

public class EvolutionUtilities
{
  /*****************************************
  *
  *  enum
  *
  *****************************************/

  public enum TimeUnit
  {
    Instant("instant", "MILLIS"),
    Minute("minute", "MINUTES"),
    Hour("hour", "HOURS"),
    Day("day", "DAYS"),
    Week("week", "WEEKS"),
    Month("month", "MONTHS"),
    Year("year", "YEARS"),
    Unknown("(unknown)", "(unknown)");
    private String externalRepresentation;
    private String chronoUnit;
    private TimeUnit(String externalRepresentation, String chronoUnit) { this.externalRepresentation = externalRepresentation; this.chronoUnit = chronoUnit; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public String getChronoUnit() { return chronoUnit; }
    public static TimeUnit fromExternalRepresentation(String externalRepresentation) { for (TimeUnit enumeratedValue : TimeUnit.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }

  /*****************************************
  *
  *  addTime
  *
  *****************************************/

  public static Date addTime(Date baseTime, int amount, TimeUnit timeUnit, String timeZone, boolean roundUp)
  {
    Date result = baseTime;
    switch (timeUnit)
      {
        case Minute:
          if (roundUp) result = RLMDateUtils.ceiling(result, Calendar.MINUTE, Calendar.SUNDAY, timeZone);
          result = RLMDateUtils.addMinutes(result, amount);
          break;

        case Hour:
          if (roundUp) result = RLMDateUtils.ceiling(result, Calendar.HOUR, Calendar.SUNDAY, timeZone);
          result = RLMDateUtils.addHours(result, amount);
          break;

        case Day:
          if (roundUp) result = RLMDateUtils.ceiling(result, Calendar.DATE, Calendar.SUNDAY, timeZone);
          result = RLMDateUtils.addDays(result, amount, timeZone);
          break;

        case Week:
          if (roundUp) result = RLMDateUtils.ceiling(result, Calendar.DAY_OF_WEEK, Calendar.SUNDAY, timeZone);
          result = RLMDateUtils.addWeeks(result, amount, timeZone);
          break;

        case Month:
          if (roundUp) result = RLMDateUtils.ceiling(result, Calendar.MONTH, Calendar.SUNDAY, timeZone);
          result = RLMDateUtils.addMonths(result, amount, timeZone);
          break;

        case Year:
          if (roundUp) result = RLMDateUtils.ceiling(result, Calendar.YEAR, Calendar.SUNDAY, timeZone);
          result = RLMDateUtils.addYears(result, amount, timeZone);
          break;

        default:
          throw new RuntimeException("unsupported timeunit: " + timeUnit);
      }
    return result;
  }

  //
  //  addTime
  //

  public static Date addTime(Date baseTime, int amount, TimeUnit timeUnit, String timeZone)
  {
    return addTime(baseTime, amount, timeUnit, timeZone, false);
  }
  
  //
  // isDateBetween
  //
  
  public static boolean isDateBetween(Date now, Date from, Date until)
  {
    return now.after(from) && now.before(until);
  }
}
