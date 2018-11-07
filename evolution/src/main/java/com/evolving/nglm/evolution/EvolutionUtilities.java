/*****************************************************************************
*
*  EvolutionUtilities.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.evolution.EvaluationCriterion.TimeUnit;

import java.util.Calendar;
import java.util.Date;

public class EvolutionUtilities
{
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
}
