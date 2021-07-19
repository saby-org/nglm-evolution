/*****************************************************************************
*
*  EvolutionUtilities.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Period;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.RLMDateUtils;

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
    Second("second", "SECONDS"),
    Minute("minute", "MINUTES"),
    Hour("hour", "HOURS"),
    Day("day", "DAYS"),
    Week("week", "WEEKS"),
    Month("month", "MONTHS"),
    Year("year", "YEARS"),
    Quarter("quarter", "QUARTERS"),
    Unknown("(unknown)", "(unknown)");
    private String externalRepresentation;
    private String chronoUnit;
    private TimeUnit(String externalRepresentation, String chronoUnit) { this.externalRepresentation = externalRepresentation; this.chronoUnit = chronoUnit; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public String getChronoUnit() { return chronoUnit; }
    public static TimeUnit fromExternalRepresentation(String externalRepresentation) { for (TimeUnit enumeratedValue : TimeUnit.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }

  public enum RoundingSelection
  {
    RoundUp("roundUp"),
    RoundDown("roundDown"),
    NoRound("noRound"),
    Unknown("unknown");
    private String externalRepresentation;
    private RoundingSelection(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static RoundingSelection fromExternalRepresentation(String externalRepresentation) { for (RoundingSelection enumeratedValue : RoundingSelection.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }

  /*****************************************
  *
  *  addTime
  *
  *****************************************/

  public static Date addTime(Date baseTime, int amount, TimeUnit timeUnit, String timeZone, RoundingSelection roundingSelection)
  {
    Date result = baseTime;
    switch (timeUnit)
      {
        case Instant:
          switch (roundingSelection) {
          case RoundUp:
            result = RLMDateUtils.ceiling(result, Calendar.MILLISECOND, timeZone);
            break;
          case RoundDown:
            result = RLMDateUtils.truncate(result, Calendar.MILLISECOND, timeZone);
            break;
          default :
            break;
          }
          result = RLMDateUtils.addMilliseconds(result, amount);
        break;

        case Minute:
          switch (roundingSelection) {
          case RoundUp:
            result = RLMDateUtils.ceiling(result, Calendar.MINUTE, timeZone);
            break;
          case RoundDown:
            result = RLMDateUtils.truncate(result, Calendar.MINUTE, timeZone);
            break;
          default :
            break;
          }
          result = RLMDateUtils.addMinutes(result, amount);
          break;

        case Hour:
          switch (roundingSelection) {
          case RoundUp:
            result = RLMDateUtils.ceiling(result, Calendar.HOUR, timeZone);
            break;
          case RoundDown:
            result = RLMDateUtils.truncate(result, Calendar.HOUR, timeZone);
            break;
          default :
            break;
          }
          result = RLMDateUtils.addHours(result, amount);
          break;

        case Day:
          switch (roundingSelection) {
          case RoundUp:
            result = RLMDateUtils.ceiling(result, Calendar.DATE, timeZone);
            break;
          case RoundDown:
            result = RLMDateUtils.truncate(result, Calendar.DATE, timeZone);
            break;
          default :
            break;
          }
          result = RLMDateUtils.addDays(result, amount, timeZone);
          break;

        case Week:
          switch (roundingSelection) {
          case RoundUp:
            result = RLMDateUtils.ceiling(result, Calendar.DAY_OF_WEEK, timeZone);
            break;
          case RoundDown:
            result = RLMDateUtils.truncate(result, Calendar.DAY_OF_WEEK, timeZone);
            break;
          default :
            break;
          }
          result = RLMDateUtils.addWeeks(result, amount, timeZone);
          break;

        case Month:
          switch (roundingSelection) {
          case RoundUp:
            result = RLMDateUtils.ceiling(result, Calendar.MONTH, timeZone);
            break;
          case RoundDown:
            result = RLMDateUtils.truncate(result, Calendar.MONTH, timeZone);
            break;
          default :
            break;
          }
          result = RLMDateUtils.addMonths(result, amount, timeZone);
          break;

        case Year:
          switch (roundingSelection) {
          case RoundUp:
            result = RLMDateUtils.ceiling(result, Calendar.YEAR, timeZone);
            break;
          case RoundDown:
            result = RLMDateUtils.truncate(result, Calendar.YEAR, timeZone);
            break;
          default :
            break;
          }
          result = RLMDateUtils.addYears(result, amount, timeZone);
          break;
          
        case Quarter:
          switch (roundingSelection) {
            case RoundUp:
              result = RLMDateUtils.ceiling(result, Calendar.MONTH, timeZone);
              break;
            case RoundDown:
              result = RLMDateUtils.truncate(result, Calendar.MONTH, timeZone);
              break;
            default :
              break;
            }
          int monthsToAdd = 3 * amount;
          result = RLMDateUtils.addMonths(result, monthsToAdd, timeZone);
          break;

        default:
          throw new RuntimeException("unsupported timeunit: " + timeUnit);
      }
    return result;
  }

  /*****************************************
  *
  *  addTime
  *
  *****************************************/

  public static Date addTime(Date baseTime, int amount, TimeUnit timeUnit, String timeZone)
  {
    return addTime(baseTime, amount, timeUnit, timeZone, RoundingSelection.NoRound);
  }

  // this take care of the TimeZone, ie: day time saving
  public static Date removeTime(Date baseTime, Period toRemove, int tenantID)
  {
    return new Date(baseTime.toInstant().atZone(Deployment.getDeployment(tenantID).getZoneId()).minus(toRemove).toInstant().toEpochMilli());
  }
  // this DOES NOT take care of the TimeZone, ie: day time saving (it means it can be inexact for a human mind of 1 hour, but it saved CPU)
  public static Date removeTime(Date baseTime, Duration toRemove)
  {
    return new Date(baseTime.toInstant().minus(toRemove).toEpochMilli());
  }
  

  /*****************************************
  *
  *  isDateBetween
  *
  *****************************************/
  
  public static boolean isDateBetween(Date now, Date from, Date until)
  {
    return from.compareTo(now) <= 0 && now.compareTo(until) < 0;
  }

  private static final byte[] emptyByteArray = {};

  public static byte[] getBytesFromUUID(UUID uuid) 
    {
      if (null == uuid) 
        {
          return emptyByteArray;
        }
      ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
      bb.putLong(uuid.getMostSignificantBits());
      bb.putLong(uuid.getLeastSignificantBits());

      return bb.array();
    }

  public static byte[] getBytesFromUUIDs(List<UUID> uuids) 
    {
      if (null == uuids) 
        {
          return emptyByteArray;
        }
      ByteBuffer bb = ByteBuffer.wrap(new byte[16 * uuids.size()]);
      for(UUID uuid: uuids) 
        {
          bb.putLong(uuid.getMostSignificantBits());
          bb.putLong(uuid.getLeastSignificantBits());  
        }
      
      return bb.array();
    }

  public static UUID getUUIDFromBytes(byte[] bytes) {
    if ((null == bytes) || (bytes.length != 16)) 
      {
        return null;
      }
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
    Long high = byteBuffer.getLong();
    Long low = byteBuffer.getLong();

    return new UUID(high, low);
  }

  public static List<UUID> getUUIDsFromBytes(byte[] bytes) {
    List<UUID> response = new ArrayList<UUID>();
    if ((null == bytes) || (bytes.length % 16 != 0)) 
      {
        return response;
      }
    for(int i = 0; i < bytes.length; i += 16)
      {
        byte[] guidBytes = Arrays.copyOfRange(bytes, i, i + 16);
        ByteBuffer byteBuffer = ByteBuffer.wrap(guidBytes);
        Long high = byteBuffer.getLong();
        Long low = byteBuffer.getLong();
        response.add(new UUID(high, low));
      }
    return response;
  }

  static Random rand = new Random(System.currentTimeMillis());

  public static UUID newSourceAndTimeUUID(byte sourceID) 
    {
      /*****************************************
      *
      *  The UUID has the following format: 
      *  
      *  |                               |                               |
      *  |sID|rnd|...|...|...|...|...|rnd|mls|...|...|...|...|...|...|mls|
      *
      *****************************************/

      Long highRandomNo = Math.abs(rand.nextLong());
      Long timeMillis = System.currentTimeMillis();    
      highRandomNo = highRandomNo >> 8;
      long sourceIDLong = sourceID;
      highRandomNo |= (sourceIDLong << 56);
      return new UUID(highRandomNo, timeMillis);
    }
}
