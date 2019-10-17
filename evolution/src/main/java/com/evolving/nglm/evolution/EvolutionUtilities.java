/*****************************************************************************
*
*  EvolutionUtilities.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.evolution.EvolutionUtilities.TimeUnit;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;

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

  /*****************************************
  *
  *  addTime
  *
  *****************************************/

  public static Date addTime(Date baseTime, int amount, TimeUnit timeUnit, String timeZone)
  {
    return addTime(baseTime, amount, timeUnit, timeZone, false);
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
