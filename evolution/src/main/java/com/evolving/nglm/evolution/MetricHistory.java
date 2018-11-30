/*****************************************************************************
*
*  MetricHistory.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SubscriberStreamEvent;
import com.evolving.nglm.core.SubscriberTrace;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import com.evolving.nglm.core.Pair;
import org.json.simple.JSONObject;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;

public class MetricHistory
{
  /*****************************************
  *
  *  enum
  *
  *****************************************/

  //
  //  BucketRepresentation
  //

  public enum BucketRepresentation
  {
    ZeroRepresentation(0, 0L, 0L),
    ByteRepresentation(1, (long) Byte.MIN_VALUE, (long) Byte.MAX_VALUE),
    ShortRepresentation(2, (long) Short.MIN_VALUE, (long) Short.MAX_VALUE),
    IntegerRepresentation(3, (long) Integer.MIN_VALUE, (long) Integer.MAX_VALUE),
    LongRepresentation(4, (long) Long.MIN_VALUE, (long) Long.MAX_VALUE),
    Unknown(-1, -1L, -1L);
    private int externalRepresentation;
    private long minValue;
    private long maxValue;
    private BucketRepresentation(int externalRepresentation, long minValue, long maxValue) { this.externalRepresentation = externalRepresentation; this.minValue = minValue; this.maxValue = maxValue; }
    public int getExternalRepresentation() { return externalRepresentation; }
    public long getMinValue() { return minValue; }
    public long getMaxValue() { return maxValue; }
    public static BucketRepresentation fromExternalRepresentation(int externalRepresentation) { for (BucketRepresentation enumeratedValue : BucketRepresentation.values()) { if (enumeratedValue.getExternalRepresentation() == externalRepresentation) return enumeratedValue; } return Unknown; }

    //
    //  requiredRepresentation
    //
    
    public static BucketRepresentation requiredRepresentation(BucketRepresentation bucketRepresentation, long value)
    {
      BucketRepresentation valueRepresentation = ZeroRepresentation;
      if (value > ZeroRepresentation.getMaxValue() || value < ZeroRepresentation.getMinValue()) valueRepresentation = ByteRepresentation;
      if (value > ByteRepresentation.getMaxValue() || value < ByteRepresentation.getMinValue()) valueRepresentation = ShortRepresentation;
      if (value > ShortRepresentation.getMaxValue() || value < ShortRepresentation.getMinValue()) valueRepresentation = IntegerRepresentation;
      if (value > IntegerRepresentation.getMaxValue() || value < IntegerRepresentation.getMinValue()) valueRepresentation = LongRepresentation;
      BucketRepresentation result = (valueRepresentation.getMaxValue() > bucketRepresentation.getMaxValue()) ? valueRepresentation : bucketRepresentation;
      return result;
    }
  }

  /****************************************
  *
  *  constants
  *
  ****************************************/

  //
  //  epoch
  //
  
  public static Date EPOCH;
  static
  {
    GregorianCalendar epochCalendar = new GregorianCalendar(TimeZone.getTimeZone(Deployment.getBaseTimeZone()));
    epochCalendar.set(2010,0,1);
    epochCalendar.set(Calendar.HOUR_OF_DAY,0);
    epochCalendar.set(Calendar.MINUTE,0);
    epochCalendar.set(Calendar.SECOND,0);
    epochCalendar.set(Calendar.MILLISECOND,0);
    EPOCH = epochCalendar.getTime();
  }

  //
  //  buckets
  //
  
  public static final int MINIMUM_DAY_BUCKETS = 35;
  public static final int MINIMUM_MONTH_BUCKETS = 3;
  
  /*****************************************
  *
  *  schema
  *
  *****************************************/

  //
  //  schema
  //

  private static Schema schema = null;
  static
  {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("metric_history");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("daysSinceEpoch", Schema.INT32_SCHEMA);
    schemaBuilder.field("dailyRepresentation", Schema.INT32_SCHEMA);
    schemaBuilder.field("dailyBuckets", Schema.OPTIONAL_BYTES_SCHEMA);
    schemaBuilder.field("monthlyRepresentation", Schema.INT32_SCHEMA);
    schemaBuilder.field("monthlyBuckets", Schema.OPTIONAL_BYTES_SCHEMA);
    schemaBuilder.field("allTimeBucket", Schema.INT64_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<MetricHistory> serde = new ConnectSerde<MetricHistory>(schema, false, MetricHistory.class, MetricHistory::pack, MetricHistory::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<MetricHistory> serde() { return serde; }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private Date baseDay;
  private Date beginningOfBaseMonth;
  private Date beginningOfDailyValues;
  private Date beginningOfMonthlyValues;
  private Date endOfMonthlyValues;
  private long[] dailyBuckets;
  private long[] monthlyBuckets;
  private long allTimeBucket;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public Date getBaseDay() { return baseDay; }
  public Date getBeginningOfBaseMonth() { return beginningOfBaseMonth; }
  public Date getBeginningOfDailyValues() { return beginningOfDailyValues; }
  public Date getBeginningOfMonthlyValues() { return beginningOfMonthlyValues; }
  public Date getEndOfMonthlyValues() { return endOfMonthlyValues; }
  public long[] getDailyBuckets() { return dailyBuckets; }
  public long[] getMonthlyBuckets() { return monthlyBuckets; }
  public long getAllTimeBucket() { return allTimeBucket; }

  /*****************************************
  *
  *  constructor (simple)
  *
  *****************************************/

  public MetricHistory(int numberOfDailyBuckets, int numberOfMonthlyBuckets)
  {
    this.dailyBuckets = new long[Math.max(numberOfDailyBuckets, MINIMUM_DAY_BUCKETS)];
    this.monthlyBuckets = new long[Math.max(numberOfMonthlyBuckets, MINIMUM_MONTH_BUCKETS)];
    this.allTimeBucket = 0L;
    this.baseDay = EPOCH;
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(Deployment.getBaseTimeZone()));
    this.beginningOfBaseMonth = RLMDateUtils.truncate(this.baseDay, Calendar.MONTH, calendar);
    this.beginningOfDailyValues = RLMDateUtils.addDays(this.baseDay, -1*(dailyBuckets.length-1), calendar);
    this.beginningOfMonthlyValues = RLMDateUtils.addMonths(this.beginningOfBaseMonth, -1*monthlyBuckets.length, calendar);
    this.endOfMonthlyValues = RLMDateUtils.addDays(this.beginningOfBaseMonth, -1, calendar);
  }
  
  /*****************************************
  *
  *  constructor (unpack)
  *
  *****************************************/

  private MetricHistory(Date baseDay, long[] dailyBuckets, long[] monthlyBuckets, long allTimeBucket)
  {
    this.dailyBuckets = dailyBuckets;
    this.monthlyBuckets = monthlyBuckets;
    this.allTimeBucket = allTimeBucket;
    this.baseDay = baseDay;
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(Deployment.getBaseTimeZone()));
    this.beginningOfBaseMonth = RLMDateUtils.truncate(this.baseDay, Calendar.MONTH, calendar);
    this.beginningOfDailyValues = RLMDateUtils.addDays(this.baseDay, -1*(dailyBuckets.length-1), calendar);
    this.beginningOfMonthlyValues = RLMDateUtils.addMonths(this.beginningOfBaseMonth, -1*monthlyBuckets.length, calendar);
    this.endOfMonthlyValues = RLMDateUtils.addDays(this.beginningOfBaseMonth, -1, calendar);
  }

  /*****************************************
  *
  *  constructor (copy)
  *
  *****************************************/

  public MetricHistory(MetricHistory metricHistory)
  {
    this.dailyBuckets = Arrays.copyOf(metricHistory.getDailyBuckets(), metricHistory.getDailyBuckets().length);
    this.monthlyBuckets = Arrays.copyOf(metricHistory.getMonthlyBuckets(), metricHistory.getMonthlyBuckets().length);
    this.allTimeBucket = metricHistory.getAllTimeBucket();
    this.baseDay = metricHistory.getBaseDay();
    this.beginningOfBaseMonth = metricHistory.getBeginningOfBaseMonth();
    this.beginningOfDailyValues = metricHistory.getBeginningOfDailyValues();
    this.beginningOfMonthlyValues = metricHistory.getBeginningOfMonthlyValues();
    this.endOfMonthlyValues = metricHistory.getEndOfMonthlyValues();
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    MetricHistory metricHistory = (MetricHistory) value;

    //
    //  pack buckets
    //

    Pair<BucketRepresentation, byte[]> daily = packBuckets(metricHistory.getDailyBuckets());
    Pair<BucketRepresentation, byte[]> monthly = packBuckets(metricHistory.getMonthlyBuckets());
    
    //
    //  pack
    //
    
    Struct struct = new Struct(schema);
    struct.put("daysSinceEpoch", RLMDateUtils.daysBetween(EPOCH, metricHistory.getBaseDay(), Deployment.getBaseTimeZone()));
    struct.put("dailyRepresentation", daily.getFirstElement().getExternalRepresentation());
    struct.put("dailyBuckets", daily.getSecondElement());
    struct.put("monthlyRepresentation", monthly.getFirstElement().getExternalRepresentation());
    struct.put("monthlyBuckets", monthly.getSecondElement());
    struct.put("allTimeBucket", metricHistory.getAllTimeBucket());
    return struct;
  }

  /****************************************
  *
  *  packBuckets
  *
  ****************************************/

  private static Pair<BucketRepresentation, byte[]> packBuckets(long[] buckets)
  {
    //
    //  determine representation
    //
    
    BucketRepresentation bucketRepresentation = BucketRepresentation.ZeroRepresentation;
    for (int i = 0; i < buckets.length; i++)
      {
        bucketRepresentation = BucketRepresentation.requiredRepresentation(bucketRepresentation, buckets[i]);
      }

    //
    //  pack
    //

    byte[] packedBuckets;
    switch (bucketRepresentation)
      {
        case ZeroRepresentation:
          packedBuckets = new byte[1];
          packedBuckets[0] = (byte) buckets.length;
          break;
          
        case ByteRepresentation:
          packedBuckets = new byte[buckets.length];
          for (int i = 0; i < buckets.length; i++)
            {
              packedBuckets[i] = (byte) buckets[i];
            }
          break;
          
        case ShortRepresentation:
          packedBuckets = new byte[2*buckets.length];
          for (int i = 0; i < buckets.length; i++)
            {
              packedBuckets[2*i]   = (byte) ((buckets[i] >> 8) & 0xFF);
              packedBuckets[2*i+1] = (byte) ((buckets[i] >> 0) & 0xFF);
            }
          break;
          
        case IntegerRepresentation:
          packedBuckets = new byte[4*buckets.length];
          for (int i = 0; i < buckets.length; i++)
            {
              packedBuckets[4*i]   = (byte) ((buckets[i] >> 24) & 0xFF);
              packedBuckets[4*i+1] = (byte) ((buckets[i] >> 16) & 0xFF);
              packedBuckets[4*i+2] = (byte) ((buckets[i] >>  8) & 0xFF);
              packedBuckets[4*i+3] = (byte) ((buckets[i] >>  0) & 0xFF);
            }
          break;
          
        case LongRepresentation:
          packedBuckets = new byte[8*buckets.length];
          for (int i = 0; i < buckets.length; i++)
            {
              packedBuckets[8*i]   = (byte) ((buckets[i] >> 56) & 0xFF);
              packedBuckets[8*i+1] = (byte) ((buckets[i] >> 48) & 0xFF);
              packedBuckets[8*i+2] = (byte) ((buckets[i] >> 40) & 0xFF);
              packedBuckets[8*i+3] = (byte) ((buckets[i] >> 32) & 0xFF);
              packedBuckets[8*i+4] = (byte) ((buckets[i] >> 24) & 0xFF);
              packedBuckets[8*i+5] = (byte) ((buckets[i] >> 16) & 0xFF);
              packedBuckets[8*i+6] = (byte) ((buckets[i] >>  8) & 0xFF);
              packedBuckets[8*i+7] = (byte) ((buckets[i] >>  0) & 0xFF);
            }
          break;

        default:
          throw new RuntimeException("unnkown representation");
      }

    //
    //  return
    //

    return new Pair<BucketRepresentation, byte[]>(bucketRepresentation, packedBuckets);
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static MetricHistory unpack(SchemaAndValue schemaAndValue)
  {
    //
    //  data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;

    //
    //  unpack
    //

    Struct valueStruct = (Struct) value;
    int daysSinceEpoch = valueStruct.getInt32("daysSinceEpoch");
    BucketRepresentation dailyRepresentation = BucketRepresentation.fromExternalRepresentation(valueStruct.getInt32("dailyRepresentation"));
    byte[] packedDailyBuckets = valueStruct.getBytes("dailyBuckets");
    BucketRepresentation monthlyRepresentation = BucketRepresentation.fromExternalRepresentation(valueStruct.getInt32("monthlyRepresentation"));
    byte[] packedMonthlyBuckets = valueStruct.getBytes("monthlyBuckets");
    long allTimeBucket = valueStruct.getInt64("allTimeBucket");

    //
    //  unpack buckets
    //

    long[] dailyBuckets = unpackBuckets(dailyRepresentation, packedDailyBuckets);
    long[] monthlyBuckets = unpackBuckets(monthlyRepresentation, packedMonthlyBuckets);
    
    //
    //  return
    //

    return new MetricHistory(RLMDateUtils.addDays(EPOCH, daysSinceEpoch, Deployment.getBaseTimeZone()), dailyBuckets, monthlyBuckets, allTimeBucket);
  }
  
  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static MetricHistory unpack(SchemaAndValue schemaAndValue, int numberOfDailyBuckets, int numberOfMonthlyBuckets)
  {
    //
    //  basic unpack
    //
    
    MetricHistory metricHistory = unpack(schemaAndValue);
    
    //
    //  ensure
    //

    numberOfDailyBuckets = Math.max(numberOfDailyBuckets, MINIMUM_DAY_BUCKETS);
    numberOfMonthlyBuckets = Math.max(numberOfMonthlyBuckets, MINIMUM_MONTH_BUCKETS);
    if (metricHistory.getDailyBuckets().length != numberOfDailyBuckets || metricHistory.getMonthlyBuckets().length != numberOfMonthlyBuckets)
      {
        metricHistory.resize(numberOfDailyBuckets, numberOfMonthlyBuckets);
      }

    //
    //  return
    //

    return metricHistory;
  }

  /****************************************
  *
  *  unpackBuckets
  *
  ****************************************/

  private static long[] unpackBuckets(BucketRepresentation bucketRepresentation, byte[] packedBuckets)
  {
    int numberOfBuckets;
    long[] buckets;
    switch (bucketRepresentation)
      {
        case ZeroRepresentation:
          numberOfBuckets = packedBuckets[0];
          buckets = new long[numberOfBuckets];
          break;
          
        case ByteRepresentation:
          numberOfBuckets = packedBuckets.length;
          buckets = new long[numberOfBuckets];
          for (int i = 0; i < numberOfBuckets; i++)
            {
              buckets[i] = packedBuckets[i];
            }
          break;
          
        case ShortRepresentation:
          numberOfBuckets = packedBuckets.length/2;
          buckets = new long[numberOfBuckets];
          for (int i = 0; i < numberOfBuckets; i++)
            {
              long bucket = 0L;
              bucket = bucket | (((long) packedBuckets[2*i])   & 0xFF) << 8;
              bucket = bucket | (((long) packedBuckets[2*i+1]) & 0xFF) << 0;
              buckets[i] = bucket;
            }
          break;
          
        case IntegerRepresentation:
          numberOfBuckets = packedBuckets.length/4;
          buckets = new long[numberOfBuckets];
          for (int i = 0; i < numberOfBuckets; i++)
            {
              long bucket = 0L;
              bucket = bucket | (((long) packedBuckets[4*i])   & 0xFF) << 24;
              bucket = bucket | (((long) packedBuckets[4*i+1]) & 0xFF) << 16;
              bucket = bucket | (((long) packedBuckets[4*i+2]) & 0xFF) <<  8;
              bucket = bucket | (((long) packedBuckets[4*i+3]) & 0xFF) <<  0;
              buckets[i] = bucket;
            }
          break;
          
        case LongRepresentation:
          numberOfBuckets = packedBuckets.length/8;
          buckets = new long[numberOfBuckets];
          for (int i = 0; i < numberOfBuckets; i++)
            {
              long bucket = 0L;
              bucket = bucket | (((long) packedBuckets[8*i])   & 0xFF) << 56;
              bucket = bucket | (((long) packedBuckets[8*i+1]) & 0xFF) << 48;
              bucket = bucket | (((long) packedBuckets[8*i+2]) & 0xFF) << 40;
              bucket = bucket | (((long) packedBuckets[8*i+3]) & 0xFF) << 32;
              bucket = bucket | (((long) packedBuckets[8*i+4]) & 0xFF) << 24;
              bucket = bucket | (((long) packedBuckets[8*i+5]) & 0xFF) << 16;
              bucket = bucket | (((long) packedBuckets[8*i+6]) & 0xFF) <<  8;
              bucket = bucket | (((long) packedBuckets[8*i+7]) & 0xFF) <<  0;
              buckets[i] = bucket;
            }
          break;

        default:
          throw new RuntimeException("unknown representation");
      }

    return buckets;
  }

  /****************************************
  *
  *  update
  *
  ****************************************/

  public synchronized void update(Date date, long increment)
  {
    /****************************************
    *
    *  day
    *
    ****************************************/
    
    Date day = RLMDateUtils.truncate(date, Calendar.DATE, Calendar.SUNDAY, Deployment.getBaseTimeZone());
    Date beginningOfCurrentMonth = RLMDateUtils.truncate(day, Calendar.MONTH, Deployment.getBaseTimeZone());

    /****************************************
    *
    *  update data structures (as necessary)
    *
    ****************************************/

    if (day.after(baseDay))
      {
        //
        //  create newDailyBuckets
        //
        
        long[] newDailyBuckets = new long[dailyBuckets.length];
        int dayOffset = RLMDateUtils.daysBetween(baseDay, day, Deployment.getBaseTimeZone());
        for (int i = 0; i < dailyBuckets.length - dayOffset; i++)
          {
            newDailyBuckets[i] = dailyBuckets[i + dayOffset];
          }

        //
        //  create newMonthlyBuckets
        //

        long[] newMonthlyBuckets = new long[monthlyBuckets.length];
        int monthOffset = RLMDateUtils.monthsBetween(beginningOfBaseMonth, beginningOfCurrentMonth, Deployment.getBaseTimeZone());
        for (int i = 0; i < monthlyBuckets.length - monthOffset; i++)
          {
            newMonthlyBuckets[i] = monthlyBuckets[i + monthOffset];
          }

        //
        //  populate monthly bucket for "old" baseMonth
        //

        if (0 < monthOffset && monthOffset <= monthlyBuckets.length)
          {
            newMonthlyBuckets[monthlyBuckets.length - monthOffset] = getValue(beginningOfBaseMonth, baseDay);
          }

        //
        //  update
        //
        
        dailyBuckets = newDailyBuckets;
        monthlyBuckets = newMonthlyBuckets;
        baseDay = day;
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(Deployment.getBaseTimeZone()));
        beginningOfBaseMonth = RLMDateUtils.truncate(baseDay, Calendar.MONTH, calendar);
        beginningOfDailyValues = RLMDateUtils.addDays(baseDay, -1*(dailyBuckets.length-1), calendar);
        beginningOfMonthlyValues = RLMDateUtils.addMonths(beginningOfBaseMonth, -1*monthlyBuckets.length, calendar);
        endOfMonthlyValues = RLMDateUtils.addDays(beginningOfBaseMonth, -1, calendar);
      }
    
    /****************************************
    *
    *  apply update
    *
    ****************************************/

    //
    //  daily
    //

    if (day.compareTo(beginningOfDailyValues) >= 0)
      {
        int bucketIndex = dailyBuckets.length - RLMDateUtils.daysBetween(day, baseDay, Deployment.getBaseTimeZone()) - 1;
        dailyBuckets[bucketIndex] += increment;
      }
    
    //
    //  monthlyBucket
    //

    if (beginningOfCurrentMonth.compareTo(beginningOfMonthlyValues) >= 0 && beginningOfCurrentMonth.compareTo(beginningOfBaseMonth) < 0)
      {
        int bucketIndex = monthlyBuckets.length - RLMDateUtils.monthsBetween(beginningOfCurrentMonth, beginningOfBaseMonth, Deployment.getBaseTimeZone());
        monthlyBuckets[bucketIndex] += increment;
      }
    
    //
    //  allTimeBucket
    //

    allTimeBucket += increment;
  }

  /*****************************************
  *
  *  getValue
  *
  *  startDay - legal values are
  *    A - the beginning of time (NULL)
  *    B - the first of the previous "4" months (including this one)
  *    C - any of the last "34" days
  *    D - any date in the future
  *
  *  endDay - legal values are
  *    A - the last of the previous "4" months (including this one)
  *    B - any of the last "34" days
  *    C - any date in the future
  *    D - the end of time (NULL)
  *  (and endDate must be >= startDate)
  *
  *  (where "today/future/etc" is defined relative the the accumulatorDate)
  *
  *  cases
  *    A - A    result := Total - Sum(months starting after endDate) - Sum(days on/after beginningOfMonthCalendar)
  *    A - B    result := Total - Sum(days after endDate)  [note:  Sum(days after endDate) == (Sum(days on/after endDate) - endDate)]
  *    A - C    result := Total
  *    A - D    result := Total
  *
  *    B - A    result := Sum(months starting on/after startDate and ending on/before endDate) 
  *    B - B    result := Sum(months starting on/after startDate) + Sum(days on/after beginningOfMonthCalendar and on/before endDate)
  *    B - C    result := Sum(months starting on/after startDate) + Sum(days on/after beginningOfMonthCalendar)
  *    B - D    result := Sum(months starting on/after startDate) + Sum(days on/after beginningOfMonthCalendar)
  *    
  *    C - A    error
  *    C - B    result := Sum(days on/after startDate and on/before endDate)
  *    C - C    result := Sum(days on/after startDate)
  *    C - D    result := Sum(days on/after startDate)
  *
  *    D - A    error
  *    D - B    error
  *    D - C    result := 0
  *    D - D    result := 0
  *
  *****************************************/

  private enum DateCase { A, B, C, D }

  public synchronized long getValue(Date startDay, Date endDay) throws IllegalArgumentException
  {
    /****************************************
    *
    *  validate input
    *
    ****************************************/

    //
    //  startDay
    //

    startDay = Objects.equals(startDay, NGLMRuntime.BEGINNING_OF_TIME) ? null : startDay;
    if (startDay != null && ! Objects.equals(startDay, RLMDateUtils.truncate(startDay, Calendar.DATE, Calendar.SUNDAY, Deployment.getBaseTimeZone())))
      {
        throw new IllegalArgumentException("startDay must be on a day boundary");
      }

    //
    //  endDay
    //
    
    endDay = Objects.equals(endDay, NGLMRuntime.END_OF_TIME) ? null : endDay;
    if (endDay != null && ! Objects.equals(endDay, RLMDateUtils.truncate(endDay, Calendar.DATE, Calendar.SUNDAY, Deployment.getBaseTimeZone())))
      {
        throw new IllegalArgumentException("endDay must be on a day boundary");
      }

    /****************************************
    *
    *  relevant start/end days
    *
    ****************************************/

    Set<Date> monthlyStartDays = getMonthlyStartDays(beginningOfMonthlyValues, beginningOfBaseMonth);
    Set<Date> monthlyEndDays = getMonthlyEndDays(beginningOfMonthlyValues, beginningOfBaseMonth);

    /*****************************************
    *
    *  validate startDay and endDay
    *    - startDay must be a legal value
    *    - endDay must be a legal value
    *    - startDay on/before endDay
    *
    *****************************************/
    
    //
    //  startDay is a legal value
    //
    //    A - the beginning of time (NULL)
    //    B - the first of the previous "4" months (including this one)
    //    C - any of the last "34" days
    //    D - any date in the future
    //

    DateCase startDayCase;
    if (startDay == null)
      startDayCase = DateCase.A;
    else if (monthlyStartDays.contains(startDay))
      startDayCase = DateCase.B;
    else if (! startDay.before(beginningOfDailyValues) && ! startDay.after(baseDay))
      startDayCase = DateCase.C;
    else if (startDay.after(baseDay))
      startDayCase = DateCase.D;
    else
      throw new IllegalArgumentException("unavailable startDay");

    //
    //  endDay is a legal value
    //
    //    A - the last of the previous "4" months
    //    B - any of the last "34" days
    //    C - any date in the future
    //    D - the end of time (NULL)
    //

    DateCase endDayCase;
    if (endDay == null)
      endDayCase = DateCase.D;
    else if (monthlyEndDays.contains(endDay))
      endDayCase = DateCase.A;
    else if (! endDay.before(beginningOfDailyValues) && ! endDay.after(baseDay))
      endDayCase = DateCase.B;
    else if (endDay.after(baseDay))
      endDayCase = DateCase.C;
    else
      throw new IllegalArgumentException("unavailable endDay");

    //
    //  startDay on/before endDay
    //

    if (startDay != null && endDay != null && startDay.after(endDay))
      {
        throw new IllegalArgumentException("startDay after endDay");
      }

    /*****************************************
    *
    *  calculate result
    *
    *****************************************/

    //
    //  calculate result
    //

    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(Deployment.getBaseTimeZone()));
    long result = 0;
    switch (startDayCase)
      {
        case A:
          switch (endDayCase)
            {
              case A:
                result = allTimeBucket - sumMonthlyValues(RLMDateUtils.addDays(endDay, 1, calendar), endOfMonthlyValues) - sumDailyValues(beginningOfBaseMonth,baseDay);
                break;
              case B:
                result = allTimeBucket - sumDailyValues(RLMDateUtils.addDays(endDay, 1, calendar), baseDay);
                break;
              case C:
                result = allTimeBucket;
                break;
              case D:
                result = allTimeBucket;
                break;
            }
          break;

        case B:
          switch (endDayCase)
            {
              case A:
                result = sumMonthlyValues(startDay,endDay);
                break;
              case B:
                result = sumMonthlyValues(startDay,endOfMonthlyValues) + sumDailyValues(beginningOfBaseMonth,endDay);
                break;
              case C:
                result = sumMonthlyValues(startDay,endOfMonthlyValues) + sumDailyValues(beginningOfBaseMonth,baseDay);
                break;
              case D:
                result = sumMonthlyValues(startDay,endOfMonthlyValues) + sumDailyValues(beginningOfBaseMonth,baseDay);
                break;
            }
          break;

        case C:
          switch (endDayCase)
            {
              case A:
                result = sumDailyValues(startDay, endDay);
                break;
              case B:
                result = sumDailyValues(startDay, endDay);
                break;
              case C:
                result = sumDailyValues(startDay, baseDay);
                break;
              case D:
                result = sumDailyValues(startDay, baseDay);
                break;
            }
          break;

        case D:
          switch (endDayCase)
            {
              case A:
                throw new ServerRuntimeException("Start/End case D/A can't happen");
              case B:
                throw new ServerRuntimeException("Start/End case D/B can't happen");
              case C:
                result = 0;
                break;
              case D:
                result = 0;
                break;
            }
          break;
      }

    /*****************************************
    *
    *  return
    *
    *****************************************/

    return result;
  }

  /****************************************
  *
  *  getMonthlyStartDays
  *
  ****************************************/

  private static Map<Pair<Date,Date>,Set<Date>> monthlyStartDaysCache = Collections.synchronizedMap(new LinkedHashMap<Pair<Date,Date>,Set<Date>>() { @Override protected boolean removeEldestEntry(Map.Entry eldest) { return size() > 100; } });
  private static Set<Date> getMonthlyStartDays(Date firstMonth, Date baseMonth)
  {
    Pair<Date,Date> key = new Pair<Date,Date>(firstMonth, baseMonth);
    Set<Date> result = monthlyStartDaysCache.get(key);
    if (result == null)
      {
        result = new HashSet<Date>();
        for (Date month = firstMonth; month.before(baseMonth); month = RLMDateUtils.addMonths(month, 1, Deployment.getBaseTimeZone()))
          {
            result.add(month);
          }
        monthlyStartDaysCache.put(key,result);
      }
    return result;
  }

  /****************************************
  *
  *  getMonthlyEndDays
  *
  ****************************************/

  private static Map<Pair<Date,Date>,Set<Date>> monthlyEndDaysCache = Collections.synchronizedMap(new LinkedHashMap<Pair<Date,Date>,Set<Date>>() { @Override protected boolean removeEldestEntry(Map.Entry eldest) { return size() > 100; } });
  private static Set<Date> getMonthlyEndDays(Date firstMonth, Date baseMonth)
  {
    Pair<Date,Date> key = new Pair<Date,Date>(firstMonth, baseMonth);
    Set<Date> result = monthlyEndDaysCache.get(key);
    if (result == null)
      {
        result = new HashSet<Date>();
        for (Date month = firstMonth; month.before(baseMonth); month = RLMDateUtils.addMonths(month, 1, Deployment.getBaseTimeZone()))
          {
            result.add(RLMDateUtils.addDays(RLMDateUtils.addMonths(month, 1, Deployment.getBaseTimeZone()), -1, Deployment.getBaseTimeZone()));
          }
        monthlyEndDaysCache.put(key,result);
      }
    return result;
  }
  
  /****************************************
  *
  *  sumDailyValues
  *
  ****************************************/

  private long sumDailyValues(Date startDay, Date endDay)
  {
    long result = 0L;
    int bucketIndex = 0;
    Date bucketDay = beginningOfDailyValues;
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(Deployment.getBaseTimeZone()));
    while (bucketDay.compareTo(endDay) <= 0)
      {
        if (bucketDay.compareTo(startDay) >= 0)
          {
            result += dailyBuckets[bucketIndex];
          }
        bucketIndex += 1;
        bucketDay = RLMDateUtils.addDays(bucketDay, 1, calendar);
      }
    return result;
  }
  
  /****************************************
  *
  *  sumMonthlyValues
  *
  ****************************************/

  private long sumMonthlyValues(Date startDay, Date endDay)
  {
    long result = 0L;
    int bucketIndex = 0;
    Date bucketMonth = beginningOfMonthlyValues;
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(Deployment.getBaseTimeZone()));
    while (bucketMonth.before(endDay))
      {
        if (bucketMonth.compareTo(startDay) >= 0)
          {
            result += monthlyBuckets[bucketIndex];
          }
        bucketIndex += 1;
        bucketMonth = RLMDateUtils.addMonths(bucketMonth, 1, calendar);
      }
    return result;
  }
  
  /****************************************
  *
  *  resize
  *
  ****************************************/

  public synchronized void resize(int numberOfDailyBuckets, int numberOfMonthlyBuckets)
  {
    //
    //  minimums
    //

    numberOfDailyBuckets = Math.max(numberOfDailyBuckets, MINIMUM_DAY_BUCKETS);
    numberOfMonthlyBuckets = Math.max(numberOfMonthlyBuckets, MINIMUM_MONTH_BUCKETS);

    //
    //  create newDailyBuckets
    //

    long[] newDailyBuckets = new long[numberOfDailyBuckets];
    for (int i = 0; i < Math.min(numberOfDailyBuckets, dailyBuckets.length); i++)
      {
        newDailyBuckets[numberOfDailyBuckets-i-1] = dailyBuckets[dailyBuckets.length-i-1];
      }
    dailyBuckets = newDailyBuckets;

    //
    //  create newMonthlyBuckets
    //

    long[] newMonthlyBuckets = new long[numberOfMonthlyBuckets];
    for (int i = 0; i < Math.min(numberOfMonthlyBuckets, monthlyBuckets.length); i++)
      {
        newMonthlyBuckets[numberOfMonthlyBuckets-i-1] = monthlyBuckets[monthlyBuckets.length-i-1];
      }
    monthlyBuckets = newMonthlyBuckets;
  }
}
