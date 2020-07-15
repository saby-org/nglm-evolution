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
import com.evolving.nglm.core.SystemTime;

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
import java.util.concurrent.ConcurrentHashMap;

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
    UninitializedRepresentation(5, 0L, 0L),
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

  //
  //  MetricHistoryMode
  //

  public enum MetricHistoryMode
  {
    Standard("standard", 0),
    Max("max", 1),
    Min("min", 2),
    Unknown("(unknown)", -1);
    private String externalRepresentation;
    private int internalRepresentation;
    private MetricHistoryMode(String externalRepresentation, int internalRepresentation) { this.externalRepresentation = externalRepresentation; this.internalRepresentation = internalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public int getInternalRepresentation() { return internalRepresentation; }
    public static MetricHistoryMode fromExternalRepresentation(String externalRepresentation) { for (MetricHistoryMode enumeratedValue : MetricHistoryMode.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
    public static MetricHistoryMode fromInternalRepresentation(int internalRepresentation) { for (MetricHistoryMode enumeratedValue : MetricHistoryMode.values()) { if (enumeratedValue.getInternalRepresentation() == internalRepresentation) return enumeratedValue; } return Unknown; }
  }

  //
  //  Criteria
  //

  public enum Criteria { IsNonZero, IsZero; }

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
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(2));
    schemaBuilder.field("daysSinceEpoch", Schema.INT32_SCHEMA);
    schemaBuilder.field("dailyRepresentation", Schema.INT32_SCHEMA);
    schemaBuilder.field("dailyBuckets", Schema.OPTIONAL_BYTES_SCHEMA);
    schemaBuilder.field("monthlyRepresentation", Schema.INT32_SCHEMA);
    schemaBuilder.field("monthlyBuckets", Schema.OPTIONAL_BYTES_SCHEMA);
    schemaBuilder.field("allTimeBucket", Schema.INT64_SCHEMA);
    schemaBuilder.field("metricHistoryMode", SchemaBuilder.int32().defaultValue(0).schema());
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

  private boolean initialized;
  private Date baseDay;
  private Date beginningOfBaseMonth;
  private Date beginningOfDailyValues;
  private Date beginningOfMonthlyValues;
  private Date endOfMonthlyValues;
  private long[] dailyBuckets;
  private long[] monthlyBuckets;
  private long allTimeBucket;
  private MetricHistoryMode metricHistoryMode;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public boolean getInitialized() { return initialized; }
  public Date getBaseDay() { return baseDay; }
  public Date getBeginningOfBaseMonth() { return beginningOfBaseMonth; }
  public Date getBeginningOfDailyValues() { return beginningOfDailyValues; }
  public Date getBeginningOfMonthlyValues() { return beginningOfMonthlyValues; }
  public Date getEndOfMonthlyValues() { return endOfMonthlyValues; }
  public long[] getDailyBuckets() { return dailyBuckets; }
  public long[] getMonthlyBuckets() { return monthlyBuckets; }
  public long getAllTimeBucket() { return allTimeBucket; }
  public MetricHistoryMode getMetricHistoryMode() { return metricHistoryMode; }

  /*****************************************
  *
  *  constructor (simple)
  *
  *****************************************/

  //
  //  convenience constructors
  //

  public MetricHistory(int numberOfDailyBuckets, int numberOfMonthlyBuckets) { this(numberOfDailyBuckets, numberOfMonthlyBuckets, MetricHistoryMode.Standard); }
  
  //
  //  full
  //
  
  public MetricHistory(int numberOfDailyBuckets, int numberOfMonthlyBuckets, MetricHistoryMode metricHistoryMode)
  {
    this.initialized = false;
    this.dailyBuckets = allocateBuckets(metricHistoryMode, Math.max(numberOfDailyBuckets, MINIMUM_DAY_BUCKETS));
    this.monthlyBuckets = allocateBuckets(metricHistoryMode, Math.max(numberOfMonthlyBuckets, MINIMUM_MONTH_BUCKETS));
    this.allTimeBucket = (metricHistoryMode == MetricHistoryMode.Standard) ? 0L : -1L;
    this.baseDay = EPOCH;
    this.beginningOfBaseMonth = RLMDateUtils.truncate(this.baseDay, Calendar.MONTH, Deployment.getBaseTimeZone());
    this.beginningOfDailyValues = RLMDateUtils.addDays(this.baseDay, -1*(dailyBuckets.length-1), Deployment.getBaseTimeZone());
    this.beginningOfMonthlyValues = RLMDateUtils.addMonths(this.beginningOfBaseMonth, -1*monthlyBuckets.length, Deployment.getBaseTimeZone());
    this.endOfMonthlyValues = RLMDateUtils.addDays(this.beginningOfBaseMonth, -1, Deployment.getBaseTimeZone());
    this.metricHistoryMode = metricHistoryMode;
  }

  /****************************************
  *
  *  allocateBuckets
  *
  ****************************************/

  private static long[] allocateBuckets(MetricHistoryMode metricHistoryMode, int numberOfBuckets)
  {
    long[] newBuckets = new long[numberOfBuckets];
    for (int i = 0; i < numberOfBuckets; i++)
      {
        switch (metricHistoryMode)
          {
            case Standard:
              newBuckets[i] = 0L;
              break;

            case Max:
            case Min:
              newBuckets[i] = -1L;
              break;
          }
      }
    return newBuckets;
  }
  
  /*****************************************
  *
  *  constructor (unpack)
  *
  *****************************************/

  private MetricHistory(boolean initialized, Date baseDay, long[] dailyBuckets, long[] monthlyBuckets, long allTimeBucket, MetricHistoryMode metricHistoryMode)
  {
    this.initialized = initialized;
    this.dailyBuckets = dailyBuckets;
    this.monthlyBuckets = monthlyBuckets;
    this.allTimeBucket = allTimeBucket;
    this.baseDay = baseDay;
    this.beginningOfBaseMonth = RLMDateUtils.truncate(this.baseDay, Calendar.MONTH, Deployment.getBaseTimeZone());
    this.beginningOfDailyValues = RLMDateUtils.addDays(this.baseDay, -1*(dailyBuckets.length-1), Deployment.getBaseTimeZone());
    this.beginningOfMonthlyValues = RLMDateUtils.addMonths(this.beginningOfBaseMonth, -1*monthlyBuckets.length, Deployment.getBaseTimeZone());
    this.endOfMonthlyValues = RLMDateUtils.addDays(this.beginningOfBaseMonth, -1, Deployment.getBaseTimeZone());
    this.metricHistoryMode = metricHistoryMode;
  }

  /*****************************************
  *
  *  constructor (copy)
  *
  *****************************************/

  public MetricHistory(MetricHistory metricHistory)
  {
    this.initialized = metricHistory.getInitialized();
    this.dailyBuckets = Arrays.copyOf(metricHistory.getDailyBuckets(), metricHistory.getDailyBuckets().length);
    this.monthlyBuckets = Arrays.copyOf(metricHistory.getMonthlyBuckets(), metricHistory.getMonthlyBuckets().length);
    this.allTimeBucket = metricHistory.getAllTimeBucket();
    this.baseDay = metricHistory.getBaseDay();
    this.beginningOfBaseMonth = metricHistory.getBeginningOfBaseMonth();
    this.beginningOfDailyValues = metricHistory.getBeginningOfDailyValues();
    this.beginningOfMonthlyValues = metricHistory.getBeginningOfMonthlyValues();
    this.endOfMonthlyValues = metricHistory.getEndOfMonthlyValues();
    this.metricHistoryMode = metricHistory.getMetricHistoryMode();
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

    Pair<BucketRepresentation, byte[]> daily = metricHistory.getInitialized() ? packBuckets(metricHistory.getDailyBuckets()) : packUninitializedBuckets(metricHistory.getDailyBuckets());
    Pair<BucketRepresentation, byte[]> monthly = metricHistory.getInitialized() ? packBuckets(metricHistory.getMonthlyBuckets()): packUninitializedBuckets(metricHistory.getMonthlyBuckets());
    
    //
    //  pack
    //
    
    Struct struct = new Struct(schema);
    struct.put("daysSinceEpoch", getDaysSinceEpoch(metricHistory.getBaseDay()));
    struct.put("dailyRepresentation", daily.getFirstElement().getExternalRepresentation());
    struct.put("dailyBuckets", daily.getSecondElement());
    struct.put("monthlyRepresentation", monthly.getFirstElement().getExternalRepresentation());
    struct.put("monthlyBuckets", monthly.getSecondElement());
    struct.put("allTimeBucket", metricHistory.getAllTimeBucket());
    struct.put("metricHistoryMode", metricHistory.getMetricHistoryMode().getInternalRepresentation());
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

  /****************************************
  *
  *  packUninitializedBuckets
  *
  ****************************************/

  private static Pair<BucketRepresentation, byte[]> packUninitializedBuckets(long[] buckets)
  {
    BucketRepresentation bucketRepresentation = BucketRepresentation.UninitializedRepresentation;
    byte[] packedBuckets = new byte[1];
    packedBuckets[0] = (byte) buckets.length;
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
    MetricHistoryMode metricHistoryMode = (schemaVersion >= 2) ? MetricHistoryMode.fromInternalRepresentation(valueStruct.getInt32("metricHistoryMode")) : MetricHistoryMode.Standard;

    //
    //  unpack buckets
    //

    long[] dailyBuckets = (dailyRepresentation != BucketRepresentation.UninitializedRepresentation) ? unpackBuckets(dailyRepresentation, packedDailyBuckets) : unpackUninitializedBuckets(metricHistoryMode, packedDailyBuckets);
    long[] monthlyBuckets = (monthlyRepresentation != BucketRepresentation.UninitializedRepresentation) ? unpackBuckets(monthlyRepresentation, packedMonthlyBuckets) : unpackUninitializedBuckets(metricHistoryMode, packedMonthlyBuckets);

    //
    //  initialized?
    //
    
    boolean initialized = (dailyRepresentation != BucketRepresentation.UninitializedRepresentation) || (monthlyRepresentation != BucketRepresentation.UninitializedRepresentation);
    
    //
    //  return
    //

    return new MetricHistory(initialized, getDateFromEpoch(daysSinceEpoch), dailyBuckets, monthlyBuckets, allTimeBucket, metricHistoryMode);
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

  public static long[] unpackBuckets(BucketRepresentation bucketRepresentation, byte[] packedBuckets)
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
              buckets[i] = (long) packedBuckets[i];
            }
          break;
          
        case ShortRepresentation:
          numberOfBuckets = packedBuckets.length/2;
          buckets = new long[numberOfBuckets];
          for (int i = 0; i < numberOfBuckets; i++)
            {
              short bucket = 0;
              bucket = (short) (bucket | (((int) packedBuckets[2*i])   & 0xFF) << 8);
              bucket = (short) (bucket | (((int) packedBuckets[2*i+1]) & 0xFF) << 0);
              buckets[i] = (long) bucket;
            }
          break;
          
        case IntegerRepresentation:
          numberOfBuckets = packedBuckets.length/4;
          buckets = new long[numberOfBuckets];
          for (int i = 0; i < numberOfBuckets; i++)
            {
              int bucket = 0;
              bucket = bucket | (((int) packedBuckets[4*i])   & 0xFF) << 24;
              bucket = bucket | (((int) packedBuckets[4*i+1]) & 0xFF) << 16;
              bucket = bucket | (((int) packedBuckets[4*i+2]) & 0xFF) <<  8;
              bucket = bucket | (((int) packedBuckets[4*i+3]) & 0xFF) <<  0;
              buckets[i] = (long) bucket;
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
  *  unpackUninitializedBuckets
  *
  ****************************************/

  private static long[] unpackUninitializedBuckets(MetricHistoryMode metricHistoryMode, byte[] packedBuckets)
  {
    int numberOfBuckets = packedBuckets[0];
    long[] buckets = allocateBuckets(metricHistoryMode, numberOfBuckets);
    return buckets;
  }

  /****************************************
  *
  *  date-based caches
  *
  ****************************************/

  private static Map<Date, Integer> daysSinceEpochIntegers = new ConcurrentHashMap<Date, Integer>();
  private static Map<Integer, Date> daysSinceEpochDates = new ConcurrentHashMap<Integer, Date>();

  //
  //  getDaysSinceEpoch
  //
  
  private static Integer getDaysSinceEpoch(Date baseDay)
  {
    Integer result = daysSinceEpochIntegers.get(baseDay);
    if (result == null)
      {
        result = RLMDateUtils.daysBetween(EPOCH, baseDay, Deployment.getBaseTimeZone());
        daysSinceEpochIntegers.put(baseDay, result);
      }
    return result;
  }
  
  //
  //  getDateFromEpoch
  //

  private static Date getDateFromEpoch(Integer daysSinceEpoch)
  {
    Date result = daysSinceEpochDates.get(daysSinceEpoch);
    if (result == null)
      {
        result = RLMDateUtils.addDays(EPOCH, daysSinceEpoch, Deployment.getBaseTimeZone());
        daysSinceEpochDates.put(daysSinceEpoch, result);
      }
    return result;
  }
  
  /****************************************
  *
  *  update
  *
  ****************************************/

  //
  //  update (simple)
  //
      
  public synchronized void update(Date date, long value)
  {
    update(date, value, null);
  }
  
  //
  //  update (full)
  //
      
  public synchronized void update(Date date, long value, Object briefcase)
  {
    /****************************************
    *
    *  day
    *
    ****************************************/
    
    Date now = SystemTime.getCurrentTime();
    Date effectiveDate = date.before(now) ? date : now;
    Date day = RLMDateUtils.truncate(effectiveDate, Calendar.DATE, Calendar.SUNDAY, Deployment.getBaseTimeZone());
    Date beginningOfCurrentMonth = RLMDateUtils.truncate(day, Calendar.MONTH, Deployment.getBaseTimeZone());

    /****************************************
    *
    *  validate
    *
    ****************************************/

    switch (metricHistoryMode)
      {
        case Standard:
          break;

        case Max:
        case Min:
          if (value < 0)
            {
              return;
            }
          break;
      }
    
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
        
        long[] newDailyBuckets = allocateBuckets(metricHistoryMode, dailyBuckets.length);
        int dayOffset = RLMDateUtils.daysBetween(baseDay, day, Deployment.getBaseTimeZone());
        for (int i = 0; i < dailyBuckets.length - dayOffset; i++)
          {
            newDailyBuckets[i] = dailyBuckets[i + dayOffset];
          }

        //
        //  create newMonthlyBuckets
        //

        long[] newMonthlyBuckets = allocateBuckets(metricHistoryMode, monthlyBuckets.length);
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
        beginningOfBaseMonth = RLMDateUtils.truncate(baseDay, Calendar.MONTH, Deployment.getBaseTimeZone());
        beginningOfDailyValues = RLMDateUtils.addDays(baseDay, -1*(dailyBuckets.length-1), Deployment.getBaseTimeZone());
        beginningOfMonthlyValues = RLMDateUtils.addMonths(beginningOfBaseMonth, -1*monthlyBuckets.length, Deployment.getBaseTimeZone());
        endOfMonthlyValues = RLMDateUtils.addDays(beginningOfBaseMonth, -1, Deployment.getBaseTimeZone());
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
        switch (metricHistoryMode)
          {
            case Standard:
              dailyBuckets[bucketIndex] += value;
              initialized = true;
              break;

            case Max:
              dailyBuckets[bucketIndex] = (dailyBuckets[bucketIndex] >= 0L) ? Math.max(dailyBuckets[bucketIndex], value) : value;
              initialized = true;
              break;
              
            case Min:
              long threshold = (briefcase != null) ? ((Long) briefcase).longValue() : 0;
              if (value >= threshold)
                {
                  dailyBuckets[bucketIndex] = (dailyBuckets[bucketIndex] >= 0L) ? Math.min(dailyBuckets[bucketIndex], value) : value;
                  initialized = true;
                }              
              break;
              
            default:
              throw new RuntimeException();
          }
      }
    
    //
    //  monthlyBucket
    //

    if (beginningOfCurrentMonth.compareTo(beginningOfMonthlyValues) >= 0 && beginningOfCurrentMonth.compareTo(beginningOfBaseMonth) < 0)
      {
        int bucketIndex = monthlyBuckets.length - RLMDateUtils.monthsBetween(beginningOfCurrentMonth, beginningOfBaseMonth, Deployment.getBaseTimeZone());
        switch (metricHistoryMode)
          {
            case Standard:
              monthlyBuckets[bucketIndex] += value;
              initialized = true;
              break;

            case Max:
              monthlyBuckets[bucketIndex] = (monthlyBuckets[bucketIndex] >= 0L) ? Math.max(monthlyBuckets[bucketIndex], value) : value;
              initialized = true;
              break;
              
            case Min:
              long threshold = (briefcase != null) ? ((Long) briefcase).longValue() : 0;
              if (value >= threshold)
                {
                  monthlyBuckets[bucketIndex] = (monthlyBuckets[bucketIndex] >= 0L) ? Math.min(monthlyBuckets[bucketIndex], value) : value;
                  initialized = true;
                }              
              break;
              
            default:
              throw new RuntimeException();
          }
      }
    
    //
    //  allTimeBucket
    //

    switch (metricHistoryMode)
      {
        case Standard:
          allTimeBucket += value;
          initialized = true;
          break;

        case Max:
          allTimeBucket = (allTimeBucket >= 0L) ? Math.max(allTimeBucket, value) : value;
          initialized = true;
          break;

        case Min:
          long threshold = (briefcase != null) ? ((Long) briefcase).longValue() : 0;
          if (value >= threshold)
            {
              allTimeBucket = (allTimeBucket >= 0L) ? Math.min(allTimeBucket, value) : value;
              initialized = true;
            }              
          break;

        default:
            throw new RuntimeException();
      }
  }

  /*****************************************
  *
  *  getValue
  *
  *  startDay - legal values are
  *    A - the beginning of time (NULL)
  *    B - the first of the previous "3" months
  *    C - any of the last "34" days
  *    D - any date in the future
  *
  *  endDay - legal values are
  *    A - the last of the previous "3" months
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
  
  // @rl: is there really a difference between case C & D for endDate (e.g. in the future) or is it just useless complexity 

  private enum DateCase { A, B, C, D }

  public synchronized Long getValue(Date startDay, Date endDay) throws IllegalArgumentException
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
    else if (monthlyStartDays.contains(startDay) && (endDay == null || startDay.before(endDay)))
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
    else if (monthlyEndDays.contains(endDay) && (startDay == null || startDay.before(endDay)))
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

    Long result = null;
    switch (metricHistoryMode)
      {
        case Standard:
          {
            result = 0L;
            switch (startDayCase)
              {
                case A:
                  switch (endDayCase)
                    {
                      case A:
                        result = allTimeBucket - aggregateMonthlyValues(RLMDateUtils.addDays(endDay, 1, Deployment.getBaseTimeZone()), endOfMonthlyValues) - aggregateDailyValues(beginningOfBaseMonth,baseDay);
                        break;
                      case B:
                        result = allTimeBucket - aggregateDailyValues(RLMDateUtils.addDays(endDay, 1, Deployment.getBaseTimeZone()), baseDay);
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
                        result = aggregateMonthlyValues(startDay,endDay);
                        break;
                      case B:
                        result = aggregateMonthlyValues(startDay,endOfMonthlyValues) + aggregateDailyValues(beginningOfBaseMonth,endDay);
                        break;
                      case C:
                        result = aggregateMonthlyValues(startDay,endOfMonthlyValues) + aggregateDailyValues(beginningOfBaseMonth,baseDay);
                        break;
                      case D:
                        result = aggregateMonthlyValues(startDay,endOfMonthlyValues) + aggregateDailyValues(beginningOfBaseMonth,baseDay);
                        break;
                    }
                  break;

                case C:
                  switch (endDayCase)
                    {
                      case A:
                        result = aggregateDailyValues(startDay, endDay);
                        break;
                      case B:
                        result = aggregateDailyValues(startDay, endDay);
                        break;
                      case C:
                        result = aggregateDailyValues(startDay, baseDay);
                        break;
                      case D:
                        result = aggregateDailyValues(startDay, baseDay);
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
                        result = 0L;
                        break;
                      case D:
                        result = 0L;
                        break;
                    }
                  break;
              }
          }
          break;

        case Max:
          {
            result = Long.MIN_VALUE;
            switch (startDayCase)
              {
                case A:
                  switch (endDayCase)
                    {
                      case A:
                      case B:
                      case C:
                        throw new IllegalArgumentException("unavailable Start/End combination");
                      case D:
                        result = (allTimeBucket >= 0) ? allTimeBucket : Long.MIN_VALUE;
                        break;
                    }
                  break;

                case B:
                  switch (endDayCase)
                    {
                      case A:
                        result = aggregateMonthlyValues(startDay,endDay);
                        break;
                      case B:
                        result = Math.max(aggregateMonthlyValues(startDay,endOfMonthlyValues), aggregateDailyValues(beginningOfBaseMonth,endDay));
                        break;
                      case C:
                        result = Math.max(aggregateMonthlyValues(startDay,endOfMonthlyValues), aggregateDailyValues(beginningOfBaseMonth,baseDay));
                        break;
                      case D:
                        result = Math.max(aggregateMonthlyValues(startDay,endOfMonthlyValues), aggregateDailyValues(beginningOfBaseMonth,baseDay));
                        break;
                    }
                  break;

                case C:
                  switch (endDayCase)
                    {
                      case A:
                        result = aggregateDailyValues(startDay, endDay);
                        break;
                      case B:
                        result = aggregateDailyValues(startDay, endDay);
                        break;
                      case C:
                        result = aggregateDailyValues(startDay, baseDay);
                        break;
                      case D:
                        result = aggregateDailyValues(startDay, baseDay);
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
                        result = Long.MIN_VALUE;
                        break;
                      case D:
                        result = Long.MIN_VALUE;
                        break;
                    }
                  break;
              }
            result = (result > Long.MIN_VALUE) ? result : null;
          }
          break;

        case Min:
          {
            result = Long.MAX_VALUE;
            switch (startDayCase)
              {
                case A:
                  switch (endDayCase)
                    {
                      case A:
                      case B:
                      case C:
                        throw new IllegalArgumentException("unavailable Start/End combination");
                      case D:
                        result = (allTimeBucket >= 0) ? allTimeBucket : Long.MAX_VALUE;
                        break;
                    }
                  break;

                case B:
                  switch (endDayCase)
                    {
                      case A:
                        result = aggregateMonthlyValues(startDay,endDay);
                        break;
                      case B:
                        result = Math.min(aggregateMonthlyValues(startDay,endOfMonthlyValues), aggregateDailyValues(beginningOfBaseMonth,endDay));
                        break;
                      case C:
                        result = Math.min(aggregateMonthlyValues(startDay,endOfMonthlyValues), aggregateDailyValues(beginningOfBaseMonth,baseDay));
                        break;
                      case D:
                        result = Math.min(aggregateMonthlyValues(startDay,endOfMonthlyValues), aggregateDailyValues(beginningOfBaseMonth,baseDay));
                        break;
                    }
                  break;

                case C:
                  switch (endDayCase)
                    {
                      case A:
                        result = aggregateDailyValues(startDay, endDay);
                        break;
                      case B:
                        result = aggregateDailyValues(startDay, endDay);
                        break;
                      case C:
                        result = aggregateDailyValues(startDay, baseDay);
                        break;
                      case D:
                        result = aggregateDailyValues(startDay, baseDay);
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
                        result = Long.MAX_VALUE;
                        break;
                      case D:
                        result = Long.MAX_VALUE;
                        break;
                    }
                  break;
              }
            result = (result < Long.MAX_VALUE) ? result : null;
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

  private static ThreadLocal<Map<Pair<Date,Date>,Set<Date>>> monthlyStartDaysCache = ThreadLocal.withInitial(()->new LinkedHashMap<Pair<Date,Date>,Set<Date>>() { @Override protected boolean removeEldestEntry(Map.Entry eldest) { return size() > 100; } });
  private static Set<Date> getMonthlyStartDays(Date firstMonth, Date baseMonth)
  {
    Pair<Date,Date> key = new Pair<Date,Date>(firstMonth, baseMonth);
    Set<Date> result = monthlyStartDaysCache.get().get(key);
    if (result == null)
      {
        result = new HashSet<Date>();
        for (Date month = firstMonth; month.before(baseMonth); month = RLMDateUtils.addMonths(month, 1, Deployment.getBaseTimeZone()))
          {
            result.add(month);
          }
        monthlyStartDaysCache.get().put(key,result);
      }
    return result;
  }

  /****************************************
  *
  *  getMonthlyEndDays
  *
  ****************************************/

  private static ThreadLocal<Map<Pair<Date,Date>,Set<Date>>> monthlyEndDaysCache = ThreadLocal.withInitial(()->new LinkedHashMap<Pair<Date,Date>,Set<Date>>() { @Override protected boolean removeEldestEntry(Map.Entry eldest) { return size() > 100; } });
  private static Set<Date> getMonthlyEndDays(Date firstMonth, Date baseMonth)
  {
    Pair<Date,Date> key = new Pair<Date,Date>(firstMonth, baseMonth);
    Set<Date> result = monthlyEndDaysCache.get().get(key);
    if (result == null)
      {
        result = new HashSet<Date>();
        for (Date month = firstMonth; month.before(baseMonth); month = RLMDateUtils.addMonths(month, 1, Deployment.getBaseTimeZone()))
          {
            result.add(RLMDateUtils.addDays(RLMDateUtils.addMonths(month, 1, Deployment.getBaseTimeZone()), -1, Deployment.getBaseTimeZone()));
          }
        monthlyEndDaysCache.get().put(key,result);
      }
    return result;
  }
  
  /****************************************
  *
  *  aggregateDailyValues
  *
  ****************************************/

  private long aggregateDailyValues(Date startDay, Date endDay)
  {
    //
    //  initialize result
    //
    
    long result;
    switch (metricHistoryMode)
      {
        case Standard:
          result = 0L;
          break;

        case Max:
          result = Long.MIN_VALUE;
          break;

        case Min:
          result = Long.MAX_VALUE;
          break;

        default:
          throw new RuntimeException();
      }

    //
    //  aggregate
    //
        
    int bucketIndex = dailyBuckets.length - RLMDateUtils.daysBetween(startDay, baseDay, Deployment.getBaseTimeZone()) - 1;
    Date bucketDay = startDay;
    while (bucketDay.compareTo(endDay) <= 0)
      {
        if (bucketDay.compareTo(startDay) >= 0)
          {
            switch (metricHistoryMode)
              {
                case Standard:
                  result += dailyBuckets[bucketIndex];
                  break;

                case Max:
                  result = (dailyBuckets[bucketIndex] >= 0L) ? Math.max(dailyBuckets[bucketIndex], result) : result;
                  break;

                case Min:
                  result = (dailyBuckets[bucketIndex] >= 0L) ? Math.min(dailyBuckets[bucketIndex], result) : result;
                  break;
              }
          }
        bucketIndex += 1;
        bucketDay = RLMDateUtils.addDays(bucketDay, 1, Deployment.getBaseTimeZone());
      }
    return result;
  }
  
  /****************************************
  *
  *  aggregateMonthlyValues
  *
  ****************************************/

  private long aggregateMonthlyValues(Date startDay, Date endDay)
  {
    //
    //  initialize result
    //
    
    long result;
    switch (metricHistoryMode)
      {
        case Standard:
          result = 0L;
          break;

        case Max:
          result = Long.MIN_VALUE;
          break;

        case Min:
          result = Long.MAX_VALUE;
          break;

        default:
          throw new RuntimeException();
      }

    //
    //  aggregate
    //
        
    int bucketIndex = 0;
    Date bucketMonth = beginningOfMonthlyValues;
    while (bucketMonth.before(endDay))
      {
        if (bucketMonth.compareTo(startDay) >= 0)
          {
            switch (metricHistoryMode)
              {
                case Standard:
                  result += monthlyBuckets[bucketIndex];
                  break;

                case Max:
                  result = (monthlyBuckets[bucketIndex] >= 0L) ? Math.max(monthlyBuckets[bucketIndex], result) : result;
                  break;

                case Min:
                  result = (monthlyBuckets[bucketIndex] >= 0L) ? Math.min(monthlyBuckets[bucketIndex], result) : result;
                  break;
              }
          }
        bucketIndex += 1;
        bucketMonth = RLMDateUtils.addMonths(bucketMonth, 1, Deployment.getBaseTimeZone());
      }
    return result;
  }

  /****************************************
  *
  *  aggregateIf
  *
  ****************************************/

  public synchronized Long aggregateIf(Date startDay, Date endDay, Criteria criteria, MetricHistory criteriaMetricHistory) throws IllegalArgumentException
  {
    /****************************************
    *
    *  validate input
    *
    ****************************************/

    //
    //  startDay
    //

    if (startDay == null || ! Objects.equals(startDay, RLMDateUtils.truncate(startDay, Calendar.DATE, Calendar.SUNDAY, Deployment.getBaseTimeZone())))
      {
        throw new IllegalArgumentException("startDay must be on a day boundary");
      }

    //
    //  endDay
    //
    
    if (endDay == null || ! Objects.equals(endDay, RLMDateUtils.truncate(endDay, Calendar.DATE, Calendar.SUNDAY, Deployment.getBaseTimeZone())))
      {
        throw new IllegalArgumentException("endDay must be on a day boundary");
      }

    //
    //  startDay on/before endDay
    //

    if (startDay.after(endDay))
      {
        throw new IllegalArgumentException("startDay after endDay");
      }

    //
    //  criteriaMetricHistory uses Standard mode
    //

    if (criteriaMetricHistory.getMetricHistoryMode() != MetricHistoryMode.Standard)
      {
        throw new IllegalArgumentException("criteria metric history mode must be standard");
      }
    
    /*****************************************
    *
    *  initialize result
    *
    *****************************************/
    
    long result;
    switch (metricHistoryMode)
      {
        case Standard:
          result = 0L;
          break;

        case Max:
          result = Long.MIN_VALUE;
          break;

        case Min:
          result = Long.MAX_VALUE;
          break;

        default:
          throw new RuntimeException();
      }

    /*****************************************
    *
    *  aggregate result
    *
    *****************************************/
    
    Date bucketDay = startDay;
    while (bucketDay.compareTo(endDay) <= 0)
      {
        //
        //  criteria passes on provided day?
        //
        
        boolean passesCriteria = false;
        switch (criteria)
          {
            case IsZero:
              passesCriteria = criteriaMetricHistory.getValue(bucketDay, bucketDay) == 0;
              break;
              
            case IsNonZero:
              passesCriteria = criteriaMetricHistory.getValue(bucketDay, bucketDay) > 0;
              break;
          }
        
        //
        //  aggregate (if necessary)
        //
        
        if (passesCriteria)
          {
            switch (metricHistoryMode)
              {
                case Standard:
                  result += getValue(bucketDay, bucketDay);
                  break;

                case Max:
                  Long valueMax = getValue(bucketDay, bucketDay);
                  result = (valueMax >= 0L) ? Math.max(valueMax, result) : result;
                  break;

                case Min:
                  Long valueMin = getValue(bucketDay, bucketDay);
                  result = (valueMin >= 0L) ? Math.min(valueMin, result) : result;
                  break;
              }
          }
        bucketDay = RLMDateUtils.addDays(bucketDay, 1, Deployment.getBaseTimeZone());
      }
    
    /*****************************************
    *
    *  return result
    *
    *****************************************/

    return result;
  }

  /****************************************
  *
  *  countIf
  *
  ****************************************/

  public synchronized Long countIf(Date startDay, Date endDay, Criteria criteria) throws IllegalArgumentException
  {
    /****************************************
    *
    *  validate input
    *
    ****************************************/

    //
    //  startDay
    //

    if (startDay == null || ! Objects.equals(startDay, RLMDateUtils.truncate(startDay, Calendar.DATE, Calendar.SUNDAY, Deployment.getBaseTimeZone())))
      {
        throw new IllegalArgumentException("startDay must be on a day boundary");
      }

    //
    //  endDay
    //
    
    if (endDay == null || ! Objects.equals(endDay, RLMDateUtils.truncate(endDay, Calendar.DATE, Calendar.SUNDAY, Deployment.getBaseTimeZone())))
      {
        throw new IllegalArgumentException("endDay must be on a day boundary");
      }

    //
    //  startDay on/before endDay
    //

    if (startDay.after(endDay))
      {
        throw new IllegalArgumentException("startDay after endDay");
      }

    //
    //  Standard mode
    //

    if (metricHistoryMode != MetricHistoryMode.Standard)
      {
        throw new IllegalArgumentException("metric history mode must be standard");
      }
    
    /*****************************************
    *
    *  initialize result
    *
    *****************************************/
    
    long result = 0;

    /*****************************************
    *
    *  aggregate result
    *
    *****************************************/
    
    Date bucketDay = startDay;
    while (bucketDay.compareTo(endDay) <= 0)
      {
        //
        //  criteria passes on provided day?
        //
        
        boolean passesCriteria = false;
        switch (criteria)
          {
            case IsZero:
              passesCriteria = getValue(bucketDay, bucketDay) == 0;
              break;
              
            case IsNonZero:
              passesCriteria = getValue(bucketDay, bucketDay) > 0;
              break;
          }
        
        //
        //  count (if necessary)
        //
        
        if (passesCriteria)
          {
            result += 1;
          }
        bucketDay = RLMDateUtils.addDays(bucketDay, 1, Deployment.getBaseTimeZone());
      }
    
    /*****************************************
    *
    *  return result
    *
    *****************************************/

    return result;
  }

  /****************************************
  *
  *  Resize
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

    long[] newDailyBuckets = allocateBuckets(metricHistoryMode, numberOfDailyBuckets);
    for (int i = 0; i < Math.min(numberOfDailyBuckets, dailyBuckets.length); i++)
      {
        newDailyBuckets[numberOfDailyBuckets-i-1] = dailyBuckets[dailyBuckets.length-i-1];
      }
    dailyBuckets = newDailyBuckets;

    //
    //  create newMonthlyBuckets
    //

    long[] newMonthlyBuckets = allocateBuckets(metricHistoryMode, numberOfMonthlyBuckets);
    for (int i = 0; i < Math.min(numberOfMonthlyBuckets, monthlyBuckets.length); i++)
      {
        newMonthlyBuckets[numberOfMonthlyBuckets-i-1] = monthlyBuckets[monthlyBuckets.length-i-1];
      }
    monthlyBuckets = newMonthlyBuckets;
  }
  
  /****************************************
  *
  *  toString
  *
  ****************************************/

  public String toString()
  {
    StringBuilder builder = new StringBuilder();
    builder.append("{ baseDay=" + baseDay + ", mode=" + metricHistoryMode + ", dailyBuckets=[");
    for (int i = 0; i < dailyBuckets.length; i++)
      {
        if (i > 0) builder.append(",");
        builder.append(dailyBuckets[i]);
      }
    builder.append("], monthlyBuckets=[");
    for (int i = 0; i < monthlyBuckets.length; i++)
      {
        if (i > 0) builder.append(",");
        builder.append(monthlyBuckets[i]);
      }
    builder.append("], allTime=" + allTimeBucket + " }");
    return builder.toString();
  }
}
