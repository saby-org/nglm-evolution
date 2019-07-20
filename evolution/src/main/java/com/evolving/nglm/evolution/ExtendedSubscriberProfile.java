/*****************************************************************************
*
*  ExtendedSubscriberProfile.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.Pair;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.SubscriberStreamOutput;
import com.evolving.nglm.core.SubscriberTrace;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import com.evolving.nglm.evolution.SubscriberProfile.ValidateUpdateProfileRequestException;

public abstract class ExtendedSubscriberProfile
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(ExtendedSubscriberProfile.class);

  /*****************************************
  *
  *  schema
  *
  *****************************************/

  //
  //  schema
  //

  private static Schema commonSchema = null;
  static
  {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(2));
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("subscriberTraceEnabled", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("subscriberTraceMessage", Schema.OPTIONAL_STRING_SCHEMA);
    commonSchema = schemaBuilder.build();
  };

  //
  //  accessor
  //

  public static Schema commonSchema() { return commonSchema; }

  /*****************************************
  *
  *  extended subscriber profile
  *
  *****************************************/

  //
  //  methods
  //

  private static Constructor extendedSubscriberProfileConstructor;
  private static Constructor extendedSubscriberProfileCopyConstructor;
  private static ConnectSerde<ExtendedSubscriberProfile> extendedSubscriberProfileSerde;
  static
  {
    try
      {
        Class<ExtendedSubscriberProfile> extendedSubscriberProfileClass = Deployment.getExtendedSubscriberProfileClass();
        extendedSubscriberProfileConstructor = extendedSubscriberProfileClass.getDeclaredConstructor(String.class);
        extendedSubscriberProfileCopyConstructor = extendedSubscriberProfileClass.getDeclaredConstructor(extendedSubscriberProfileClass);
        Method serdeMethod = extendedSubscriberProfileClass.getMethod("serde");
        extendedSubscriberProfileSerde = (ConnectSerde<ExtendedSubscriberProfile>) serdeMethod.invoke(null);
      }
    catch (InvocationTargetException e)
      {
        throw new RuntimeException(e.getCause());
      }
    catch (NoSuchMethodException|IllegalAccessException e)
      {
        throw new RuntimeException(e);
      }
  }

  //
  //  accessors
  //

  static Constructor getExtendedSubscriberProfileConstructor() { return extendedSubscriberProfileConstructor; }
  static Constructor getExtendedSubscriberProfileCopyConstructor() { return extendedSubscriberProfileCopyConstructor; }
  static ConnectSerde<ExtendedSubscriberProfile> getExtendedSubscriberProfileSerde() { return extendedSubscriberProfileSerde; }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private String subscriberID;
  private boolean subscriberTraceEnabled;
  private SubscriberTrace subscriberTrace;

  /****************************************
  *
  *  accessors - basic
  *
  ****************************************/

  public String getSubscriberID() { return subscriberID; }
  public boolean getSubscriberTraceEnabled() { return subscriberTraceEnabled; }
  public SubscriberTrace getSubscriberTrace() { return subscriberTrace; }

  /*****************************************
  *
  *  setters
  *
  *****************************************/

  public void setSubscriberTrace(SubscriberTrace subscriberTrace) { this.subscriberTrace = subscriberTrace; }

  /*****************************************
  *
  *  abstract
  *
  *****************************************/

  protected abstract void addProfileFieldsForGUIPresentation(Map<String, Object> baseProfilePresentation, Map<String, Object> kpiPresentation);
  protected abstract void addProfileFieldsForThirdPartyPresentation(Map<String, Object> baseProfilePresentation, Map<String, Object> kpiPresentation);
  protected abstract void validateUpdateProfileRequestFields(JSONObject jsonRoot) throws ValidateUpdateProfileRequestException;

  /****************************************
  *
  *  getHistory utilities
  *
  ****************************************/

  //
  //  getYesterday
  //

  protected Long getYesterday(MetricHistory metricHistory, Date evaluationDate)
  {
    Date day = RLMDateUtils.truncate(evaluationDate, Calendar.DATE, Calendar.SUNDAY, Deployment.getBaseTimeZone());
    Date startDay = RLMDateUtils.addDays(day, -1, Deployment.getBaseTimeZone());
    Date endDay = startDay;
    return metricHistory.getValue(startDay, endDay);
  }

  //
  //  getPrevious7Days
  //

  protected Long getPrevious7Days(MetricHistory metricHistory, Date evaluationDate)
  {
    Date day = RLMDateUtils.truncate(evaluationDate, Calendar.DATE, Calendar.SUNDAY, Deployment.getBaseTimeZone());
    Date startDay = RLMDateUtils.addDays(day, -7, Deployment.getBaseTimeZone());
    Date endDay = RLMDateUtils.addDays(day, -1, Deployment.getBaseTimeZone());
    return metricHistory.getValue(startDay, endDay);
  }

  //
  //  getPrevious14Days
  //

  protected Long getPrevious14Days(MetricHistory metricHistory, Date evaluationDate)
  {
    Date day = RLMDateUtils.truncate(evaluationDate, Calendar.DATE, Calendar.SUNDAY, Deployment.getBaseTimeZone());
    Date startDay = RLMDateUtils.addDays(day, -14, Deployment.getBaseTimeZone());
    Date endDay = RLMDateUtils.addDays(day, -1, Deployment.getBaseTimeZone());
    return metricHistory.getValue(startDay, endDay);
  }

  //
  //  getPreviousMonth
  //

  protected Long getPreviousMonth(MetricHistory metricHistory, Date evaluationDate)
  {
    Date day = RLMDateUtils.truncate(evaluationDate, Calendar.DATE, Calendar.SUNDAY, Deployment.getBaseTimeZone());
    Date startOfMonth = RLMDateUtils.truncate(day, Calendar.MONTH, Calendar.SUNDAY, Deployment.getBaseTimeZone());
    Date startDay = RLMDateUtils.addMonths(startOfMonth, -1, Deployment.getBaseTimeZone());
    Date endDay = RLMDateUtils.addDays(startOfMonth, -1, Deployment.getBaseTimeZone());
    return metricHistory.getValue(startDay, endDay);
  }

  //
  //  getPrevious90Days
  //

  protected Long getPrevious90Days(MetricHistory metricHistory, Date evaluationDate)
  {
    Date day = RLMDateUtils.truncate(evaluationDate, Calendar.DATE, Calendar.SUNDAY, Deployment.getBaseTimeZone());
    Date startDay = RLMDateUtils.addDays(day, -90, Deployment.getBaseTimeZone());
    Date endDay = RLMDateUtils.addDays(day, -1, Deployment.getBaseTimeZone());
    return metricHistory.getValue(startDay, endDay);
  }

  //
  //  getAggregateIfZeroPrevious90Days
  //

  protected Long getAggregateIfZeroPrevious90Days(MetricHistory metricHistory, MetricHistory criteriaMetricHistory, Date evaluationDate)
  {
    Date day = RLMDateUtils.truncate(evaluationDate, Calendar.DATE, Calendar.SUNDAY, Deployment.getBaseTimeZone());
    Date startDay = RLMDateUtils.addDays(day, -90, Deployment.getBaseTimeZone());
    Date endDay = RLMDateUtils.addDays(day, -1, Deployment.getBaseTimeZone());
    return metricHistory.aggregateIf(startDay, endDay, MetricHistory.Criteria.IsZero, criteriaMetricHistory);
  }

  //
  //  getAggregateIfNonZeroPrevious90Days
  //

  protected Long getAggregateIfNonZeroPrevious90Days(MetricHistory metricHistory, MetricHistory criteriaMetricHistory, Date evaluationDate)
  {
    Date day = RLMDateUtils.truncate(evaluationDate, Calendar.DATE, Calendar.SUNDAY, Deployment.getBaseTimeZone());
    Date startDay = RLMDateUtils.addDays(day, -90, Deployment.getBaseTimeZone());
    Date endDay = RLMDateUtils.addDays(day, -1, Deployment.getBaseTimeZone());
    return metricHistory.aggregateIf(startDay, endDay, MetricHistory.Criteria.IsNonZero, criteriaMetricHistory);
  }

  //
  //  getThreeMonthAverage
  //

  protected Long getThreeMonthAverage(MetricHistory metricHistory, Date evaluationDate)
  {
    //
    //  undefined
    //

    switch (metricHistory.getMetricHistoryMode())
      {
        case Min:
        case Max:
          return null;
      }

    //
    //  retrieve values by month
    //

    int numberOfMonths = 3;
    Date day = RLMDateUtils.truncate(evaluationDate, Calendar.DATE, Calendar.SUNDAY, Deployment.getBaseTimeZone());
    Date startOfMonth = RLMDateUtils.truncate(day, Calendar.MONTH, Calendar.SUNDAY, Deployment.getBaseTimeZone());
    long[] valuesByMonth = new long[numberOfMonths];
    for (int i=0; i<numberOfMonths; i++)
      {
        Date startDay = RLMDateUtils.addMonths(startOfMonth, -(i+1), Deployment.getBaseTimeZone());
        Date endDay = RLMDateUtils.addDays(RLMDateUtils.addMonths(startDay, 1, Deployment.getBaseTimeZone()), -1, Deployment.getBaseTimeZone());
        valuesByMonth[i] = metricHistory.getValue(startDay, endDay);
      }

    //
    //  average (excluding "leading" zeroes)
    //

    long totalValue = 0L;
    int includedMonths = 0;
    for (int i=numberOfMonths-1; i>=0; i--)
      {
        totalValue += valuesByMonth[i];
        if (totalValue > 0L) includedMonths += 1;
      }

    //
    //  result
    //

    return (includedMonths > 0) ? totalValue / includedMonths : 0L;
  }

  /****************************************
  *
  *  setters
  *
  ****************************************/

  public void setSubscriberTraceEnabled(boolean subscriberTraceEnabled) { this.subscriberTraceEnabled = subscriberTraceEnabled; }

  /*****************************************
  *
  *  creete (static constructor)
  *
  *****************************************/

  public static ExtendedSubscriberProfile create(String subscriberID)
  {
    ExtendedSubscriberProfile extendedSubscriberProfile = null;
    try
      {
        extendedSubscriberProfile = (ExtendedSubscriberProfile) extendedSubscriberProfileConstructor.newInstance(subscriberID);
      }
    catch (InvocationTargetException e)
      {
        throw new RuntimeException(e.getCause());
      }
    catch (InstantiationException|IllegalAccessException e)
      {
        throw new RuntimeException(e);
      }
    return extendedSubscriberProfile;
  }

  /*****************************************
  *
  *  copy (static constructor)
  *
  *****************************************/

  public static ExtendedSubscriberProfile copy(ExtendedSubscriberProfile originalExtendedSubscriberProfile)
  {
    ExtendedSubscriberProfile extendedSubscriberProfile = null;
    try
      {
        extendedSubscriberProfile = (ExtendedSubscriberProfile) extendedSubscriberProfileCopyConstructor.newInstance(originalExtendedSubscriberProfile);
      }
    catch (InvocationTargetException e)
      {
        throw new RuntimeException(e.getCause());
      }
    catch (InstantiationException|IllegalAccessException e)
      {
        throw new RuntimeException(e);
      }
    return extendedSubscriberProfile;
  }

  /*****************************************
  *
  *  constructor (simple)
  *
  *****************************************/

  protected ExtendedSubscriberProfile(String subscriberID)
  {
    this.subscriberID = subscriberID;
    this.subscriberTraceEnabled = false;
    this.subscriberTrace = null;
  }

  /*****************************************
  *
  *  constructor (unpack)
  *
  *****************************************/

  protected ExtendedSubscriberProfile(SchemaAndValue schemaAndValue)
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
    String subscriberID = valueStruct.getString("subscriberID");
    Boolean subscriberTraceEnabled = valueStruct.getBoolean("subscriberTraceEnabled");
    SubscriberTrace subscriberTrace = valueStruct.getString("subscriberTraceMessage") != null ? new SubscriberTrace(valueStruct.getString("subscriberTraceMessage")) : null;

    //
    //  return
    //

    this.subscriberID = subscriberID;
    this.subscriberTraceEnabled = subscriberTraceEnabled;
    this.subscriberTrace = subscriberTrace;
  }

  /*****************************************
  *
  *  constructor (copy)
  *
  *****************************************/

  protected ExtendedSubscriberProfile(ExtendedSubscriberProfile extendedSubscriberProfile)
  {
    this.subscriberID = extendedSubscriberProfile.getSubscriberID();
    this.subscriberTraceEnabled = extendedSubscriberProfile.getSubscriberTraceEnabled();
    this.subscriberTrace = extendedSubscriberProfile.getSubscriberTrace();
  }

  /*****************************************
  *
  *  packCommon
  *
  *****************************************/

  protected static void packCommon(Struct struct, ExtendedSubscriberProfile extendedSubscriberProfile)
  {
    struct.put("subscriberID", extendedSubscriberProfile.getSubscriberID());
    struct.put("subscriberTraceEnabled", extendedSubscriberProfile.getSubscriberTraceEnabled());
    struct.put("subscriberTraceMessage", extendedSubscriberProfile.getSubscriberTrace() != null ? extendedSubscriberProfile.getSubscriberTrace().getSubscriberTraceMessage() : null);
  }

  /*****************************************
  *
  *  toString
  *
  *****************************************/
  
  /*****************************************
  *
  *  readString
  *
  *****************************************/
  
  protected String readString(JSONObject jsonRoot, String key, boolean validateNotEmpty) throws ValidateUpdateProfileRequestException
  {
    String result = readString(jsonRoot, key);
    if (validateNotEmpty && (result == null || result.trim().isEmpty()) && jsonRoot.containsKey(key))
      {
        throw new ValidateUpdateProfileRequestException(RESTAPIGenericReturnCodes.BAD_FIELD_VALUE.getGenericResponseMessage() + " ("+key+") ", RESTAPIGenericReturnCodes.BAD_FIELD_VALUE.getGenericResponseCode());
      }
    return result;
  }
  
  /*****************************************
  *
  *  readString
  *
  *****************************************/
  
  protected String readString(JSONObject jsonRoot, String key) throws ValidateUpdateProfileRequestException
  {
    String result = null;
    try 
      {
        result = JSONUtilities.decodeString(jsonRoot, key, false);
      }
    catch (JSONUtilitiesException e) 
      {
        throw new ValidateUpdateProfileRequestException(RESTAPIGenericReturnCodes.BAD_FIELD_VALUE.getGenericResponseMessage() + " ("+key+") ", RESTAPIGenericReturnCodes.BAD_FIELD_VALUE.getGenericResponseCode());
      }
    return result;
  }
  
  /*****************************************
  *
  *  readInteger
  *
  *****************************************/
  
  protected Integer readInteger(JSONObject jsonRoot, String key, boolean validateNotEmpty) throws ValidateUpdateProfileRequestException
  {
    Integer result = readInteger(jsonRoot, key);
    if (validateNotEmpty && (result == null) && jsonRoot.containsKey(key))
      {
        throw new ValidateUpdateProfileRequestException(RESTAPIGenericReturnCodes.BAD_FIELD_VALUE.getGenericResponseMessage() + " ("+key+") ", RESTAPIGenericReturnCodes.BAD_FIELD_VALUE.getGenericResponseCode());
      }
    return result;
  }
  
  /*****************************************
  *
  *  readInteger
  *
  *****************************************/
  
  protected Integer readInteger(JSONObject jsonRoot, String key) throws ValidateUpdateProfileRequestException
  {
    Integer result = null;
    try 
      {
        result = JSONUtilities.decodeInteger(jsonRoot, key);
      }
    catch (JSONUtilitiesException e) 
      {
        throw new ValidateUpdateProfileRequestException(RESTAPIGenericReturnCodes.BAD_FIELD_VALUE.getGenericResponseMessage() + " ("+key+") ", RESTAPIGenericReturnCodes.BAD_FIELD_VALUE.getGenericResponseCode());
      }
    return result;
  }
  

  //
  //  toString
  //

  public String toString(ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader)
  {
    Date now = SystemTime.getCurrentTime();
    StringBuilder b = new StringBuilder();
    b.append("ExtendedSubscriberProfile:{");
    b.append(commonToString(subscriberGroupEpochReader));
    b.append("}");
    return b.toString();
  }

  //
  //  commonToString
  //

  protected String commonToString(ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader)
  {
    StringBuilder b = new StringBuilder();
    b.append(subscriberID);
    b.append("," + subscriberTraceEnabled);
    return b.toString();
  }
}
