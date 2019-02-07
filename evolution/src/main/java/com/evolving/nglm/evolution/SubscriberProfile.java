/*****************************************************************************
*
*  SubscriberProfile.java
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
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

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
import com.evolving.nglm.core.SystemTime;

public abstract class SubscriberProfile implements SubscriberStreamOutput
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(SubscriberProfile.class);

  /*****************************************
  *
  *  enum
  *
  *****************************************/

  //
  //  EvolutionSubscriberStatus
  //

  public enum EvolutionSubscriberStatus
  {
    Active("active"),
    Inactive("inactive"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private EvolutionSubscriberStatus(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static EvolutionSubscriberStatus fromExternalRepresentation(String externalRepresentation) { for (EvolutionSubscriberStatus enumeratedValue : EvolutionSubscriberStatus.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }

  //
  //  CompressionType
  //

  public enum CompressionType
  {
    None("none", 0),
    GZip("gzip", 1),
    Unknown("(unknown)", 99);
    private String stringRepresentation;
    private int externalRepresentation;
    private CompressionType(String stringRepresentation, int externalRepresentation) { this.stringRepresentation = stringRepresentation; this.externalRepresentation = externalRepresentation; }
    public String getStringRepresentation() { return stringRepresentation; }
    public int getExternalRepresentation() { return externalRepresentation; }
    public static CompressionType fromStringRepresentation(String stringRepresentation) { for (CompressionType enumeratedValue : CompressionType.values()) { if (enumeratedValue.getStringRepresentation().equalsIgnoreCase(stringRepresentation)) return enumeratedValue; } return Unknown; }
    public static CompressionType fromExternalRepresentation(int externalRepresentation) { for (CompressionType enumeratedValue : CompressionType.values()) { if (enumeratedValue.getExternalRepresentation() ==externalRepresentation) return enumeratedValue; } return Unknown; }
  }

  /*****************************************
  *
  *  static
  *
  *****************************************/

  public final static String UniversalControlGroupID = "universalcontrolgroup";
  public static final byte SubscriberProfileCompressionEpoch = 0;
  
  /*****************************************
  *
  *  schema
  *
  *****************************************/

  //
  //  schema
  //

  private static Schema commonSchema = null;
  private static Schema groupIDSchema = null;
  static
  {
    //
    //  groupID schema
    //

    SchemaBuilder groupIDSchemaBuilder = SchemaBuilder.struct();
    groupIDSchemaBuilder.name("subscribergroup_groupid");
    groupIDSchemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    groupIDSchemaBuilder.field("dimensionID", Schema.STRING_SCHEMA);
    groupIDSchemaBuilder.field("segmentID", Schema.STRING_SCHEMA);
    groupIDSchema = groupIDSchemaBuilder.build();
    
    //
    //  commonSchema
    //

    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("subscriberTraceEnabled", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("evolutionSubscriberStatus", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("evolutionSubscriberStatusChangeDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("previousEvolutionSubscriberStatus", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("subscriberGroups", SchemaBuilder.map(groupIDSchema, Schema.INT32_SCHEMA).name("subscriber_profile_subscribergroups").schema());
    schemaBuilder.field("language", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("subscriberHistory", SubscriberHistory.serde().optionalSchema());
    commonSchema = schemaBuilder.build();
  };

  //
  //  accessor
  //

  public static Schema commonSchema() { return commonSchema; }

  /*****************************************
  *
  *  subscriber profile
  *
  *****************************************/

  //
  //  methods
  //
  
  private static Constructor subscriberProfileConstructor;
  private static Constructor subscriberProfileCopyConstructor;
  private static ConnectSerde<SubscriberProfile> subscriberProfileSerde;
  static
  {
    try
      {
        Class<SubscriberProfile> subscriberProfileClass = Deployment.getSubscriberProfileClass();
        subscriberProfileConstructor = subscriberProfileClass.getDeclaredConstructor(String.class);
        subscriberProfileCopyConstructor = subscriberProfileClass.getDeclaredConstructor(subscriberProfileClass);
        Method serdeMethod = subscriberProfileClass.getMethod("serde");
        subscriberProfileSerde = (ConnectSerde<SubscriberProfile>) serdeMethod.invoke(null);
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

  static Constructor getSubscriberProfileConstructor() { return subscriberProfileConstructor; }
  static Constructor getSubscriberProfileCopyConstructor() { return subscriberProfileCopyConstructor; }
  static ConnectSerde<SubscriberProfile> getSubscriberProfileSerde() { return subscriberProfileSerde; }
  
  /****************************************
  *
  *  data
  *
  ****************************************/

  private String subscriberID;
  private boolean subscriberTraceEnabled;
  private EvolutionSubscriberStatus evolutionSubscriberStatus;
  private Date evolutionSubscriberStatusChangeDate;
  private EvolutionSubscriberStatus previousEvolutionSubscriberStatus;
  private Map<Pair<String,String>,Integer> subscriberGroups; // Map<Pair<dimensionID,segmentID> epoch>>
  private String language;
  private SubscriberHistory subscriberHistory;

  /****************************************
  *
  *  accessors - basic
  *
  ****************************************/

  public String getSubscriberID() { return subscriberID; }
  public boolean getSubscriberTraceEnabled() { return subscriberTraceEnabled; }
  public EvolutionSubscriberStatus getEvolutionSubscriberStatus() { return evolutionSubscriberStatus; }
  public Date getEvolutionSubscriberStatusChangeDate() { return evolutionSubscriberStatusChangeDate; }
  public EvolutionSubscriberStatus getPreviousEvolutionSubscriberStatus() { return previousEvolutionSubscriberStatus; }
  public Map<Pair<String, String>, Integer> getSubscriberGroups() { return subscriberGroups; }
  public boolean getUniversalControlGroup(ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader) { return getInSubscriberGroup(UniversalControlGroupID, subscriberGroupEpochReader); }
  public String getLanguage() { return language; }
  public SubscriberHistory getSubscriberHistory() { return subscriberHistory; }
  
  /*****************************************
  *
  *  abstract
  *
  *****************************************/
  
  protected abstract void addProfileFieldsForGUIPresentation(Map<String, Object> baseProfilePresentation, Map<String, Object> kpiPresentation);
  protected abstract void addProfileFieldsForThirdPartyPresentation(Map<String, Object> baseProfilePresentation, Map<String, Object> kpiPresentation);

  /****************************************
  *
  *  abstract -- scoring
  *
  ****************************************/

  public abstract MetricHistory getDataRevenueAmountMetricHistory();
  public abstract MetricHistory getVoiceRevenueAmountMetricHistory();

  /****************************************
  *
  *  abstract -- identifiers (with default "null" implementations)a
  *
  ****************************************/

  public String getMSISDN() { return null; }

  /****************************************
  *
  *  accessors - subscriberGroups
  *
  ****************************************/

  //
  //  getSubscriberGroups (set of segmentID)
  //

  public Set<String> getSubscriberGroups(ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader)
  {
    Set<String> result = new HashSet<String>();
    for (Pair<String,String> groupID : subscriberGroups.keySet())
      {
        String dimensionID = groupID.getFirstElement();
        String segmentID = groupID.getSecondElement();
        int epoch = subscriberGroups.get(groupID);
        if (epoch >= (subscriberGroupEpochReader.get(dimensionID) != null ? subscriberGroupEpochReader.get(dimensionID).getEpoch() : 0))
          {
            result.add(segmentID);
          }
      }
    return result;
  }
  
  /****************************************
  *
  *  presentation utilities
  *
  ****************************************/
  
  //
  //  getProfileMapForGUIPresentation
  //
  
  public Map<String, Object> getProfileMapForGUIPresentation(ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader)
  {
    HashMap<String, Object> baseProfilePresentation = new HashMap<String,Object>();
    HashMap<String, Object> generalDetailsPresentation = new HashMap<String,Object>();
    HashMap<String, Object> kpiPresentation = new HashMap<String,Object>();
    
    //
    // prepare basic generalDetails
    //
    
    generalDetailsPresentation.put("evolutionSubscriberStatus", (getEvolutionSubscriberStatus() != null) ? getEvolutionSubscriberStatus().getExternalRepresentation() : null);
    generalDetailsPresentation.put("evolutionSubscriberStatusChangeDate", getEvolutionSubscriberStatusChangeDate());
    generalDetailsPresentation.put("previousEvolutionSubscriberStatus", (getPreviousEvolutionSubscriberStatus() != null) ? getPreviousEvolutionSubscriberStatus().getExternalRepresentation() : null);
    List<String> subscriberGroups = new ArrayList<String>(getSubscriberGroups(subscriberGroupEpochReader));
    generalDetailsPresentation.put("subscriberGroups", JSONUtilities.encodeArray(subscriberGroups));
    generalDetailsPresentation.put("language", getLanguage());
    
    //
    // prepare basic kpiPresentation (if any)
    //
    
    //
    // prepare subscriber communicationChannels : TODO
    //
    
    List<Object> communicationChannels = new ArrayList<Object>();
    
    //
    // prepare custom generalDetails and kpiPresentation
    //
    
    addProfileFieldsForGUIPresentation(generalDetailsPresentation, kpiPresentation);
    
    //
    // prepare ProfilePresentation
    //
    
    baseProfilePresentation.put("generalDetails", JSONUtilities.encodeObject(generalDetailsPresentation));
    baseProfilePresentation.put("kpis", JSONUtilities.encodeObject(kpiPresentation));
    baseProfilePresentation.put("communicationChannels", JSONUtilities.encodeArray(communicationChannels));
    
    return baseProfilePresentation;
  }
  
  //
  //  getProfileMapForThirdPartyPresentation
  //
  
  public Map<String,Object> getProfileMapForThirdPartyPresentation(ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader)
  {
    HashMap<String, Object> baseProfilePresentation = new HashMap<String,Object>();
    HashMap<String, Object> generalDetailsPresentation = new HashMap<String,Object>();
    HashMap<String, Object> kpiPresentation = new HashMap<String,Object>();
    
    //
    // prepare basic generalDetails
    //
    
    generalDetailsPresentation.put("evolutionSubscriberStatus", (getEvolutionSubscriberStatus() != null) ? getEvolutionSubscriberStatus().getExternalRepresentation() : null);
    generalDetailsPresentation.put("evolutionSubscriberStatusChangeDate", getEvolutionSubscriberStatusChangeDate());
    generalDetailsPresentation.put("previousEvolutionSubscriberStatus", (getPreviousEvolutionSubscriberStatus() != null) ? getPreviousEvolutionSubscriberStatus().getExternalRepresentation() : null);
    List<String> subscriberGroups = new ArrayList<String>(getSubscriberGroups(subscriberGroupEpochReader));
    generalDetailsPresentation.put("subscriberGroups", JSONUtilities.encodeArray(subscriberGroups));
    generalDetailsPresentation.put("language", getLanguage());
    
    //
    // prepare basic kpiPresentation (if any)
    //
    
    //
    // prepare subscriber communicationChannels : TODO
    //
    
    List<Object> communicationChannels = new ArrayList<Object>();
    
    //
    // prepare custom generalDetails and kpiPresentation
    //
    
    addProfileFieldsForThirdPartyPresentation(generalDetailsPresentation, kpiPresentation);
    
    //
    // prepare ProfilePresentation
    //
    
    baseProfilePresentation.put("generalDetails", JSONUtilities.encodeObject(generalDetailsPresentation));
    baseProfilePresentation.put("kpis", JSONUtilities.encodeObject(kpiPresentation));
    baseProfilePresentation.put("communicationChannels", JSONUtilities.encodeArray(communicationChannels));
    
    return baseProfilePresentation;
  }
  
  //
  //  getInSubscriberGroup
  //
  
  public boolean getInSubscriberGroup(String requestedSegmentID, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader)
  {
    return getSubscriberGroups(subscriberGroupEpochReader).contains(requestedSegmentID);
  }

  /****************************************
  *
  *  getHistory utilities
  *
  ****************************************/

  //
  //  getYesterday
  //
  
  protected long getYesterday(MetricHistory metricHistory, Date evaluationDate)
  {
    Date day = RLMDateUtils.truncate(evaluationDate, Calendar.DATE, Calendar.SUNDAY, Deployment.getBaseTimeZone());
    Date startDay = RLMDateUtils.addDays(day, -1, Deployment.getBaseTimeZone());
    Date endDay = startDay;
    return metricHistory.getValue(startDay, endDay);
  }
  
  //
  //  getPrevious7Days
  //
  
  protected long getPrevious7Days(MetricHistory metricHistory, Date evaluationDate)
  {
    Date day = RLMDateUtils.truncate(evaluationDate, Calendar.DATE, Calendar.SUNDAY, Deployment.getBaseTimeZone());
    Date startDay = RLMDateUtils.addDays(day, -7, Deployment.getBaseTimeZone());
    Date endDay = RLMDateUtils.addDays(day, -1, Deployment.getBaseTimeZone());
    return metricHistory.getValue(startDay, endDay);
  }
  
  //
  //  getPrevious14Days
  //
  
  protected long getPrevious14Days(MetricHistory metricHistory, Date evaluationDate)
  {
    Date day = RLMDateUtils.truncate(evaluationDate, Calendar.DATE, Calendar.SUNDAY, Deployment.getBaseTimeZone());
    Date startDay = RLMDateUtils.addDays(day, -14, Deployment.getBaseTimeZone());
    Date endDay = RLMDateUtils.addDays(day, -1, Deployment.getBaseTimeZone());
    return metricHistory.getValue(startDay, endDay);
  }
  
  //
  //  getPreviousMonth
  //
  
  protected long getPreviousMonth(MetricHistory metricHistory, Date evaluationDate)
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

  protected long getPrevious90Days(MetricHistory metricHistory, Date evaluationDate)
  {
    Date day = RLMDateUtils.truncate(evaluationDate, Calendar.DATE, Calendar.SUNDAY, Deployment.getBaseTimeZone());
    Date startDay = RLMDateUtils.addDays(day, -90, Deployment.getBaseTimeZone());
    Date endDay = RLMDateUtils.addDays(day, -1, Deployment.getBaseTimeZone());
    return metricHistory.getValue(startDay, endDay);
  }
  
  /****************************************
  *
  *  setters
  *
  ****************************************/

  public void setSubscriberTraceEnabled(boolean subscriberTraceEnabled) { this.subscriberTraceEnabled = subscriberTraceEnabled; }
  public void setEvolutionSubscriberStatus(EvolutionSubscriberStatus evolutionSubscriberStatus) { this.evolutionSubscriberStatus = evolutionSubscriberStatus; }
  public void setEvolutionSubscriberStatusChangeDate(Date evolutionSubscriberStatusChangeDate) { this.evolutionSubscriberStatusChangeDate = evolutionSubscriberStatusChangeDate; }
  public void setPreviousEvolutionSubscriberStatus(EvolutionSubscriberStatus previousEvolutionSubscriberStatus) { this.previousEvolutionSubscriberStatus = previousEvolutionSubscriberStatus; }
  public void setLanguage(String language) { this.language = language; }
  public void setSubscriberHistory(SubscriberHistory subscriberHistory) { this.subscriberHistory = subscriberHistory; }

  //
  //  setSubscriberGroup
  //
  
  public void setSubscriberGroup(String dimensionID, String segmentID, int epoch, boolean addSubscriber)
  {
    Pair<String,String> groupID = new Pair<String,String>(dimensionID, segmentID);
    if (subscriberGroups.get(groupID) == null || subscriberGroups.get(groupID).intValue() <= epoch)
      {
        //
        //  unconditionally remove groupID (if present)
        //

        subscriberGroups.remove(groupID);

        //
        //  add (if necessary)
        //

        if (addSubscriber)
          {
            subscriberGroups.put(groupID, epoch);
          }
      }
  }

  /*****************************************
  *
  *  constructor (simple)
  *
  *****************************************/

  protected SubscriberProfile(String subscriberID)
  {
    this.subscriberID = subscriberID;
    this.subscriberTraceEnabled = false;
    this.evolutionSubscriberStatus = null;
    this.evolutionSubscriberStatusChangeDate = null;
    this.previousEvolutionSubscriberStatus = null;
    this.subscriberGroups = new HashMap<Pair<String,String>, Integer>();
    this.language = null;
    this.subscriberHistory = null;
  }
  
  /*****************************************
  *
  *  constructor (unpack)
  *
  *****************************************/

  protected SubscriberProfile(SchemaAndValue schemaAndValue)
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
    EvolutionSubscriberStatus evolutionSubscriberStatus = (valueStruct.getString("evolutionSubscriberStatus") != null) ? EvolutionSubscriberStatus.fromExternalRepresentation(valueStruct.getString("evolutionSubscriberStatus")) : null;
    Date evolutionSubscriberStatusChangeDate = (Date) valueStruct.get("evolutionSubscriberStatusChangeDate");
    EvolutionSubscriberStatus previousEvolutionSubscriberStatus = (valueStruct.getString("previousEvolutionSubscriberStatus") != null) ? EvolutionSubscriberStatus.fromExternalRepresentation(valueStruct.getString("previousEvolutionSubscriberStatus")) : null;
    Map<Pair<String,String>, Integer> subscriberGroups = unpackSubscriberGroups(valueStruct.get("subscriberGroups"));
    String language = valueStruct.getString("language");
    SubscriberHistory subscriberHistory  = valueStruct.get("subscriberHistory") != null ? SubscriberHistory.unpack(new SchemaAndValue(schema.field("subscriberHistory").schema(), valueStruct.get("subscriberHistory"))) : null;

    //
    //  return
    //

    this.subscriberID = subscriberID;
    this.subscriberTraceEnabled = subscriberTraceEnabled;
    this.evolutionSubscriberStatus = evolutionSubscriberStatus;
    this.evolutionSubscriberStatusChangeDate = evolutionSubscriberStatusChangeDate;
    this.previousEvolutionSubscriberStatus = previousEvolutionSubscriberStatus;
    this.subscriberGroups = subscriberGroups;
    this.language = language;
    this.subscriberHistory = subscriberHistory;
  }

  /*****************************************
  *
  *  unpackSubscriberGroups
  *
  *****************************************/

  private static Map<Pair<String,String>, Integer> unpackSubscriberGroups(Object value)
  {
    Map<Pair<String,String>, Integer> result = new HashMap<Pair<String,String>, Integer>();
    if (value != null)
      {
        Map<Object, Integer> valueMap = (Map<Object, Integer>) value;
        for (Object packedGroupID : valueMap.keySet())
          {
            Pair<String,String> groupID = unpackGroupID(packedGroupID);
            Integer epoch = valueMap.get(packedGroupID);
            result.put(groupID, epoch);
          }
      }
    return result;
  }

  /*****************************************
  *
  *  unpackPair
  *
  *****************************************/

  private static Pair<String, String> unpackGroupID(Object value)
  {
    Struct valueStruct = (Struct) value;
    String dimensionID = valueStruct.getString("dimensionID");
    String segmentID = valueStruct.getString("segmentID");
    return new Pair<String,String>(dimensionID, segmentID);
  }

  /*****************************************
  *
  *  constructor (copy)
  *
  *****************************************/

  protected SubscriberProfile(SubscriberProfile subscriberProfile)
  {
    this.subscriberID = subscriberProfile.getSubscriberID();
    this.subscriberTraceEnabled = subscriberProfile.getSubscriberTraceEnabled();
    this.evolutionSubscriberStatus = subscriberProfile.getEvolutionSubscriberStatus();
    this.evolutionSubscriberStatusChangeDate = subscriberProfile.getEvolutionSubscriberStatusChangeDate();
    this.previousEvolutionSubscriberStatus = subscriberProfile.getPreviousEvolutionSubscriberStatus();
    this.subscriberGroups = new HashMap<Pair<String,String>, Integer>(subscriberProfile.getSubscriberGroups());
    this.language = subscriberProfile.getLanguage();
    this.subscriberHistory = subscriberProfile.getSubscriberHistory() != null ? new SubscriberHistory(subscriberProfile.getSubscriberHistory()) : null;
  }

  /*****************************************
  *
  *  packCommon
  *
  *****************************************/
  
  protected static void packCommon(Struct struct, SubscriberProfile subscriberProfile)
  {
    struct.put("subscriberID", subscriberProfile.getSubscriberID());
    struct.put("subscriberTraceEnabled", subscriberProfile.getSubscriberTraceEnabled());
    struct.put("evolutionSubscriberStatus", (subscriberProfile.getEvolutionSubscriberStatus() != null) ? subscriberProfile.getEvolutionSubscriberStatus().getExternalRepresentation() : null);
    struct.put("evolutionSubscriberStatusChangeDate", subscriberProfile.getEvolutionSubscriberStatusChangeDate());
    struct.put("previousEvolutionSubscriberStatus", (subscriberProfile.getPreviousEvolutionSubscriberStatus() != null) ? subscriberProfile.getPreviousEvolutionSubscriberStatus().getExternalRepresentation() : null);
    struct.put("subscriberGroups", packSubscriberGroups(subscriberProfile.getSubscriberGroups()));
    struct.put("language", subscriberProfile.getLanguage());
    struct.put("subscriberHistory", (subscriberProfile.getSubscriberHistory() != null) ? SubscriberHistory.serde().packOptional(subscriberProfile.getSubscriberHistory()) : null);
  }
  
  /****************************************
  *
  *  packSubscriberGroups
  *
  ****************************************/

  private static Object packSubscriberGroups(Map<Pair<String,String>, Integer> subscriberGroups)
  {
    Map<Object, Object> result = new HashMap<Object, Object>();
    for (Pair<String,String> groupID : subscriberGroups.keySet())
      {
        String dimensionID = groupID.getFirstElement();
        String segmentID = groupID.getSecondElement();
        Integer epoch = subscriberGroups.get(groupID);
        Struct packedGroupID = new Struct(groupIDSchema);
        packedGroupID.put("dimensionID", dimensionID);
        packedGroupID.put("segmentID", segmentID);
        result.put(packedGroupID, epoch);
      }
    return result;
  }

  /*****************************************
  *
  *  copy
  *
  *****************************************/

  public SubscriberProfile copy()
  {
    try
      {
        return (SubscriberProfile) subscriberProfileCopyConstructor.newInstance(this);
      }
    catch (InvocationTargetException e)
      {
        throw new RuntimeException(e.getCause());
      }
    catch (InstantiationException|IllegalAccessException e)
      {
        throw new RuntimeException(e);
      }
  }

  /*****************************************
  *
  *  toString
  *
  *****************************************/

  //
  //  toString
  //
  
  public String toString(ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader)
  {
    Date now = SystemTime.getCurrentTime();
    StringBuilder b = new StringBuilder();
    b.append("SubscriberProfile:{");
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
    b.append("," + evolutionSubscriberStatus);
    b.append("," + evolutionSubscriberStatusChangeDate);
    b.append("," + previousEvolutionSubscriberStatus);
    b.append("," + language);
    b.append("," + getUniversalControlGroup(subscriberGroupEpochReader));
    b.append("," + (subscriberHistory != null ? subscriberHistory.getDeliveryRequests().size() : null));
    return b.toString();
  }

  /*****************************************
  *
  *  compressSubscriberProfile
  *
  *****************************************/

  public static byte [] compressSubscriberProfile(byte [] data, CompressionType compressionType)
  {
    //
    //  sanity
    //

    if (SubscriberProfileCompressionEpoch != 0) throw new ServerRuntimeException("unsupported compression epoch");

    //
    //  compress (if indicated)
    //
    
    byte[] payload;
    switch (compressionType)
      {
        case None:
          payload = data;
          break;
        case GZip:
          payload = compress_gzip(data);
          break;
        default:
          throw new RuntimeException("unsupported compression type");
      }

    //
    //  prepare result
    //

    byte[] result = new byte[payload.length+1];

    //
    //  compression epoch
    //

    result[0] = SubscriberProfileCompressionEpoch;

    //
    //  payload
    //

    System.arraycopy(payload, 0, result, 1, payload.length);

    //
    //  return
    //

    return result;
  }

  /*****************************************
  *
  *  uncompressSubscriberProfile
  *
  *****************************************/

  public static byte [] uncompressSubscriberProfile(byte [] compressedData, CompressionType compressionType)
  {
    /****************************************
    *
    *  check epoch
    *
    ****************************************/
    
    int epoch = compressedData[0];
    if (epoch != 0) throw new ServerRuntimeException("unsupported compression epoch");

    /****************************************
    *
    *  uncompress according to provided algorithm
    *
    ****************************************/

    //
    // extract payload
    // 

    byte [] rawPayload = new byte[compressedData.length-1];
    System.arraycopy(compressedData, 1, rawPayload, 0, compressedData.length-1);

    //
    //  uncompress
    //
    
    byte [] payload;
    switch (compressionType)
      {
        case None:
          payload = rawPayload;
          break;
        case GZip:
          payload = uncompress_gzip(rawPayload);
          break;
        default:
          throw new RuntimeException("unsupported compression type");
      }

    //
    //  return
    //

    return payload;
  }

  /*****************************************
  *
  *  compress_gzip
  *
  *****************************************/

  private static byte[] compress_gzip(byte [] data)
  {
    int len = data.length;
    byte [] compressedData;
    try
      {
        //
        //  length (to make the uncompress easier)
        //

        ByteArrayOutputStream bos = new ByteArrayOutputStream(4 + len);
        byte[] lengthBytes = integerToBytes(len);
        bos.write(lengthBytes);

        //
        //  payload
        //

        GZIPOutputStream gzip = new GZIPOutputStream(bos);
        gzip.write(data);

        //
        // result
        //

        gzip.close();
        compressedData = bos.toByteArray();
        bos.close();
      }
    catch (IOException ioe)
      {
        throw new ServerRuntimeException("compress", ioe);
      }

    return compressedData;
  }

  /*****************************************
  *
  *  uncompress_gzip
  *
  *****************************************/

  private static byte[] uncompress_gzip(byte [] compressedData)
  {
    //
    // sanity
    //
    
    if (compressedData == null) return null;

    //
    //  uncompress
    //
    
    byte [] uncompressedData;
    try
      {
        ByteArrayInputStream bis = new ByteArrayInputStream(compressedData);

        //
        //  extract length
        //

        byte [] lengthBytes = new byte[4];
        bis.read(lengthBytes, 0, 4);
        int dataLength = bytesToInteger(lengthBytes);

        //
        //  extract payload
        //

        uncompressedData = new byte[dataLength];
        GZIPInputStream gis = new GZIPInputStream(bis);
        int bytesRead = 0;
        int pos = 0;
        while (pos < dataLength)
          {
            bytesRead = gis.read(uncompressedData, pos, dataLength-pos);
            pos = pos + bytesRead;                
          }

        //
        //  close
        //
        
        gis.close();
        bis.close();
      }
    catch (IOException ioe)
      {
        throw new ServerRuntimeException("uncompress", ioe);
      }

    return uncompressedData;
  }

  /*****************************************
  *
  *  integerToBytes
  *
  *****************************************/

  private static byte[] integerToBytes(int value)
  {
    byte [] result = new byte[4];
    result[0] = (byte) (value >> 24);
    result[1] = (byte) (value >> 16);
    result[2] = (byte) (value >> 8);
    result[3] = (byte) (value);
    return result;
  }

  /*****************************************
  *
  *  bytesToInteger
  *
  *****************************************/

  private static int bytesToInteger(byte[] data)
  {
    return ((0x000000FF & data[0]) << 24) + ((0x000000FF & data[0]) << 16) + ((0x000000FF & data[2]) << 8) + ((0x000000FF) & data[3]);
  }
}
