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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
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
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import com.evolving.nglm.core.Pair;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.ReferenceDataReader;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.SubscriberStreamOutput;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.LoyaltyProgramHistory.TierHistory;

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
    groupIDSchemaBuilder.version(SchemaUtilities.packSchemaVersion(2));
    groupIDSchemaBuilder.field("subscriberGroupIDs", SchemaBuilder.array(Schema.STRING_SCHEMA).defaultValue(new ArrayList<String>()).schema());
    groupIDSchema = groupIDSchemaBuilder.build();

    //
    //  commonSchema
    //

    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(3));
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("subscriberTraceEnabled", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("evolutionSubscriberStatus", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("evolutionSubscriberStatusChangeDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("previousEvolutionSubscriberStatus", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("segments", SchemaBuilder.map(groupIDSchema, Schema.INT32_SCHEMA).name("subscriber_profile_segments").schema());
    schemaBuilder.field("loyaltyPrograms", SchemaBuilder.map(Schema.STRING_SCHEMA, LoyaltyProgramPointsState.schema()).name("subscriber_profile_loyaltyPrograms").schema());
    schemaBuilder.field("targets", SchemaBuilder.map(groupIDSchema, Schema.INT32_SCHEMA).name("subscriber_profile_targets").schema());
    schemaBuilder.field("exclusionInclusionTargets", SchemaBuilder.map(groupIDSchema, Schema.INT32_SCHEMA).name("subscriber_profile_exclusion_inclusion_targets").schema());
    schemaBuilder.field("relations", SchemaBuilder.map(Schema.STRING_SCHEMA, SubscriberRelatives.serde().schema()).name("subscriber_profile_relations").schema());
    schemaBuilder.field("universalControlGroup", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("tokens", SchemaBuilder.array(Token.commonSerde().schema()).defaultValue(Collections.<Token>emptyList()).schema());
    schemaBuilder.field("pointBalances", SchemaBuilder.map(Schema.STRING_SCHEMA, PointBalance.schema()).name("subscriber_profile_balances").schema());
    schemaBuilder.field("language", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("extendedSubscriberProfile", ExtendedSubscriberProfile.getExtendedSubscriberProfileSerde().optionalSchema());
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
  private Map<Pair<String,String>,Integer> segments; // Map<Pair<dimensionID,segmentID> epoch>>
  private Map<String,LoyaltyProgramPointsState> loyaltyPrograms; //Map<loyaltyProgID,<loyaltyProgramState>>
  private Map<String,Integer> targets;
  private Map<String, SubscriberRelatives> relations; // Map<RelationshipID, SubscrbierRelatives(Parent & Children)>
  private boolean universalControlGroup;
  private List<Token> tokens;
  private Map<String,PointBalance> pointBalances;
  private String languageID;
  private ExtendedSubscriberProfile extendedSubscriberProfile;
  private SubscriberHistory subscriberHistory;
  private Map<String,Integer> exclusionInclusionTargets; 

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
  public Map<Pair<String, String>, Integer> getSegments() { return segments; }
  public Map<String, LoyaltyProgramPointsState> getLoyaltyPrograms() { return loyaltyPrograms; }
  public Map<String, Integer> getTargets() { return targets; }
  public Map<String, SubscriberRelatives> getRelations() { return relations; }
  public boolean getUniversalControlGroup() { return universalControlGroup; }
  public List<Token> getTokens(){ return tokens; }
  public Map<String,PointBalance> getPointBalances() { return pointBalances; }
  public String getLanguageID() { return languageID; }
  public ExtendedSubscriberProfile getExtendedSubscriberProfile() { return extendedSubscriberProfile; }
  public SubscriberHistory getSubscriberHistory() { return subscriberHistory; }
  public Map<String, Integer> getExclusionInclusionTargets() { return exclusionInclusionTargets; }

  //
  //  temporary (until we can update nglm-kazakhstan)
  //

  public boolean getUniversalControlGroup(ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader) { return getUniversalControlGroup(); }

  /*****************************************
  *
  *  getLanguage
  *
  *****************************************/

  public String getLanguage()
  {
    return (languageID != null && Deployment.getSupportedLanguages().get(getLanguageID()) != null) ? Deployment.getSupportedLanguages().get(getLanguageID()).getName() : Deployment.getBaseLanguage();
  }

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
  *  abstract -- identifiers (with default "null" implementations)a
  *
  ****************************************/

  public String getMSISDN() { return null; }
  public String getEmail() { return null; }
  public String getAppID() { return null; }

  /****************************************
  *
  *  accessors - segments
  *
  ****************************************/

  //
  //  getSegmentsMap (map of <dimensionID,segmentID>)
  //

  public Map<String, String> getSegmentsMap(ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader)
  {
    Map<String, String> result = new HashMap<String, String>();
    for (Pair<String,String> groupID : segments.keySet())
      {
        String dimensionID = groupID.getFirstElement();
        int epoch = segments.get(groupID);
        if (epoch == (subscriberGroupEpochReader.get(dimensionID) != null ? subscriberGroupEpochReader.get(dimensionID).getEpoch() : 0))
          {
            result.put(dimensionID, groupID.getSecondElement());
          }
      }
    return result;
  }

  //
  //  getSegments (set of segmentID)
  //

  public Set<String> getSegments(ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader)
  {
    return new HashSet<String>(getSegmentsMap(subscriberGroupEpochReader).values());
  }

  //
  //  getSegmentNames (set of segment name)
  //

  public Set<String> getSegmentNames(SegmentationDimensionService segmentationDimensionService, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader)
  {
    Date evaluationDate = SystemTime.getCurrentTime();
    Set<String> result = new HashSet<String>();
    for (Pair<String,String> groupID : segments.keySet())
      {
        String dimensionID = groupID.getFirstElement();
        String segmentID = groupID.getSecondElement();
        SegmentationDimension segmentationDimension = segmentationDimensionService.getActiveSegmentationDimension(dimensionID, evaluationDate);
        if (segmentationDimension != null)
          {
            int epoch = segments.get(groupID);
            if (epoch == (subscriberGroupEpochReader.get(dimensionID) != null ? subscriberGroupEpochReader.get(dimensionID).getEpoch() : 0))
              {
                Segment segment = segmentationDimensionService.getSegment(segmentID);
                result.add(segment.getName());
              }
          }
      }
    return result;
  }

  /****************************************
  *
  *  accessors - loyalty programs
  *
  ****************************************/

  //TODO SCH : think of what we need to retrieve ... ... ... ... ... ... ... ... ... 
  
  /****************************************
  *
  *  accessors - targets
  *
  ****************************************/

  //
  //  getTargets (set of targetID)
  //

  public Set<String> getTargets(ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader)
  {
    Set<String> result = new HashSet<String>();
    for (String targetID : targets.keySet())
      {
        int epoch = targets.get(targetID);
        if (epoch == (subscriberGroupEpochReader.get(targetID) != null ? subscriberGroupEpochReader.get(targetID).getEpoch() : 0))
          {
            result.add(targetID);
          }
      }
    return result;
  }

  //
  //  getTargetNames (set of target name)
  //

  public Set<String> getTargetNames(TargetService targetService, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader)
  {
    Date evaluationDate = SystemTime.getCurrentTime();
    Set<String> result = new HashSet<String>();
    for (String targetID : targets.keySet())
      {
        Target target = targetService.getActiveTarget(targetID, evaluationDate);
        if (target != null)
          {
            int epoch = targets.get(targetID);
            if (epoch == (subscriberGroupEpochReader.get(targetID) != null ? subscriberGroupEpochReader.get(targetID).getEpoch() : 0))
              {
                result.add(target.getTargetName());
              }
          }
      }
    return result;
  }
  
  //
  //  getExclusionInclusionTargetNames (set of target name)
  //

  public Set<String> getExclusionInclusionTargetNames(ExclusionInclusionTargetService exclusionInclusionTargetService, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader)
  {
    Date evaluationDate = SystemTime.getCurrentTime();
    Set<String> result = new HashSet<String>();
    for (String targetID : exclusionInclusionTargets.keySet())
      {
        ExclusionInclusionTarget target = exclusionInclusionTargetService.getActiveExclusionInclusionTarget(targetID, evaluationDate);
        if (target != null)
          {
            int epoch = exclusionInclusionTargets.get(targetID);
            if (epoch == (subscriberGroupEpochReader.get(targetID) != null ? subscriberGroupEpochReader.get(targetID).getEpoch() : 0))
              {
                result.add(target.getExclusionInclusionTargetName());
              }
          }
      }
    return result;
  }
  
  //
  //  getExclusionInclusionTargets (set of ExclusionInclusionTargetID)
  //

  public Set<String> getExclusionInclusionTargets(ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader)
  {
    Set<String> result = new HashSet<String>();
    for (String exclusionInclusionTargetID : exclusionInclusionTargets.keySet())
      {
        int epoch = exclusionInclusionTargets.get(exclusionInclusionTargetID);
        if (epoch == (subscriberGroupEpochReader.get(exclusionInclusionTargetID) != null ? subscriberGroupEpochReader.get(exclusionInclusionTargetID).getEpoch() : 0))
          {
            result.add(exclusionInclusionTargetID);
          }
      }
    return result;
  }

  /*****************************************
  *
  *  accessors -- segmentContactPolicy
  *
  *****************************************/

  public String getSegmentContactPolicyID(SegmentContactPolicyService segmentContactPolicyService, SegmentationDimensionService segmentationDimensionService, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader)
  {
    SegmentContactPolicy segmentContactPolicy = segmentContactPolicyService.getSingletonSegmentContactPolicy();
    SegmentationDimension segmentationDimension = (segmentContactPolicy != null) ? segmentationDimensionService.getActiveSegmentationDimension(segmentContactPolicy.getDimensionID(), SystemTime.getCurrentTime()) : null;
    String segmentID = (segmentationDimension != null) ? getSegmentsMap(subscriberGroupEpochReader).get(segmentationDimension.getSegmentationDimensionID()) : null;
    String segmentContactPolicyID = (segmentContactPolicy != null && segmentID != null) ? segmentContactPolicy.getSegments().get(segmentID) : null;
    return segmentContactPolicyID;
  }

  /****************************************
  *
  *  presentation utilities
  *
  ****************************************/

  //
  //  getProfileMapForGUIPresentation
  //

  public Map<String, Object> getProfileMapForGUIPresentation(LoyaltyProgramService loyaltyProgramService, SegmentationDimensionService segmentationDimensionService, TargetService targetService, PointService pointService, ExclusionInclusionTargetService exclusionInclusionTargetService, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader)
  {
    //
    //  now
    //

    Date now = SystemTime.getCurrentTime();

    //
    //  prepare points
    //

    ArrayList<JSONObject> pointsPresentation = new ArrayList<JSONObject>();
    for (String pointID : pointBalances.keySet())
      {
        Point point = pointService.getActivePoint(pointID, now);
        if (point != null)
          {
            HashMap<String, Object> pointPresentation = new HashMap<String,Object>();
            PointBalance pointBalance = pointBalances.get(pointID);
            pointPresentation.put("point", point.getDisplay());
            pointPresentation.put("balance", pointBalance.getBalance(now));
            pointPresentation.put("firstExpiration", pointBalance.getFirstExpirationDate(now));
            pointsPresentation.add(JSONUtilities.encodeObject(pointPresentation));
          }
      }
    
    //
    // prepare hierarchy
    //
    
    ArrayList<JSONObject> hierarchyRelations = new ArrayList<JSONObject>();
    for (String relationshipID : this.relations.keySet())
      {
        SubscriberRelatives relatives = this.relations.get(relationshipID);
        if (relatives != null)
          {
            hierarchyRelations.add(relatives.getJSONRepresentation(relationshipID));
          }
      }

    //
    //  prepare loyalty programs
    //

    ArrayList<JSONObject> loyaltyProgramsPresentation = new ArrayList<JSONObject>();
    for (String loyaltyProgramID : loyaltyPrograms.keySet())
      {
        LoyaltyProgram loyaltyProgram = loyaltyProgramService.getActiveLoyaltyProgram(loyaltyProgramID, now);
        if (loyaltyProgram != null)
          {
            
            HashMap<String, Object> loyaltyProgramPresentation = new HashMap<String,Object>();
            
            //
            //  current tier
            //
            
            LoyaltyProgramPointsState loyaltyProgramState = loyaltyPrograms.get(loyaltyProgramID);
            loyaltyProgramPresentation.put("loyaltyProgramName", loyaltyProgramState.getLoyaltyProgramName());
            loyaltyProgramPresentation.put("loyaltyProgramEnrollmentDate", loyaltyProgramState.getLoyaltyProgramEnrollmentDate());
            loyaltyProgramPresentation.put("loyaltyProgramExitDate", loyaltyProgramState.getLoyaltyProgramExitDate());
            if(loyaltyProgramState.getTierName() != null){ loyaltyProgramPresentation.put("tierName", loyaltyProgramState.getTierName()); }
            if(loyaltyProgramState.getTierEnrollmentDate() != null){ loyaltyProgramPresentation.put("tierEnrollmentDate", loyaltyProgramState.getTierEnrollmentDate()); }
            
            //
            //  history
            //
            ArrayList<JSONObject> loyaltyProgramHistoryJSON = new ArrayList<JSONObject>();
            LoyaltyProgramHistory history = loyaltyProgramState.getLoyaltyProgramHistory();
            if(history != null && history.getTierHistory() != null && !history.getTierHistory().isEmpty()){
              for(TierHistory tier : history.getTierHistory()){
                HashMap<String, Object> tierHistoryJSON = new HashMap<String,Object>();
                tierHistoryJSON.put("fromTier", tier.getFromTier());
                tierHistoryJSON.put("toTier", tier.getToTier());
                tierHistoryJSON.put("transitionDate", tier.getTransitionDate());
                loyaltyProgramHistoryJSON.add(JSONUtilities.encodeObject(tierHistoryJSON));
              }
            }
            loyaltyProgramPresentation.put("loyaltyProgramHistory", loyaltyProgramHistoryJSON);
            
            //
            //  
            //
            
            loyaltyProgramsPresentation.add(JSONUtilities.encodeObject(loyaltyProgramPresentation));
            
          }
      }

    //
    // prepare basic generalDetails
    //

    HashMap<String, Object> generalDetailsPresentation = new HashMap<String,Object>();
    generalDetailsPresentation.put("evolutionSubscriberStatus", (getEvolutionSubscriberStatus() != null) ? getEvolutionSubscriberStatus().getExternalRepresentation() : null);
    generalDetailsPresentation.put("evolutionSubscriberStatusChangeDate", getDateString(getEvolutionSubscriberStatusChangeDate()));
    generalDetailsPresentation.put("previousEvolutionSubscriberStatus", (getPreviousEvolutionSubscriberStatus() != null) ? getPreviousEvolutionSubscriberStatus().getExternalRepresentation() : null);
    generalDetailsPresentation.put("segments", JSONUtilities.encodeArray(new ArrayList<String>(getSegmentNames(segmentationDimensionService, subscriberGroupEpochReader))));
    generalDetailsPresentation.put("loyaltyPrograms", JSONUtilities.encodeArray(loyaltyProgramsPresentation));
    generalDetailsPresentation.put("targets", JSONUtilities.encodeArray(new ArrayList<String>(getTargetNames(targetService, subscriberGroupEpochReader))));
    generalDetailsPresentation.put("relations", JSONUtilities.encodeArray(hierarchyRelations));
    generalDetailsPresentation.put("points", JSONUtilities.encodeArray(pointsPresentation));
    generalDetailsPresentation.put("language", getLanguage());
    generalDetailsPresentation.put("subscriberID", getSubscriberID());
    generalDetailsPresentation.put("exclusionInclusionTargets", JSONUtilities.encodeArray(new ArrayList<String>(getExclusionInclusionTargetNames(exclusionInclusionTargetService, subscriberGroupEpochReader))));

    //
    // prepare basic kpiPresentation (if any)
    //

    HashMap<String, Object> kpiPresentation = new HashMap<String,Object>();

    //
    // prepare subscriber communicationChannels : TODO
    //

    List<Object> communicationChannels = new ArrayList<Object>();

    //
    // prepare custom generalDetails and kpiPresentation
    //

    addProfileFieldsForGUIPresentation(generalDetailsPresentation, kpiPresentation);
    if (extendedSubscriberProfile != null) extendedSubscriberProfile.addProfileFieldsForGUIPresentation(generalDetailsPresentation, kpiPresentation);

    //
    // prepare ProfilePresentation
    //

    HashMap<String, Object> baseProfilePresentation = new HashMap<String,Object>();
    baseProfilePresentation.put("generalDetails", JSONUtilities.encodeObject(generalDetailsPresentation));
    baseProfilePresentation.put("kpis", JSONUtilities.encodeObject(kpiPresentation));
    baseProfilePresentation.put("communicationChannels", JSONUtilities.encodeArray(communicationChannels));

    return baseProfilePresentation;
  }

  //
  //  getProfileMapForThirdPartyPresentation
  //

  public Map<String,Object> getProfileMapForThirdPartyPresentation(SegmentationDimensionService segmentationDimensionService, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader)
  {
    HashMap<String, Object> baseProfilePresentation = new HashMap<String,Object>();
    HashMap<String, Object> generalDetailsPresentation = new HashMap<String,Object>();
    HashMap<String, Object> kpiPresentation = new HashMap<String,Object>();

    //
    // prepare basic generalDetails
    //

    generalDetailsPresentation.put("evolutionSubscriberStatus", (getEvolutionSubscriberStatus() != null) ? getEvolutionSubscriberStatus().getExternalRepresentation() : null);
    generalDetailsPresentation.put("evolutionSubscriberStatusChangeDate", getDateString(getEvolutionSubscriberStatusChangeDate()));
    generalDetailsPresentation.put("previousEvolutionSubscriberStatus", (getPreviousEvolutionSubscriberStatus() != null) ? getPreviousEvolutionSubscriberStatus().getExternalRepresentation() : null);
    generalDetailsPresentation.put("segments", JSONUtilities.encodeArray(new ArrayList<String>(getSegmentNames(segmentationDimensionService, subscriberGroupEpochReader))));
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
    if (extendedSubscriberProfile != null) extendedSubscriberProfile.addProfileFieldsForThirdPartyPresentation(generalDetailsPresentation, kpiPresentation);

    //
    // prepare ProfilePresentation
    //

    baseProfilePresentation.put("generalDetails", JSONUtilities.encodeObject(generalDetailsPresentation));
    baseProfilePresentation.put("kpis", JSONUtilities.encodeObject(kpiPresentation));
    baseProfilePresentation.put("communicationChannels", JSONUtilities.encodeArray(communicationChannels));

    return baseProfilePresentation;
  }

  //
  //  getInSegment
  //

  public boolean getInSegment(String requestedSegmentID, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader)
  {
    return getSegments(subscriberGroupEpochReader).contains(requestedSegmentID);
  }

//  //
//  //  getInLoyaltyProgram
//  //
//
//  public boolean getInLoyaltyProgram(String requestedLoyaltyProgramID, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader)
//  {
//    return getLoyaltyPrograms(subscriberGroupEpochReader).contains(requestedLoyaltyProgramID);
//  }

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
  public void setEvolutionSubscriberStatus(EvolutionSubscriberStatus evolutionSubscriberStatus) { this.evolutionSubscriberStatus = evolutionSubscriberStatus; }
  public void setEvolutionSubscriberStatusChangeDate(Date evolutionSubscriberStatusChangeDate) { this.evolutionSubscriberStatusChangeDate = evolutionSubscriberStatusChangeDate; }
  public void setPreviousEvolutionSubscriberStatus(EvolutionSubscriberStatus previousEvolutionSubscriberStatus) { this.previousEvolutionSubscriberStatus = previousEvolutionSubscriberStatus; }
  public void setUniversalControlGroup(boolean universalControlGroup) { this.universalControlGroup = universalControlGroup; }
  public void setTokens(List<Token> tokens){ this.tokens = tokens; }
  public void setLanguageID(String languageID) { this.languageID = languageID; }
  public void setExtendedSubscriberProfile(ExtendedSubscriberProfile extendedSubscriberProfile) { this.extendedSubscriberProfile = extendedSubscriberProfile; }
  public void setSubscriberHistory(SubscriberHistory subscriberHistory) { this.subscriberHistory = subscriberHistory; }

  //
  //  setEvolutionSubscriberStatus
  //

  public void setEvolutionSubscriberStatus(EvolutionSubscriberStatus newEvolutionSubscriberStatus, Date date)
  {
    this.previousEvolutionSubscriberStatus = this.evolutionSubscriberStatus;
    this.evolutionSubscriberStatus = newEvolutionSubscriberStatus;
    this.evolutionSubscriberStatusChangeDate = date;
  }
  
  //
  //  setSegment
  //

  public void setSegment(String dimensionID, String segmentID, int epoch, boolean addSubscriber)
  {
    Pair<String,String> groupID = new Pair<String,String>(dimensionID, segmentID);
    if (segments.get(groupID) == null || segments.get(groupID).intValue() <= epoch)
      {
        //
        //  unconditionally remove groupID (if present)
        //

        segments.remove(groupID);

        //
        //  add (if necessary)
        //

        if (addSubscriber)
          {
            segments.put(groupID, epoch);
          }
      }
  }

  //
  //  setTarget
  //

  public void setTarget(String targetID, int epoch, boolean addSubscriber)
  {
    if (targets.get(targetID) == null || targets.get(targetID).intValue() <= epoch)
      {
        //
        //  unconditionally remove groupID (if present)
        //

        targets.remove(targetID);

        //
        //  add (if necessary)
        //

        if (addSubscriber)
          {
            targets.put(targetID, epoch);
          }
      }
  }
  
  //
  //  setExclusionInclusionTarget
  //

  public void setExclusionInclusionTarget(String exclusionInclusionID, int epoch, boolean addSubscriber)
  {
    if (exclusionInclusionTargets.get(exclusionInclusionID) == null || exclusionInclusionTargets.get(exclusionInclusionID).intValue() <= epoch)
      {
        //
        //  unconditionally remove groupID (if present)
        //

        exclusionInclusionTargets.remove(exclusionInclusionID);

        //
        //  add (if necessary)
        //

        if (addSubscriber)
          {
            exclusionInclusionTargets.put(exclusionInclusionID, epoch);
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
    this.segments = new HashMap<Pair<String,String>, Integer>();
    this.loyaltyPrograms = new HashMap<String,LoyaltyProgramPointsState>();
    this.targets = new HashMap<String, Integer>();
    this.relations = new HashMap<String, SubscriberRelatives>();
    this.universalControlGroup = false;
    this.tokens = new ArrayList<Token>();
    this.pointBalances = new HashMap<String,PointBalance>();
    this.languageID = null;
    this.extendedSubscriberProfile = null;
    this.subscriberHistory = null;
    this.exclusionInclusionTargets = new HashMap<String, Integer>();
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
    Map<Pair<String,String>, Integer> segments = (schemaVersion >= 2) ? unpackSegments(valueStruct.get("segments")) : unpackSegmentsV1(valueStruct.get("subscriberGroups"));
    Map<String, Integer> targets = (schemaVersion >= 2) ? unpackTargets(valueStruct.get("targets")) : new HashMap<String,Integer>();
    Map<String, SubscriberRelatives> relations = (schemaVersion >= 3) ? unpackRelations(schema.field("relations").schema(), valueStruct.get("relations")) : new HashMap<String,SubscriberRelatives>();
    boolean universalControlGroup = valueStruct.getBoolean("universalControlGroup");
    List<Token> tokens = (schemaVersion >= 2) ? unpackTokens(schema.field("tokens").schema(), valueStruct.get("tokens")) : Collections.<Token>emptyList();
    Map<String,PointBalance> pointBalances = (schemaVersion >= 2) ? unpackPointBalances(schema.field("pointBalances").schema(), (Map<String,Object>) valueStruct.get("pointBalances")): Collections.<String,PointBalance>emptyMap();
    String languageID = valueStruct.getString("language");
    ExtendedSubscriberProfile extendedSubscriberProfile = (schemaVersion >= 2) ? ExtendedSubscriberProfile.getExtendedSubscriberProfileSerde().unpackOptional(new SchemaAndValue(schema.field("extendedSubscriberProfile").schema(), valueStruct.get("extendedSubscriberProfile"))) : null;
    SubscriberHistory subscriberHistory  = valueStruct.get("subscriberHistory") != null ? SubscriberHistory.unpack(new SchemaAndValue(schema.field("subscriberHistory").schema(), valueStruct.get("subscriberHistory"))) : null;
    Map<String, Integer> exclusionInclusionTargets = (schemaVersion >= 2) ? unpackTargets(valueStruct.get("exclusionInclusionTargets")) : new HashMap<String,Integer>();
    Map<String,LoyaltyProgramPointsState> loyaltyPrograms = (schemaVersion >= 2) ? unpackLoyaltyPrograms(schema.field("loyaltyPrograms").schema(), (Map<String,Object>) valueStruct.get("loyaltyPrograms")): Collections.<String,LoyaltyProgramPointsState>emptyMap();

    //
    //  return
    //

    this.subscriberID = subscriberID;
    this.subscriberTraceEnabled = subscriberTraceEnabled;
    this.evolutionSubscriberStatus = evolutionSubscriberStatus;
    this.evolutionSubscriberStatusChangeDate = evolutionSubscriberStatusChangeDate;
    this.previousEvolutionSubscriberStatus = previousEvolutionSubscriberStatus;
    this.segments = segments;
    this.loyaltyPrograms = loyaltyPrograms;
    this.targets = targets;
    this.relations = relations;
    this.universalControlGroup = universalControlGroup;
    this.tokens = tokens;
    this.pointBalances = pointBalances;
    this.languageID = languageID;
    this.extendedSubscriberProfile = extendedSubscriberProfile;
    this.subscriberHistory = subscriberHistory;
    this.exclusionInclusionTargets = exclusionInclusionTargets;
  }

  /*****************************************
  *
  *  unpackSegments
  *
  *****************************************/

  private static Map<Pair<String,String>, Integer> unpackSegments(Object value)
  {
    Map<Pair<String,String>, Integer> result = new HashMap<Pair<String,String>, Integer>();
    if (value != null)
      {
        Map<Object, Integer> valueMap = (Map<Object, Integer>) value;
        for (Object packedGroupID : valueMap.keySet())
          {
            List<String> subscriberGroupIDs = (List<String>) ((Struct) packedGroupID).get("subscriberGroupIDs");
            Pair<String,String> groupID = new Pair<String,String>(subscriberGroupIDs.get(0), subscriberGroupIDs.get(1));
            Integer epoch = valueMap.get(packedGroupID);
            result.put(groupID, epoch);
          }
      }
    return result;
  }

  /*****************************************
  *
  *  unpackSegmentsV1
  *
  *****************************************/

  private static Map<Pair<String,String>, Integer> unpackSegmentsV1(Object value)
  {
    Map<Pair<String,String>, Integer> result = new HashMap<Pair<String,String>, Integer>();
    if (value != null)
      {
        Map<Object, Integer> valueMap = (Map<Object, Integer>) value;
        for (Object packedGroupID : valueMap.keySet())
          {
            Pair<String,String> groupID = new Pair<String,String>(((Struct) packedGroupID).getString("dimensionID"), ((Struct) packedGroupID).getString("segmentID"));
            Integer epoch = valueMap.get(packedGroupID);
            result.put(groupID, epoch);
          }
      }
    return result;
  }

  /*****************************************
  *
  *  unpackLoyaltyPrograms
  *
  *****************************************/

  private static Map<String,LoyaltyProgramPointsState> unpackLoyaltyPrograms(Schema schema, Map<String,Object> value)
  {
    //
    //  get schema for LoyaltyProgramState
    //

    Schema loyaltyProgramStateSchema = schema.valueSchema();

    //
    //  unpack
    //

    Map<String,LoyaltyProgramPointsState> result = new HashMap<String,LoyaltyProgramPointsState>();
    for (String key : value.keySet())
      {
        result.put(key, LoyaltyProgramPointsState.unpack(new SchemaAndValue(loyaltyProgramStateSchema, value.get(key))));
      }

    //
    //  return
    //

    return result;
  }

  /*****************************************
  *
  *  unpackTargets
  *
  *****************************************/

  private static Map<String, Integer> unpackTargets(Object value)
  {
    Map<String, Integer> result = new HashMap<String, Integer>();
    if (value != null)
      {
        Map<Object, Integer> valueMap = (Map<Object, Integer>) value;
        for (Object packedGroupID : valueMap.keySet())
          {
            List<String> subscriberGroupIDs = (List<String>) ((Struct) packedGroupID).get("subscriberGroupIDs");
            String targetID = subscriberGroupIDs.get(0);
            Integer epoch = valueMap.get(packedGroupID);
            result.put(targetID, epoch);
          }
      }
    return result;
  }
  


  /*****************************************
  *
  *  unpackRelations
  *
  *****************************************/

  private static Map<String, SubscriberRelatives> unpackRelations(Schema schema, Object value)
  {
    Schema mapSchema = schema.valueSchema();
    Map<String, SubscriberRelatives> result = new HashMap<String, SubscriberRelatives>();
    Map<String, Object> valueMap = (Map<String, Object>) value;
    for (String relationshipID : valueMap.keySet())
      {
        result.put(relationshipID, SubscriberRelatives.serde().unpack(
            new SchemaAndValue(mapSchema, valueMap.get(relationshipID))));
      }
    return result;
  }

  /*****************************************
  *
  *  unpackTokens
  *
  *****************************************/

  private static List<Token> unpackTokens(Schema schema, Object value)
  {
    //
    //  get schema for EvaluationCriterion
    //

    Schema tokenSchema = schema.valueSchema();

    //
    //  unpack
    //

    List<Token> result = new ArrayList<Token>();
    List<Object> valueArray = (List<Object>) value;
    for (Object token : valueArray)
    {
      result.add(Token.commonSerde().unpack(new SchemaAndValue(tokenSchema, token)));
    }

    //
    //  return
    //

    return result;
  }

  /*****************************************
  *
  *  unpackPointBalances
  *
  *****************************************/

  private static Map<String,PointBalance> unpackPointBalances(Schema schema, Map<String,Object> value)
  {
    //
    //  get schema for PointBalance
    //

    Schema pointBalanceSchema = schema.valueSchema();

    //
    //  unpack
    //

    Map<String,PointBalance> result = new HashMap<String,PointBalance>();
    for (String key : value.keySet())
      {
        result.put(key, PointBalance.unpack(new SchemaAndValue(pointBalanceSchema, value.get(key))));
      }

    //
    //  return
    //

    return result;
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
    this.segments = new HashMap<Pair<String,String>, Integer>(subscriberProfile.getSegments());
    this.loyaltyPrograms = new HashMap<String,LoyaltyProgramPointsState>(subscriberProfile.getLoyaltyPrograms());
    this.targets = new HashMap<String, Integer>(subscriberProfile.getTargets());
    this.relations = new HashMap<String, SubscriberRelatives>(subscriberProfile.getRelations());
    this.universalControlGroup = subscriberProfile.getUniversalControlGroup();
    this.tokens = new ArrayList<Token>(subscriberProfile.getTokens());
    this.pointBalances = new HashMap<String,PointBalance>(subscriberProfile.getPointBalances()); // WARNING:  NOT a deep copy, PointBalance must be copied before update
    this.languageID = subscriberProfile.getLanguageID();
    this.extendedSubscriberProfile = subscriberProfile.getExtendedSubscriberProfile() != null ? ExtendedSubscriberProfile.copy(subscriberProfile.getExtendedSubscriberProfile()) : null;
    this.subscriberHistory = subscriberProfile.getSubscriberHistory() != null ? new SubscriberHistory(subscriberProfile.getSubscriberHistory()) : null;
    this.exclusionInclusionTargets = new HashMap<String, Integer>(subscriberProfile.getExclusionInclusionTargets());
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
    struct.put("segments", packSegments(subscriberProfile.getSegments()));
    struct.put("loyaltyPrograms", packLoyaltyPrograms(subscriberProfile.getLoyaltyPrograms()));
    struct.put("targets", packTargets(subscriberProfile.getTargets()));
    struct.put("relations", packRelations(subscriberProfile.getRelations()));
    struct.put("universalControlGroup", subscriberProfile.getUniversalControlGroup());
    struct.put("tokens", packTokens(subscriberProfile.getTokens()));
    struct.put("pointBalances", packPointBalances(subscriberProfile.getPointBalances()));
    struct.put("language", subscriberProfile.getLanguageID());
    struct.put("extendedSubscriberProfile", (subscriberProfile.getExtendedSubscriberProfile() != null) ? ExtendedSubscriberProfile.getExtendedSubscriberProfileSerde().packOptional(subscriberProfile.getExtendedSubscriberProfile()) : null);
    struct.put("subscriberHistory", (subscriberProfile.getSubscriberHistory() != null) ? SubscriberHistory.serde().packOptional(subscriberProfile.getSubscriberHistory()) : null);
    struct.put("exclusionInclusionTargets", packTargets(subscriberProfile.getExclusionInclusionTargets()));
  }

  /****************************************
  *
  *  packSegments
  *
  ****************************************/

  private static Object packSegments(Map<Pair<String,String>, Integer> segments)
  {
    Map<Object, Object> result = new HashMap<Object, Object>();
    for (Pair<String,String> groupID : segments.keySet())
      {
        String dimensionID = groupID.getFirstElement();
        String segmentID = groupID.getSecondElement();
        Integer epoch = segments.get(groupID);
        Struct packedGroupID = new Struct(groupIDSchema);
        packedGroupID.put("subscriberGroupIDs", Arrays.asList(dimensionID, segmentID));
        result.put(packedGroupID, epoch);
      }
    return result;
  }

  /****************************************
  *
  *  packLoyaltyPrograms
  *
  ****************************************/

  private static Map<String,Object> packLoyaltyPrograms(Map<String,LoyaltyProgramPointsState> loyaltyPrograms)
  {
    Map<String,Object> result = new HashMap<String,Object>();
    for (String loyaltyProgramID : loyaltyPrograms.keySet())
      {
        result.put(loyaltyProgramID, LoyaltyProgramPointsState.pack(loyaltyPrograms.get(loyaltyProgramID)));
      }
    return result;
  }
  
  /****************************************
  *
  *  packTargets
  *
  ****************************************/

  private static Object packTargets(Map<String, Integer> targets)
  {
    Map<Object, Object> result = new HashMap<Object, Object>();
    for (String targetID : targets.keySet())
      {
        Integer epoch = targets.get(targetID);
        Struct packedGroupID = new Struct(groupIDSchema);
        packedGroupID.put("subscriberGroupIDs", Arrays.asList(targetID));
        result.put(packedGroupID, epoch);
      }
    return result;
  }

  /****************************************
  *
  *  packRelations
  *
  ****************************************/

  private static Object packRelations(Map<String, SubscriberRelatives> relations)
  {
    Map<String,Object> result = new HashMap<String,Object>();
    for (String relationshipID : relations.keySet())
      {
        result.put(relationshipID, SubscriberRelatives.pack(relations.get(relationshipID)));
      }
    return result;
  }
  
  /****************************************
  *
  *  packTokens
  *
  ****************************************/

  private static Object packTokens(List<Token> tokens)
  {
    List<Object> result = new ArrayList<Object>();
    for (Token token : tokens)
      {
        result.add(Token.commonSerde().pack(token));
      }
    return result;
  }

  /****************************************
  *
  *  packPointBalances
  *
  ****************************************/

  private static Map<String,Object> packPointBalances(Map<String,PointBalance> pointBalances)
  {
    Map<String,Object> result = new HashMap<String,Object>();
    for (String pointID : pointBalances.keySet())
      {
        result.put(pointID, PointBalance.pack(pointBalances.get(pointID)));
      }
    return result;
  }
  
  /****************************************
  *
  *  getInInclusionList
  *
  ****************************************/
  
  public boolean getInInclusionList(ExclusionInclusionTargetService exclusionInclusionTargetService, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader, Date now)
  {
    boolean result = false;
    for (ExclusionInclusionTarget inclusionTarget : exclusionInclusionTargetService.getActiveInclusionTargets(now))
      {
        if (subscriberGroupEpochReader.get(inclusionTarget.getExclusionInclusionTargetID()) != null && exclusionInclusionTargets.get(inclusionTarget.getExclusionInclusionTargetID()) != null)
          {
            if (Objects.equals(subscriberGroupEpochReader.get(inclusionTarget.getExclusionInclusionTargetID()), exclusionInclusionTargets.get(inclusionTarget.getExclusionInclusionTargetID())))
              {
                result = true;
                break;
              }
          }
      }
    return result;
  }
  
  /****************************************
  *
  *  getInExclusionList
  *
  ****************************************/
  
  public boolean getInExclusionList(ExclusionInclusionTargetService exclusionInclusionTargetService, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader, Date now)
  {
    boolean result = false;
    for (ExclusionInclusionTarget exclusionTarget : exclusionInclusionTargetService.getActiveExclusionTargets(now))
      {
        if (subscriberGroupEpochReader.get(exclusionTarget.getExclusionInclusionTargetID()) != null && exclusionInclusionTargets.get(exclusionTarget.getExclusionInclusionTargetID()) != null)
          {
            if (Objects.equals(subscriberGroupEpochReader.get(exclusionTarget.getExclusionInclusionTargetID()), exclusionInclusionTargets.get(exclusionTarget.getExclusionInclusionTargetID())))
              {
                result = true;
                break;
              }
          }
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
    b.append("," + universalControlGroup);
    b.append("," + languageID);
    b.append("," + extendedSubscriberProfile);
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

  /*****************************************
  *
  *  getDateString
  *
  *****************************************/

  public String getDateString(Date date)

  {
    String result = null;
    if (null == date) return result;
    try
      {
        SimpleDateFormat dateFormat = new SimpleDateFormat(Deployment.getAPIresponseDateFormat());
        dateFormat.setTimeZone(TimeZone.getTimeZone(Deployment.getBaseTimeZone()));
        result = dateFormat.format(date);
      }
    catch (Exception e)
      {
    	log.warn(e.getMessage());
      }
    return result;
  }
  
  public void validateUpdateProfileRequest(JSONObject jsonRoot) throws ValidateUpdateProfileRequestException
  {
    //
    //  read
    //
    
    String evolutionSubscriberStatus = readString(jsonRoot, "evolutionSubscriberStatus", true);
    String language = readString(jsonRoot, "language", true);
    
    //
    //  validate
    //
    
    if(language != null && (Deployment.getSupportedLanguages().get(language) == null)) throw new ValidateUpdateProfileRequestException(RESTAPIGenericReturnCodes.BAD_FIELD_VALUE.getGenericResponseMessage() + " (language) ", RESTAPIGenericReturnCodes.BAD_FIELD_VALUE.getGenericResponseCode());
    if(evolutionSubscriberStatus != null && (EvolutionSubscriberStatus.fromExternalRepresentation(evolutionSubscriberStatus) == EvolutionSubscriberStatus.Unknown)) throw new ValidateUpdateProfileRequestException(RESTAPIGenericReturnCodes.BAD_FIELD_VALUE.getGenericResponseMessage() + " (evolutionSubscriberStatus) ", RESTAPIGenericReturnCodes.BAD_FIELD_VALUE.getGenericResponseCode());

    //
    // validateUpdateProfileRequestFields
    //
    
    validateUpdateProfileRequestFields(jsonRoot);
    
    if (extendedSubscriberProfile != null) extendedSubscriberProfile.validateUpdateProfileRequestFields(jsonRoot);
  }
  
  /*****************************************
  *
  *  validateDateFromString
  *
  *****************************************/

 protected Date validateDateFromString(String dateString) throws ValidateUpdateProfileRequestException
 {
   Date result = null;
   if (dateString != null)
     {
       try 
         {
           result = GUIManagedObject.parseDateField(dateString);
         }
       catch(JSONUtilitiesException ex)
         {
           throw new ValidateUpdateProfileRequestException(RESTAPIGenericReturnCodes.BAD_FIELD_VALUE.getGenericResponseMessage()+"(invalid date "+dateString+")", RESTAPIGenericReturnCodes.BAD_FIELD_VALUE.getGenericResponseCode());
         }
       
     }
   return result;
 }
  
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
  *  readBoolean
  *
  *****************************************/
  
  protected Boolean readBoolean(JSONObject jsonRoot, String key, boolean validateNotEmpty) throws ValidateUpdateProfileRequestException
  {
    Boolean result = readBoolean(jsonRoot, key);
    if (validateNotEmpty && (result == null) && jsonRoot.containsKey(key))
      {
        throw new ValidateUpdateProfileRequestException(RESTAPIGenericReturnCodes.BAD_FIELD_VALUE.getGenericResponseMessage() + " ("+key+") ", RESTAPIGenericReturnCodes.BAD_FIELD_VALUE.getGenericResponseCode());
      }
    return result;
  }
  
  /*****************************************
  *
  *  readBoolean
  *
  *****************************************/
  
  protected Boolean readBoolean(JSONObject jsonRoot, String key) throws ValidateUpdateProfileRequestException
  {
    Boolean result = null;
    try 
      {
        result = JSONUtilities.decodeBoolean(jsonRoot, key);
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
  
  /*****************************************
  *
  *  readDouble
  *
  *****************************************/
  
  protected Double readDouble(JSONObject jsonRoot, String key, boolean validateNotEmpty) throws ValidateUpdateProfileRequestException
  {
    Double result = readDouble(jsonRoot, key);
    if (validateNotEmpty && (result == null) && jsonRoot.containsKey(key))
      {
        throw new ValidateUpdateProfileRequestException(RESTAPIGenericReturnCodes.BAD_FIELD_VALUE.getGenericResponseMessage() + " ("+key+") ", RESTAPIGenericReturnCodes.BAD_FIELD_VALUE.getGenericResponseCode());
      }
    return result;
  }
  
  /*****************************************
  *
  *  readDouble
  *
  *****************************************/
  
  protected Double readDouble(JSONObject jsonRoot, String key) throws ValidateUpdateProfileRequestException
  {
    Double result = null;
    try 
      {
        result = JSONUtilities.decodeDouble(jsonRoot, key);
      }
    catch (JSONUtilitiesException e) 
      {
        throw new ValidateUpdateProfileRequestException(RESTAPIGenericReturnCodes.BAD_FIELD_VALUE.getGenericResponseMessage() + " ("+key+") ", RESTAPIGenericReturnCodes.BAD_FIELD_VALUE.getGenericResponseCode());
      }
    return result;
  }
  
  
  /*****************************************
  *
  *  class ValidateUpdateProfileRequestException
  *
  *****************************************/

 public static class ValidateUpdateProfileRequestException extends Exception
 {
   /*****************************************
    *
    *  data
    *
    *****************************************/

   private int responseCode;

   /*****************************************
    *
    *  accessors
    *
    *****************************************/

   public int getResponseCode() { return responseCode; }

   /*****************************************
    *
    *  constructor
    *
    *****************************************/

   public ValidateUpdateProfileRequestException(String responseMessage, int responseCode)
   {
     super(responseMessage);
     this.responseCode = responseCode;
   }

   /*****************************************
    *
    *  constructor - excpetion
    *
    *****************************************/

   public ValidateUpdateProfileRequestException(Throwable e)
   {
     super(e.getMessage(), e);
     this.responseCode = -1;
   }
 }
}
