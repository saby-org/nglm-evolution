/*****************************************************************************
*
*  JourneyObjective.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.EvolutionUtilities.RoundingSelection;
import com.evolving.nglm.evolution.EvolutionUtilities.TimeUnit;
import com.evolving.nglm.evolution.GUIManagedObject.GUIDependencyDef;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.RLMDateUtils;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Objects;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@GUIDependencyDef(objectType = "journeyObjective", serviceClass = JourneyObjectiveService.class, dependencies = { "contactpolicy" , "catalogcharacteristic" , "journeyobjective"})
public class JourneyObjective extends GUIManagedObject implements GUIManagedObject.ElasticSearchMapping
{

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
    schemaBuilder.name("journey_objective");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("parentJourneyObjectiveID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("targetingLimitMaxSimultaneous", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("targetingLimitWaitingPeriodDuration", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("targetingLimitWaitingPeriodTimeUnit", Schema.STRING_SCHEMA);
    schemaBuilder.field("targetingLimitMaxOccurrence", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("targetingLimitSlidingWindowDuration", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("targetingLimitSlidingWindowTimeUnit", Schema.STRING_SCHEMA);
    schemaBuilder.field("contactPolicyID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("catalogCharacteristics", SchemaBuilder.array(Schema.STRING_SCHEMA).schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<JourneyObjective> serde = new ConnectSerde<JourneyObjective>(schema, false, JourneyObjective.class, JourneyObjective::pack, JourneyObjective::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<JourneyObjective> serde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private String parentJourneyObjectiveID;
  private Integer targetingLimitMaxSimultaneous;
  private Integer targetingLimitWaitingPeriodDuration;
  private TimeUnit targetingLimitWaitingPeriodTimeUnit;
  private Integer targetingLimitMaxOccurrence;
  private Integer targetingLimitSlidingWindowDuration;
  private TimeUnit targetingLimitSlidingWindowTimeUnit;
  private String contactPolicyID;
  private List<String> catalogCharacteristics;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getJourneyObjectiveID() { return getGUIManagedObjectID(); }
  public String getJourneyObjectiveName() { return getGUIManagedObjectName(); }
  public String getParentJourneyObjectiveID() { return parentJourneyObjectiveID; }
  public Integer getTargetingLimitMaxSimultaneous() { return targetingLimitMaxSimultaneous; }
  public Integer getTargetingLimitWaitingPeriodDuration() { return targetingLimitWaitingPeriodDuration; }
  public TimeUnit getTargetingLimitWaitingPeriodTimeUnit() { return targetingLimitWaitingPeriodTimeUnit; }
  public Integer getTargetingLimitMaxOccurrence() { return targetingLimitMaxOccurrence; }
  public Integer getTargetingLimitSlidingWindowDuration() { return targetingLimitSlidingWindowDuration; }
  public TimeUnit getTargetingLimitSlidingWindowTimeUnit() { return targetingLimitSlidingWindowTimeUnit; }
  public String getContactPolicyID() { return contactPolicyID; }
  public List<String> getCatalogCharacteristics() { return catalogCharacteristics; }

  //
  //  effective limit accessors
  //
  
  public Integer getEffectiveTargetingLimitMaxSimultaneous() { return targetingLimitMaxSimultaneous != null ? targetingLimitMaxSimultaneous : new Integer(Integer.MAX_VALUE); }
  public Date getEffectiveWaitingPeriodEndDate(Date now, int tenantID) { return (targetingLimitMaxSimultaneous != null && targetingLimitMaxSimultaneous == 1 && targetingLimitWaitingPeriodDuration != null) ? EvolutionUtilities.addTime(now, -1 * targetingLimitWaitingPeriodDuration,  targetingLimitWaitingPeriodTimeUnit, Deployment.getDeployment(tenantID).getTimeZone(), RoundingSelection.NoRound) : now; }
  public Integer getEffectiveTargetingLimitMaxOccurrence() { return targetingLimitMaxOccurrence != null ? targetingLimitMaxOccurrence : new Integer(Integer.MAX_VALUE); }
  public Date getEffectiveSlidingWindowStartDate(Date now, int tenantID) { return (targetingLimitSlidingWindowDuration != null) ? EvolutionUtilities.addTime(now, -1 * targetingLimitSlidingWindowDuration,  targetingLimitSlidingWindowTimeUnit, Deployment.getDeployment(tenantID).getTimeZone(), RoundingSelection.NoRound) : now; }
  
  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public JourneyObjective(SchemaAndValue schemaAndValue, String parentJourneyObjectiveID, Integer targetingLimitMaxSimultaneous, Integer targetingLimitWaitingPeriodDuration, TimeUnit targetingLimitWaitingPeriodTimeUnit, Integer targetingLimitMaxOccurrence, Integer targetingLimitSlidingWindowDuration, TimeUnit targetingLimitSlidingWindowTimeUnit, String contactPolicyID, List<String> catalogCharacteristics)
  {
    super(schemaAndValue);
    this.parentJourneyObjectiveID = parentJourneyObjectiveID;
    this.targetingLimitMaxSimultaneous = targetingLimitMaxSimultaneous;
    this.targetingLimitWaitingPeriodDuration = targetingLimitWaitingPeriodDuration;
    this.targetingLimitWaitingPeriodTimeUnit = targetingLimitWaitingPeriodTimeUnit;
    this.targetingLimitMaxOccurrence = targetingLimitMaxOccurrence;
    this.targetingLimitSlidingWindowDuration = targetingLimitSlidingWindowDuration;
    this.targetingLimitSlidingWindowTimeUnit = targetingLimitSlidingWindowTimeUnit;
    this.contactPolicyID = contactPolicyID; 
    this.catalogCharacteristics = catalogCharacteristics;
  }
  
  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    JourneyObjective journeyObjective = (JourneyObjective) value;
    Struct struct = new Struct(schema);
    packCommon(struct, journeyObjective);
    struct.put("parentJourneyObjectiveID", journeyObjective.getParentJourneyObjectiveID());
    struct.put("targetingLimitMaxSimultaneous", journeyObjective.getTargetingLimitMaxSimultaneous());
    struct.put("targetingLimitWaitingPeriodDuration", journeyObjective.getTargetingLimitWaitingPeriodDuration());
    struct.put("targetingLimitWaitingPeriodTimeUnit", journeyObjective.getTargetingLimitWaitingPeriodTimeUnit().getExternalRepresentation());
    struct.put("targetingLimitMaxOccurrence", journeyObjective.getTargetingLimitMaxOccurrence());
    struct.put("targetingLimitSlidingWindowDuration", journeyObjective.getTargetingLimitSlidingWindowDuration());
    struct.put("targetingLimitSlidingWindowTimeUnit", journeyObjective.getTargetingLimitSlidingWindowTimeUnit().getExternalRepresentation());
    struct.put("contactPolicyID", journeyObjective.getContactPolicyID());
    struct.put("catalogCharacteristics", journeyObjective.getCatalogCharacteristics());
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static JourneyObjective unpack(SchemaAndValue schemaAndValue)
  {
    //
    //  data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion1(schema.version()) : null;

    //
    //  unpack
    //

    Struct valueStruct = (Struct) value;
    String parentJourneyObjectiveID = valueStruct.getString("parentJourneyObjectiveID");
    Integer targetingLimitMaxSimultaneous = valueStruct.getInt32("targetingLimitMaxSimultaneous");
    Integer targetingLimitWaitingPeriodDuration = valueStruct.getInt32("targetingLimitWaitingPeriodDuration");
    TimeUnit targetingLimitWaitingPeriodTimeUnit = TimeUnit.fromExternalRepresentation(valueStruct.getString("targetingLimitWaitingPeriodTimeUnit"));
    Integer targetingLimitMaxOccurrence = valueStruct.getInt32("targetingLimitMaxOccurrence");
    Integer targetingLimitSlidingWindowDuration = valueStruct.getInt32("targetingLimitSlidingWindowDuration");
    TimeUnit targetingLimitSlidingWindowTimeUnit = TimeUnit.fromExternalRepresentation(valueStruct.getString("targetingLimitSlidingWindowTimeUnit"));
    String contactPolicyID = valueStruct.getString("contactPolicyID");
    List<String> catalogCharacteristics = (List<String>) valueStruct.get("catalogCharacteristics");

    //
    //  return
    //

    return new JourneyObjective(schemaAndValue, parentJourneyObjectiveID, targetingLimitMaxSimultaneous, targetingLimitWaitingPeriodDuration, targetingLimitWaitingPeriodTimeUnit, targetingLimitMaxOccurrence, targetingLimitSlidingWindowDuration, targetingLimitSlidingWindowTimeUnit, contactPolicyID, catalogCharacteristics);
  }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public JourneyObjective(JSONObject jsonRoot, long epoch, GUIManagedObject existingJourneyObjectiveUnchecked, int tenantID) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingJourneyObjectiveUnchecked != null) ? existingJourneyObjectiveUnchecked.getEpoch() : epoch, tenantID);

    /*****************************************
    *
    *  existingJourneyObjective
    *
    *****************************************/

    JourneyObjective existingJourneyObjective = (existingJourneyObjectiveUnchecked != null && existingJourneyObjectiveUnchecked instanceof JourneyObjective) ? (JourneyObjective) existingJourneyObjectiveUnchecked : null;
    
    /*****************************************
    *
    *  attributes
    *
    *****************************************/

    this.parentJourneyObjectiveID = JSONUtilities.decodeString(jsonRoot, "parentJourneyObjectiveID", false);
    this.targetingLimitMaxSimultaneous = JSONUtilities.decodeInteger(jsonRoot, "targetingLimitMaxSimultaneous", false);
    this.targetingLimitWaitingPeriodDuration = JSONUtilities.decodeInteger(jsonRoot, "targetingLimitWaitingPeriodDuration", false);
    this.targetingLimitWaitingPeriodTimeUnit = TimeUnit.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "targetingLimitWaitingPeriodTimeUnit", "(unknown)"));
    this.targetingLimitMaxOccurrence = JSONUtilities.decodeInteger(jsonRoot, "targetingLimitMaxOccurrence", false);
    this.targetingLimitSlidingWindowDuration = JSONUtilities.decodeInteger(jsonRoot, "targetingLimitSlidingWindowDuration", false);
    this.targetingLimitSlidingWindowTimeUnit = TimeUnit.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "targetingLimitSlidingWindowTimeUnit", "(unknown)"));
    this.contactPolicyID = JSONUtilities.decodeString(jsonRoot, "contactPolicyID", false);
    this.catalogCharacteristics = decodeCatalogCharacteristics(JSONUtilities.decodeJSONArray(jsonRoot, "catalogCharacteristics", true));

    /*****************************************
    *
    *  validate
    *
    *****************************************/

    if (getRawEffectiveStartDate() != null) throw new GUIManagerException("unsupported start date", JSONUtilities.decodeString(jsonRoot, "effectiveStartDate", false));
    if (getRawEffectiveEndDate() != null) throw new GUIManagerException("unsupported end date", JSONUtilities.decodeString(jsonRoot, "effectiveEndDate", false));

    /*****************************************
    *
    *  epoch
    *
    *****************************************/

    if (epochChanged(existingJourneyObjective))
      {
        this.setEpoch(epoch);
      }
  }

  /*****************************************
  *
  *  decodeCatalogCharacteristics
  *
  *****************************************/

  private List<String> decodeCatalogCharacteristics(JSONArray jsonArray) throws GUIManagerException
  {
    List<String> catalogCharacteristics = new ArrayList<String>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        JSONObject catalogCharacteristicJSON = (JSONObject) jsonArray.get(i);
        catalogCharacteristics.add(JSONUtilities.decodeString(catalogCharacteristicJSON, "catalogCharacteristicID", true));
      }
    return catalogCharacteristics;
  }

  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(JourneyObjective existingJourneyObjective)
  {
    if (existingJourneyObjective != null && existingJourneyObjective.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingJourneyObjective.getGUIManagedObjectID());
        epochChanged = epochChanged || ! Objects.equals(parentJourneyObjectiveID, existingJourneyObjective.getParentJourneyObjectiveID());
        epochChanged = epochChanged || ! Objects.equals(targetingLimitMaxSimultaneous, existingJourneyObjective.getTargetingLimitMaxSimultaneous());
        epochChanged = epochChanged || ! Objects.equals(targetingLimitWaitingPeriodDuration, existingJourneyObjective.getTargetingLimitWaitingPeriodDuration());
        epochChanged = epochChanged || ! Objects.equals(targetingLimitWaitingPeriodTimeUnit, existingJourneyObjective.getTargetingLimitWaitingPeriodTimeUnit());
        epochChanged = epochChanged || ! Objects.equals(targetingLimitMaxOccurrence, existingJourneyObjective.getTargetingLimitMaxOccurrence());
        epochChanged = epochChanged || ! Objects.equals(targetingLimitSlidingWindowDuration, existingJourneyObjective.getTargetingLimitSlidingWindowDuration());
        epochChanged = epochChanged || ! Objects.equals(targetingLimitSlidingWindowTimeUnit, existingJourneyObjective.getTargetingLimitSlidingWindowTimeUnit());
        epochChanged = epochChanged || ! Objects.equals(contactPolicyID, existingJourneyObjective.getContactPolicyID());
        epochChanged = epochChanged || ! Objects.equals(catalogCharacteristics, existingJourneyObjective.getCatalogCharacteristics());
        return epochChanged;
      }
    else
      {
        return true;
      }
  }
  
  /*****************************************
  *
  *  validate
  *
  *****************************************/

  public void validate(JourneyObjectiveService journeyObjectiveService, ContactPolicyService contactPolicyService, CatalogCharacteristicService catalogCharacteristicService, Date date) throws GUIManagerException
  {
    /*****************************************
    *
    *  validate contact policy exists and is active
    *
    *****************************************/

    if (contactPolicyID != null)
      {
        ContactPolicy contactPolicy = contactPolicyService.getActiveContactPolicy(contactPolicyID, date);
        if (contactPolicy == null) throw new GUIManagerException("unknown contact policy", contactPolicyID);
      }

    /*****************************************
    *
    *  validate catalog characteristics exist and are active
    *
    *****************************************/

    for (String catalogCharacteristicID : catalogCharacteristics)
      {
        CatalogCharacteristic catalogCharacteristic = catalogCharacteristicService.getActiveCatalogCharacteristic(catalogCharacteristicID, date);
        if (catalogCharacteristic == null) throw new GUIManagerException("unknown catalog characteristic", catalogCharacteristicID);
      }
    
    /*****************************************
    *
    *  validate journey objective ancestors
    *  - ensure all parents exist and are active
    *  - ensure no cycles
    *
    *****************************************/

    JourneyObjective walk = this;
    while (walk != null)
      {
        //
        //  done if no parent
        //
        
        if (walk.getParentJourneyObjectiveID() == null) break;
        
        //
        //  verify parent
        //   1) exists
        //   2) is a journeyObjective
        //   3) is active
        //   4) does not create a cycle
        //
        
        GUIManagedObject uncheckedParent = journeyObjectiveService.getStoredJourneyObjective(walk.getParentJourneyObjectiveID());
        if (uncheckedParent == null) throw new GUIManagerException("unknown journey objective ancestor", walk.getParentJourneyObjectiveID());
        if (uncheckedParent instanceof IncompleteObject) throw new GUIManagerException("invalid journey objective ancestor", walk.getParentJourneyObjectiveID());
        JourneyObjective parent = (JourneyObjective) uncheckedParent;
        if (! journeyObjectiveService.isActiveJourneyObjective(parent, date)) throw new GUIManagerException("inactive journey objective ancestor", walk.getParentJourneyObjectiveID());
        if (parent.equals(this)) throw new GUIManagerException("cycle in journey objective hierarchy", getJourneyObjectiveID());

        //
        //  "recurse"
        //
        
        walk = parent;
      }
  }
  
  @Override public Map<String, List<String>> getGUIDependencies(List<GUIService> guiServiceList, int tenantID)
  {
    Map<String, List<String>> result = new HashMap<String, List<String>>();
    List<String> contactPolicyIDs = new ArrayList<String>();
    List<String> parentJOIDs = new ArrayList<String>();
    parentJOIDs.add(getParentJourneyObjectiveID());
    contactPolicyIDs.add(getContactPolicyID());
    result.put("contactpolicy", contactPolicyIDs);
    result.put("catalogcharacteristic".toLowerCase(), getCatalogCharacteristics());
    result.put("journeyobjective".toLowerCase(), parentJOIDs);
    return result;
  }
  @Override
  public String getESDocumentID()
  {
    return this.getJourneyObjectiveID();
  }
  @Override
  public Map<String, Object> getESDocumentMap(JourneyService journeyService, TargetService targetService, JourneyObjectiveService journeyObjectiveService, ContactPolicyService contactPolicyService)
  {
    Date now = SystemTime.getCurrentTime();
    Map<String,Object> documentMap = new HashMap<String,Object>();
         
    // We read all data from JSONRepresentation()
    // because native data in object is sometimes not correct
    
    JSONObject jr = this.getJSONRepresentation();
    if (jr != null)
      {
        documentMap.put("id",      jr.get("id"));
        documentMap.put("display", jr.get("display"));
        String contactPolicyID = (String) jr.get("contactPolicyID");
        ContactPolicy contactPolicy = contactPolicyService.getActiveContactPolicy(contactPolicyID, now);
        documentMap.put("contactPolicy", (contactPolicy == null) ? "" : contactPolicy.getGUIManagedObjectDisplay());
        documentMap.put("timestamp",     RLMDateUtils.formatDateForElasticsearchDefault(SystemTime.getCurrentTime()));
      }
    return documentMap;
  }
  @Override
  public String getESIndexName()
  {
    return "mapping_journeyobjective";
  }
}
