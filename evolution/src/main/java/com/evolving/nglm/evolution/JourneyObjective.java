/*****************************************************************************
*
*  JourneyObjective.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.EvaluationCriterion.TimeUnit;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;

import com.evolving.nglm.core.JSONUtilities;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.Date;
import java.util.Objects;
import java.util.List;

public class JourneyObjective extends GUIManagedObject
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
    schemaBuilder.field("contactPolicyMaxOccurrence", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("contactPolicyMaxSimultaneous", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("contactPolicyWaitingPeriodDuration", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("contactPolicyWaitingPeriodTimeUnit", Schema.STRING_SCHEMA);
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
  private Integer contactPolicyMaxOccurrence;
  private Integer contactPolicyMaxSimultaneous;
  private Integer contactPolicyWaitingPeriodDuration;
  private TimeUnit contactPolicyWaitingPeriodTimeUnit;
  private List<String> catalogCharacteristics;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getJourneyObjectiveID() { return getGUIManagedObjectID(); }
  public String getJourneyObjectiveName() { return getGUIManagedObjectName(); }
  public String getParentJourneyObjectiveID() { return parentJourneyObjectiveID; }
  public Integer getContactPolicyMaxOccurrence() { return contactPolicyMaxOccurrence; }
  public Integer getContactPolicyMaxSimultaneous() { return contactPolicyMaxSimultaneous; }
  public Integer getContactPolicyWaitingPeriodDuration() { return contactPolicyWaitingPeriodDuration; }
  public TimeUnit getContactPolicyWaitingPeriodTimeUnit() { return contactPolicyWaitingPeriodTimeUnit; }
  public List<String> getCatalogCharacteristics() { return catalogCharacteristics; }
  
  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public JourneyObjective(SchemaAndValue schemaAndValue, String parentJourneyObjectiveID, Integer contactPolicyMaxOccurrence, Integer contactPolicyMaxSimultaneous, Integer contactPolicyWaitingPeriodDuration, TimeUnit contactPolicyWaitingPeriodTimeUnit, List<String> catalogCharacteristics)
  {
    super(schemaAndValue);
    this.parentJourneyObjectiveID = parentJourneyObjectiveID;
    this.contactPolicyMaxOccurrence = contactPolicyMaxOccurrence;
    this.contactPolicyMaxSimultaneous = contactPolicyMaxSimultaneous;
    this.contactPolicyWaitingPeriodDuration = contactPolicyWaitingPeriodDuration;
    this.contactPolicyWaitingPeriodTimeUnit = contactPolicyWaitingPeriodTimeUnit;
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
    struct.put("contactPolicyMaxOccurrence", journeyObjective.getContactPolicyMaxOccurrence());
    struct.put("contactPolicyMaxSimultaneous", journeyObjective.getContactPolicyMaxSimultaneous());
    struct.put("contactPolicyWaitingPeriodDuration", journeyObjective.getContactPolicyWaitingPeriodDuration());
    struct.put("contactPolicyWaitingPeriodTimeUnit", journeyObjective.getContactPolicyWaitingPeriodTimeUnit().getExternalRepresentation());
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
    Integer contactPolicyMaxOccurrence = valueStruct.getInt32("contactPolicyMaxOccurrence");
    Integer contactPolicyMaxSimultaneous = valueStruct.getInt32("contactPolicyMaxSimultaneous");
    Integer contactPolicyWaitingPeriodDuration = valueStruct.getInt32("contactPolicyWaitingPeriodDuration");
    TimeUnit contactPolicyWaitingPeriodTimeUnit = TimeUnit.fromExternalRepresentation(valueStruct.getString("contactPolicyWaitingPeriodTimeUnit"));
    List<String> catalogCharacteristics = (List<String>) valueStruct.get("catalogCharacteristics");

    //
    //  return
    //

    return new JourneyObjective(schemaAndValue, parentJourneyObjectiveID, contactPolicyMaxOccurrence, contactPolicyMaxSimultaneous, contactPolicyWaitingPeriodDuration, contactPolicyWaitingPeriodTimeUnit, catalogCharacteristics);
  }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public JourneyObjective(JSONObject jsonRoot, long epoch, GUIManagedObject existingJourneyObjectiveUnchecked) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingJourneyObjectiveUnchecked != null) ? existingJourneyObjectiveUnchecked.getEpoch() : epoch);

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
    this.contactPolicyMaxOccurrence = JSONUtilities.decodeInteger(jsonRoot, "contactPolicyMaxOccurrence", false);
    this.contactPolicyMaxSimultaneous = JSONUtilities.decodeInteger(jsonRoot, "contactPolicyMaxSimultaneous", false);
    this.contactPolicyWaitingPeriodDuration = JSONUtilities.decodeInteger(jsonRoot, "contactPolicyWaitingPeriodDuration", false);
    this.contactPolicyWaitingPeriodTimeUnit = TimeUnit.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "contactPolicyWaitingPeriodTimeUnit", "(unknown)"));
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

  public void validate(JourneyObjectiveService journeyObjectiveService, CatalogCharacteristicService catalogCharacteristicService, Date date) throws GUIManagerException
  {
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
}
