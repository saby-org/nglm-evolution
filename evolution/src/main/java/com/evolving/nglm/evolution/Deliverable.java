/*****************************************************************************
*
*  Deliverable.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.GUIService.GUIManagedObjectListener;
import com.evolving.nglm.evolution.Target.TargetingType;

public class Deliverable extends GUIManagedObject implements GUIManagedObject.ElasticSearchMapping
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
    schemaBuilder.name("deliverable");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),3));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("fulfillmentProviderID", Schema.STRING_SCHEMA);
    schemaBuilder.field("externalAccountID", SchemaBuilder.string().defaultValue("").schema());
    schemaBuilder.field("unitaryCost", Schema.INT32_SCHEMA);
    schemaBuilder.field("generatedFromAccount", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("label", Schema.OPTIONAL_STRING_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<Deliverable> serde = new ConnectSerde<Deliverable>(schema, false, Deliverable.class, Deliverable::pack, Deliverable::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<Deliverable> serde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private String fulfillmentProviderID;
  private String externalAccountID;
  private int unitaryCost;
  private boolean generatedFromAccount;
  private String label;
  
  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getDeliverableID() { return getGUIManagedObjectID(); }
  public String getDeliverableName() { return getGUIManagedObjectName(); }
  public String getDeliverableDisplay() { return getGUIManagedObjectDisplay(); }
  public String getFulfillmentProviderID() { return fulfillmentProviderID; }
  public String getExternalAccountID() { return externalAccountID; }
  public int getUnitaryCost() { return unitaryCost; }
  public boolean getGeneratedFromAccount() { return generatedFromAccount; }
  public String getLabel() { return label; }
   
  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public Deliverable(SchemaAndValue schemaAndValue, String fulfillmentProviderID, String externalAccountID, int unitaryCost, boolean generatedFromAccount, String label)
  {
    super(schemaAndValue);
    this.fulfillmentProviderID = fulfillmentProviderID;
    this.externalAccountID = externalAccountID;
    this.unitaryCost = unitaryCost;
    this.generatedFromAccount = generatedFromAccount;
    this.label = label;
   }
  
  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    Deliverable deliverable = (Deliverable) value;
    Struct struct = new Struct(schema);
    packCommon(struct, deliverable);
    struct.put("fulfillmentProviderID", deliverable.getFulfillmentProviderID());
    struct.put("externalAccountID", deliverable.getExternalAccountID());
    struct.put("unitaryCost", deliverable.getUnitaryCost());
    struct.put("generatedFromAccount", deliverable.getGeneratedFromAccount());
    struct.put("label", deliverable.getLabel());
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static Deliverable unpack(SchemaAndValue schemaAndValue)
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
    String fulfillmentProviderID = valueStruct.getString("fulfillmentProviderID");
    String externalAccountID = (schemaVersion >= 2) ? valueStruct.getString("externalAccountID") : fulfillmentProviderID;
    int unitaryCost = valueStruct.getInt32("unitaryCost");
    boolean generatedFromAccount = valueStruct.getBoolean("generatedFromAccount");
    String label = (schemaVersion >= 3) ? valueStruct.getString("label") : "";
     //
    //  return
    //

    return new Deliverable(schemaAndValue, fulfillmentProviderID, externalAccountID, unitaryCost, generatedFromAccount, label);
  }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public Deliverable(JSONObject jsonRoot, long epoch, GUIManagedObject existingDeliverableUnchecked, int tenantID) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingDeliverableUnchecked != null) ? existingDeliverableUnchecked.getEpoch() : epoch, tenantID);

    /*****************************************
    *
    *  existingDeliverable
    *
    *****************************************/

    Deliverable existingDeliverable = (existingDeliverableUnchecked != null && existingDeliverableUnchecked instanceof Deliverable) ? (Deliverable) existingDeliverableUnchecked : null;
    
    /*****************************************
    *
    *  attributes
    *
    *****************************************/

    this.fulfillmentProviderID = JSONUtilities.decodeString(jsonRoot, "fulfillmentProviderID", true);
    this.externalAccountID = JSONUtilities.decodeString(jsonRoot, "externalAccountID", true);
    this.unitaryCost = JSONUtilities.decodeInteger(jsonRoot, "unitaryCost", true);
    this.generatedFromAccount = JSONUtilities.decodeBoolean(jsonRoot, "generatedFromAccount", Boolean.FALSE);
    this.label = JSONUtilities.decodeString(jsonRoot, "label", false);
    
    /*****************************************
    *
    *  validate
    *
    *****************************************/

    //
    //  validate provider
    //

    //  TBD

    /*****************************************
    *
    *  epoch
    *
    *****************************************/

    if (epochChanged(existingDeliverable))
      {
        this.setEpoch(epoch);
      }
  }

  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(Deliverable existingDeliverable)
  {
    if (existingDeliverable != null && existingDeliverable.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingDeliverable.getGUIManagedObjectID());
        epochChanged = epochChanged || ! Objects.equals(fulfillmentProviderID, existingDeliverable.getFulfillmentProviderID());
        epochChanged = epochChanged || ! Objects.equals(externalAccountID, existingDeliverable.getExternalAccountID());
        epochChanged = epochChanged || ! (unitaryCost == existingDeliverable.getUnitaryCost());
        epochChanged = epochChanged || ! (generatedFromAccount == existingDeliverable.getGeneratedFromAccount());
        epochChanged = epochChanged || ! Objects.equals(label, existingDeliverable.getLabel());
        return epochChanged;
      }
    else
      {
        return true;
      }
  }
  @Override
  public String getESDocumentID()
  {
    return "_" + this.getDeliverableID().hashCode();   
  }
  @Override
  public Map<String, Object> getESDocumentMap(JourneyService journeyService, TargetService targetService, JourneyObjectiveService journeyObjectiveService, ContactPolicyService contactPolicyService)
  {
    Map<String,Object> documentMap = new HashMap<String,Object>();
    documentMap.put("deliverableID", this.getDeliverableID());
    documentMap.put("deliverableName", this.getDeliverableDisplay());
    documentMap.put("deliverableActive", this.getActive());
    documentMap.put("deliverableProviderID", this.getFulfillmentProviderID());
    
    return documentMap;
  }
  @Override
  public String getESIndexName()
  {
    return "mapping_deliverables";
  }
}
