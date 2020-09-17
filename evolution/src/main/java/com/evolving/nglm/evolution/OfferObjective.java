/*****************************************************************************
*
*  OfferObjective.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManagedObject.GUIDependencyDef;
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
import java.util.HashMap;
import java.util.Objects;
import java.util.List;
import java.util.Map;

@GUIDependencyDef(objectType = "offerObjective", serviceClass = OfferObjectiveService.class, dependencies = {"catalogCharacteristic"})
public class OfferObjective extends GUIManagedObject
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
    schemaBuilder.name("offer_objective");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("catalogCharacteristics", SchemaBuilder.array(Schema.STRING_SCHEMA).schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<OfferObjective> serde = new ConnectSerde<OfferObjective>(schema, false, OfferObjective.class, OfferObjective::pack, OfferObjective::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<OfferObjective> serde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private List<String> catalogCharacteristics;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getOfferObjectiveID() { return getGUIManagedObjectID(); }
  public String getOfferObjectiveName() { return getGUIManagedObjectName(); }
  public String getOfferObjectiveDisplay() { return getGUIManagedObjectDisplay(); }
  public String getDisplay() { return getGUIManagedObjectDisplay(); }
  public List<String> getCatalogCharacteristics() { return catalogCharacteristics; }
  
  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public OfferObjective(SchemaAndValue schemaAndValue, List<String> catalogCharacteristics)
  {
    super(schemaAndValue);
    this.catalogCharacteristics = catalogCharacteristics;
  }
  
  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    OfferObjective offerObjective = (OfferObjective) value;
    Struct struct = new Struct(schema);
    packCommon(struct, offerObjective);
    struct.put("catalogCharacteristics", offerObjective.getCatalogCharacteristics());
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static OfferObjective unpack(SchemaAndValue schemaAndValue)
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
    List<String> catalogCharacteristics = (List<String>) valueStruct.get("catalogCharacteristics");
    
    //
    //  return
    //

    return new OfferObjective(schemaAndValue, catalogCharacteristics);
  }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public OfferObjective(JSONObject jsonRoot, long epoch, GUIManagedObject existingOfferObjectiveUnchecked) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingOfferObjectiveUnchecked != null) ? existingOfferObjectiveUnchecked.getEpoch() : epoch);

    /*****************************************
    *
    *  existingOfferObjective
    *
    *****************************************/

    OfferObjective existingOfferObjective = (existingOfferObjectiveUnchecked != null && existingOfferObjectiveUnchecked instanceof OfferObjective) ? (OfferObjective) existingOfferObjectiveUnchecked : null;
    
    /*****************************************
    *
    *  attributes
    *
    *****************************************/

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

    if (epochChanged(existingOfferObjective))
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

  private boolean epochChanged(OfferObjective existingOfferObjective)
  {
    if (existingOfferObjective != null && existingOfferObjective.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingOfferObjective.getGUIManagedObjectID());
        epochChanged = epochChanged || ! Objects.equals(catalogCharacteristics, existingOfferObjective.getCatalogCharacteristics());
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

  public void validate(CatalogCharacteristicService catalogCharacteristicService, Date date) throws GUIManagerException
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
  }
  
  @Override public Map<String, List<String>> getGUIDependencies()
  {
    Map<String, List<String>> result = new HashMap<String, List<String>>();
    System.out.println("oFFEROBJ:"+((getCatalogCharacteristics()!=null && getCatalogCharacteristics().size()>0 )?getCatalogCharacteristics().get(0):"no val"));
    result.put("CatalogCharacteristic".toLowerCase(), getCatalogCharacteristics());
    return result;
  }
}
