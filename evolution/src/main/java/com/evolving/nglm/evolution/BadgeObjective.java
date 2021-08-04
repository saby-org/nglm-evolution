package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.GUIManagedObject.GUIDependencyDef;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

@GUIDependencyDef(objectType = "badgeObjective", serviceClass = BadgeObjectiveService.class, dependencies = {"catalogcharacteristic"})
public class BadgeObjective extends GUIManagedObject
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
    schemaBuilder.name("badge_objective");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("catalogCharacteristics", SchemaBuilder.array(Schema.STRING_SCHEMA).schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<BadgeObjective> serde = new ConnectSerde<BadgeObjective>(schema, false, BadgeObjective.class, BadgeObjective::pack, BadgeObjective::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<BadgeObjective> serde() { return serde; }
  
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

  public String getBadgeObjectiveID() { return getGUIManagedObjectID(); }
  public String getBadgeObjectiveName() { return getGUIManagedObjectName(); }
  public String getBadgeObjectiveDisplay() { return getGUIManagedObjectDisplay(); }
  public String getDisplay() { return getGUIManagedObjectDisplay(); }
  public List<String> getCatalogCharacteristics() { return catalogCharacteristics; }
  
  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public BadgeObjective(SchemaAndValue schemaAndValue, List<String> catalogCharacteristics)
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
    BadgeObjective badgeObjective = (BadgeObjective) value;
    Struct struct = new Struct(schema);
    packCommon(struct, badgeObjective);
    struct.put("catalogCharacteristics", badgeObjective.getCatalogCharacteristics());
    return struct;
  }
  
  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static BadgeObjective unpack(SchemaAndValue schemaAndValue)
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

    return new BadgeObjective(schemaAndValue, catalogCharacteristics);
  }
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public BadgeObjective(JSONObject jsonRoot, long epoch, GUIManagedObject existingBadgeObjectiveUnchecked, int tenantID) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingBadgeObjectiveUnchecked != null) ? existingBadgeObjectiveUnchecked.getEpoch() : epoch, tenantID);

    /*****************************************
    *
    *  existingBadgeObjective
    *
    *****************************************/

    BadgeObjective existingBadgeObjective = (existingBadgeObjectiveUnchecked != null && existingBadgeObjectiveUnchecked instanceof BadgeObjective) ? (BadgeObjective) existingBadgeObjectiveUnchecked : null;
    
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

    if (epochChanged(existingBadgeObjective))
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

  private boolean epochChanged(BadgeObjective existingBadgeObjective)
  {
    if (existingBadgeObjective != null && existingBadgeObjective.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingBadgeObjective.getGUIManagedObjectID());
        epochChanged = epochChanged || ! Objects.equals(catalogCharacteristics, existingBadgeObjective.getCatalogCharacteristics());
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
  
  @Override public Map<String, List<String>> getGUIDependencies(int tenantID)
  {
    Map<String, List<String>> result = new HashMap<String, List<String>>();
    result.put("catalogcharacteristic".toLowerCase(), getCatalogCharacteristics());
    return result;
  }

}
