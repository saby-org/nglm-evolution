package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class BadgeObjectiveInstance
{


  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(BadgeObjectiveInstance.class);

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
    //
    //  schema
    //
    
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("badge_objective_instance");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("badgeObjectiveID", Schema.STRING_SCHEMA);
    schemaBuilder.field("catalogCharacteristics", SchemaBuilder.array(CatalogCharacteristicInstance.schema()).schema());
    schema = schemaBuilder.build();
  };

  //
  //  accessor
  //

  public static Schema schema() { return schema; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private String badgeObjectiveID;
  private Set<CatalogCharacteristicInstance> catalogCharacteristics;

  /*****************************************
  *
  *  constructor -- simple
  *
  *****************************************/

  public BadgeObjectiveInstance(String badgeObjectiveID, Set<CatalogCharacteristicInstance> catalogCharacteristics)
  {
    this.badgeObjectiveID = badgeObjectiveID;
    this.catalogCharacteristics = catalogCharacteristics;
  }

  /*****************************************
  *
  *  constructor -- external JSON
  *
  *****************************************/

  BadgeObjectiveInstance(JSONObject jsonRoot, CatalogCharacteristicService catalogCharacteristicService) throws GUIManagerException
  {
    this.badgeObjectiveID = JSONUtilities.decodeString(jsonRoot, "badgeObjectiveID", true);
    this.catalogCharacteristics = decodeCatalogCharacteristics(JSONUtilities.decodeJSONArray(jsonRoot, "catalogCharacteristics", false), catalogCharacteristicService);
  }

  /*****************************************
  *
  *  decodeCatalogCharacteristics
  *
  *****************************************/

  private Set<CatalogCharacteristicInstance> decodeCatalogCharacteristics(JSONArray jsonArray, CatalogCharacteristicService catalogCharacteristicService) throws GUIManagerException
  {
    Set<CatalogCharacteristicInstance> result = new HashSet<CatalogCharacteristicInstance>();
    if (jsonArray != null)
      {
        for (int i=0; i<jsonArray.size(); i++)
          {
            result.add(new CatalogCharacteristicInstance((JSONObject) jsonArray.get(i), catalogCharacteristicService));
          }
      }
    return result;
  }

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getBadgeObjectiveID() { return badgeObjectiveID; }
  public Set<CatalogCharacteristicInstance> getCatalogCharacteristics() { return catalogCharacteristics; }

  /*****************************************
  *
  *  serde
  *
  *****************************************/

  public static ConnectSerde<BadgeObjectiveInstance> serde()
  {
    return new ConnectSerde<BadgeObjectiveInstance>(schema, false, BadgeObjectiveInstance.class, BadgeObjectiveInstance::pack, BadgeObjectiveInstance::unpack);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    BadgeObjectiveInstance badgeBadgeObjective = (BadgeObjectiveInstance) value;
    Struct struct = new Struct(schema);
    struct.put("badgeObjectiveID", badgeBadgeObjective.getBadgeObjectiveID());
    struct.put("catalogCharacteristics", packCatalogCharacteristics(badgeBadgeObjective.getCatalogCharacteristics()));
    return struct;
  }

  /****************************************
  *
  *  packCatalogCharacteristics
  *
  ****************************************/

  private static List<Object> packCatalogCharacteristics(Set<CatalogCharacteristicInstance> catalogCharacteristics)
  {
    List<Object> result = new ArrayList<Object>();
    for (CatalogCharacteristicInstance catalogCharacteristic : catalogCharacteristics)
      {
        result.add(CatalogCharacteristicInstance.pack(catalogCharacteristic));
      }
    return result;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static BadgeObjectiveInstance unpack(SchemaAndValue schemaAndValue)
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
    String badgeObjectiveID = valueStruct.getString("badgeObjectiveID");
    Set<CatalogCharacteristicInstance> catalogCharacteristics = unpackCatalogCharacteristics(schema.field("catalogCharacteristics").schema(), valueStruct.get("catalogCharacteristics"));
    
    //
    //  return
    //

    return new BadgeObjectiveInstance(badgeObjectiveID, catalogCharacteristics);
  }

  /*****************************************
  *
  *  unpackBadgeCatalogCharacteristics
  *
  *****************************************/

  private static Set<CatalogCharacteristicInstance> unpackCatalogCharacteristics(Schema schema, Object value)
  {
    //
    //  get schema for BadgeCatalogCharacteristic
    //

    Schema propertySchema = schema.valueSchema();
    
    //
    //  unpack
    //

    Set<CatalogCharacteristicInstance> result = new HashSet<CatalogCharacteristicInstance>();
    List<Object> valueArray = (List<Object>) value;
    for (Object property : valueArray)
      {
        result.add(CatalogCharacteristicInstance.unpack(new SchemaAndValue(propertySchema, property)));
      }

    //
    //  return
    //

    return result;
  }

  /*****************************************
  *
  *  equals
  *
  *****************************************/

  public boolean equals(Object obj)
  {
    boolean result = false;
    if (obj instanceof BadgeObjectiveInstance)
      {
        BadgeObjectiveInstance badgeBadgeObjective = (BadgeObjectiveInstance) obj;
        result = true;
        result = result && Objects.equals(badgeObjectiveID, badgeBadgeObjective.getBadgeObjectiveID());
        result = result && Objects.equals(catalogCharacteristics, badgeBadgeObjective.getCatalogCharacteristics());
      }
    return result;
  }

  /*****************************************
  *
  *  hashCode
  *
  *****************************************/

  public int hashCode()
  {
    return badgeObjectiveID.hashCode();
  }
  
  @Override
  public String toString()
  {
    return "BadgeObjectiveInstance [badgeObjectiveID=" + badgeObjectiveID + ", catalogCharacteristics=" + catalogCharacteristics + "]";
  }

}
