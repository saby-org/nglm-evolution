/*****************************************************************************
*
*  CatalogObjective.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.SchemaUtilities;

import com.rii.utilities.JSONUtilities;
import com.rii.utilities.JSONUtilities.JSONUtilitiesException;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.Objects;
import java.util.ArrayList;
import java.util.List;

public class CatalogObjective extends GUIManagedObject
{
  /*****************************************
  *
  *  enum
  *
  *****************************************/

  //
  //  Section
  //

  public enum Section
  {
    Product("product"),
    Offer("offer"),
    Journey("journey"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private Section(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static Section fromExternalRepresentation(String externalRepresentation) { for (Section enumeratedValue : Section.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }

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
    schemaBuilder.name("catalogobjective");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("section", Schema.STRING_SCHEMA);
    schemaBuilder.field("catalogCharacteristics", SchemaBuilder.array(Schema.STRING_SCHEMA).schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<CatalogObjective> serde = new ConnectSerde<CatalogObjective>(schema, false, CatalogObjective.class, CatalogObjective::pack, CatalogObjective::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<CatalogObjective> serde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private Section section;
  private List<String> catalogCharacteristics;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getCatalogObjectiveID() { return getGUIManagedObjectID(); }
  public Section getSection() { return section; }
  public List<String> getCatalogCharacteristics() { return catalogCharacteristics; }
  
  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public CatalogObjective(SchemaAndValue schemaAndValue, Section section, List<String> catalogCharacteristics)
  {
    super(schemaAndValue);
    this.section = section;
    this.catalogCharacteristics = catalogCharacteristics;
  }
  
  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    CatalogObjective catalogObjective = (CatalogObjective) value;
    Struct struct = new Struct(schema);
    packCommon(struct, catalogObjective);
    struct.put("section", catalogObjective.getSection().getExternalRepresentation());
    struct.put("catalogCharacteristics", catalogObjective.getCatalogCharacteristics());
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static CatalogObjective unpack(SchemaAndValue schemaAndValue)
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
    Section section = Section.fromExternalRepresentation((String) valueStruct.get("section"));
    List<String> catalogCharacteristics = (List<String>) valueStruct.get("catalogCharacteristics");
    
    //
    //  return
    //

    return new CatalogObjective(schemaAndValue, section, catalogCharacteristics);
  }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public CatalogObjective(JSONObject jsonRoot, long epoch, GUIManagedObject existingCatalogObjectiveUnchecked) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingCatalogObjectiveUnchecked != null) ? existingCatalogObjectiveUnchecked.getEpoch() : epoch);

    /*****************************************
    *
    *  existingCatalogObjective
    *
    *****************************************/

    CatalogObjective existingCatalogObjective = (existingCatalogObjectiveUnchecked != null && existingCatalogObjectiveUnchecked instanceof CatalogObjective) ? (CatalogObjective) existingCatalogObjectiveUnchecked : null;
    
    /*****************************************
    *
    *  attributes
    *
    *****************************************/

    this.section = Section.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "section", true));
    this.catalogCharacteristics = decodeCatalogCharacteristics(JSONUtilities.decodeJSONArray(jsonRoot, "catalogCharacteristics", true));

    /*****************************************
    *
    *  validate
    *
    *****************************************/

    // none yet

    /*****************************************
    *
    *  epoch
    *
    *****************************************/

    if (epochChanged(existingCatalogObjective))
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

  private boolean epochChanged(CatalogObjective existingCatalogObjective)
  {
    if (existingCatalogObjective != null && existingCatalogObjective.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingCatalogObjective.getGUIManagedObjectID());
        epochChanged = epochChanged || ! Objects.equals(section, existingCatalogObjective.getSection());
        epochChanged = epochChanged || ! Objects.equals(catalogCharacteristics, existingCatalogObjective.getCatalogCharacteristics());
        return epochChanged;
      }
    else
      {
        return true;
      }
  }
}
