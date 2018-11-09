package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

/****************************************************************************
*
*  CatalogCharacteristicInstance
*
****************************************************************************/

public class CatalogCharacteristicInstance
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
    //
    //  schema
    //
    

    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("offer_catalog_characteristic");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("catalogCharacteristicID", Schema.STRING_SCHEMA);
    schemaBuilder.field("singletonValue", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("listValues", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().schema());
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

  private String catalogCharacteristicID;
  private String singletonValue;
  private List<String> listValues;

  /*****************************************
  *
  *  constructor -- simple
  *
  *****************************************/

  private CatalogCharacteristicInstance(String catalogCharacteristicID, String singletonValue, List<String> listValues)
  {
    this.catalogCharacteristicID = catalogCharacteristicID;
    this.singletonValue = singletonValue;
    this.listValues = listValues;
  }

  /*****************************************
  *
  *  constructor -- external JSON
  *
  *****************************************/

  CatalogCharacteristicInstance(JSONObject jsonRoot) throws GUIManagerException
  {
    //
    //  catalog characteristic
    //

    this.catalogCharacteristicID = JSONUtilities.decodeString(jsonRoot, "catalogCharacteristicID", true);
    Object valueJSON = jsonRoot.get("value");
    if (valueJSON instanceof JSONArray)
      {
        this.singletonValue = null;
        this.listValues = decodeListValues((JSONArray) valueJSON);
      }
    else
      {
        this.singletonValue = JSONUtilities.decodeString(jsonRoot, "value", false);
        this.listValues = null;
      }
  }
  
  /*****************************************
  *
  *  decodeListValues
  *
  *****************************************/

  private List<String> decodeListValues(JSONArray jsonArray)
  {
    List<String> result = new ArrayList<String>();
    if (jsonArray != null)
      {
        for (int i=0; i<jsonArray.size(); i++)
          {
            result.add((String) jsonArray.get(i));
          }
      }
    return result;
  }

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getCatalogCharacteristicID() { return catalogCharacteristicID; }
  public String getSingletonValue() { return singletonValue; }
  public List<String> getListValues() { return listValues; }
  public Object getValue() { return (singletonValue != null) ? singletonValue : listValues; }
  
  /*****************************************
  *
  *  serde
  *
  *****************************************/

  public static ConnectSerde<CatalogCharacteristicInstance> serde()
  {
    return new ConnectSerde<CatalogCharacteristicInstance>(schema, false, CatalogCharacteristicInstance.class, CatalogCharacteristicInstance::pack, CatalogCharacteristicInstance::unpack);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    CatalogCharacteristicInstance offerCatalogCharacteristic = (CatalogCharacteristicInstance) value;
    Struct struct = new Struct(schema);
    struct.put("catalogCharacteristicID", offerCatalogCharacteristic.getCatalogCharacteristicID());
    struct.put("singletonValue", offerCatalogCharacteristic.getSingletonValue());
    struct.put("listValues", offerCatalogCharacteristic.getListValues());
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static CatalogCharacteristicInstance unpack(SchemaAndValue schemaAndValue)
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
    String catalogCharacteristicID = valueStruct.getString("catalogCharacteristicID");
    String singletonValue = valueStruct.getString("singletonValue");
    List<String> listValues = (List<String>) valueStruct.get("listValues");

    //
    //  validate
    //

    // TBD

    //
    //  return
    //

    return new CatalogCharacteristicInstance(catalogCharacteristicID, singletonValue, listValues);
  }

  /*****************************************
  *
  *  equals
  *
  *****************************************/

  public boolean equals(Object obj)
  {
    boolean result = false;
    if (obj instanceof CatalogCharacteristicInstance)
      {
        CatalogCharacteristicInstance offerCatalogCharacteristic = (CatalogCharacteristicInstance) obj;
        result = true;
        result = result && Objects.equals(catalogCharacteristicID, offerCatalogCharacteristic.getCatalogCharacteristicID());
        result = result && Objects.equals(singletonValue, offerCatalogCharacteristic.getSingletonValue());
        result = result && Objects.equals(listValues, offerCatalogCharacteristic.getListValues());
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
    return catalogCharacteristicID.hashCode();
  }
}

