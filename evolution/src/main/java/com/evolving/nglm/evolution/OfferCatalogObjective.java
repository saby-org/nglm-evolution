/*****************************************************************************
*
*  OfferCatalogObjective.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.ServerRuntimeException;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rii.utilities.JSONUtilities;
import com.rii.utilities.JSONUtilities.JSONUtilitiesException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import com.rii.utilities.SystemTime;

import java.nio.charset.StandardCharsets;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class OfferCatalogObjective
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(OfferCatalogObjective.class);

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
    schemaBuilder.name("offer_catalog_objective");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("catalogObjectiveID", Schema.STRING_SCHEMA);
    schemaBuilder.field("offerCatalogCharacteristics", SchemaBuilder.array(OfferCatalogCharacteristic.schema()).schema());
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

  private String catalogObjectiveID;
  private Set<OfferCatalogCharacteristic> offerCatalogCharacteristics;

  /*****************************************
  *
  *  constructor -- simple
  *
  *****************************************/

  private OfferCatalogObjective(String catalogObjectiveID, Set<OfferCatalogCharacteristic> offerCatalogCharacteristics)
  {
    this.catalogObjectiveID = catalogObjectiveID;
    this.offerCatalogCharacteristics = offerCatalogCharacteristics;
  }

  /*****************************************
  *
  *  constructor -- external JSON
  *
  *****************************************/

  OfferCatalogObjective(JSONObject jsonRoot) throws GUIManagerException
  {
    this.catalogObjectiveID = JSONUtilities.decodeString(jsonRoot, "catalogObjectiveID", true);
    this.offerCatalogCharacteristics = decodeOfferCatalogCharacteristics(JSONUtilities.decodeJSONArray(jsonRoot, "catalogCharacteristics", false));
  }

  /*****************************************
  *
  *  decodeOfferCatalogCharacteristics
  *
  *****************************************/

  private Set<OfferCatalogCharacteristic> decodeOfferCatalogCharacteristics(JSONArray jsonArray) throws GUIManagerException
  {
    Set<OfferCatalogCharacteristic> result = new HashSet<OfferCatalogCharacteristic>();
    if (jsonArray != null)
      {
        for (int i=0; i<jsonArray.size(); i++)
          {
            result.add(new OfferCatalogCharacteristic((JSONObject) jsonArray.get(i)));
          }
      }
    return result;
  }

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getCatalogObjectiveID() { return catalogObjectiveID; }
  public Set<OfferCatalogCharacteristic> getOfferCatalogCharacteristics() { return offerCatalogCharacteristics; }

  /*****************************************
  *
  *  serde
  *
  *****************************************/

  public static ConnectSerde<OfferCatalogObjective> serde()
  {
    return new ConnectSerde<OfferCatalogObjective>(schema, false, OfferCatalogObjective.class, OfferCatalogObjective::pack, OfferCatalogObjective::unpack);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    OfferCatalogObjective offerCatalogObjective = (OfferCatalogObjective) value;
    Struct struct = new Struct(schema);
    struct.put("catalogObjectiveID", offerCatalogObjective.getCatalogObjectiveID());
    struct.put("offerCatalogCharacteristics", packOfferCatalogCharacteristics(offerCatalogObjective.getOfferCatalogCharacteristics()));
    return struct;
  }

  /****************************************
  *
  *  packOfferCatalogCharacteristics
  *
  ****************************************/

  private static List<Object> packOfferCatalogCharacteristics(Set<OfferCatalogCharacteristic> offerCatalogCharacteristics)
  {
    List<Object> result = new ArrayList<Object>();
    for (OfferCatalogCharacteristic offerCatalogCharacteristic : offerCatalogCharacteristics)
      {
        result.add(OfferCatalogCharacteristic.pack(offerCatalogCharacteristic));
      }
    return result;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static OfferCatalogObjective unpack(SchemaAndValue schemaAndValue)
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
    String catalogObjectiveID = valueStruct.getString("catalogObjectiveID");
    Set<OfferCatalogCharacteristic> offerCatalogCharacteristics = unpackOfferCatalogCharacteristics(schema.field("offerCatalogCharacteristics").schema(), valueStruct.get("offerCatalogCharacteristics"));
    
    //
    //  return
    //

    return new OfferCatalogObjective(catalogObjectiveID, offerCatalogCharacteristics);
  }

  /*****************************************
  *
  *  unpackOfferCatalogCharacteristics
  *
  *****************************************/

  private static Set<OfferCatalogCharacteristic> unpackOfferCatalogCharacteristics(Schema schema, Object value)
  {
    //
    //  get schema for OfferCatalogCharacteristic
    //

    Schema propertySchema = schema.valueSchema();
    
    //
    //  unpack
    //

    Set<OfferCatalogCharacteristic> result = new HashSet<OfferCatalogCharacteristic>();
    List<Object> valueArray = (List<Object>) value;
    for (Object property : valueArray)
      {
        result.add(OfferCatalogCharacteristic.unpack(new SchemaAndValue(propertySchema, property)));
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
    if (obj instanceof OfferCatalogObjective)
      {
        OfferCatalogObjective offerCatalogObjective = (OfferCatalogObjective) obj;
        result = true;
        result = result && Objects.equals(catalogObjectiveID, offerCatalogObjective.getCatalogObjectiveID());
        result = result && Objects.equals(offerCatalogCharacteristics, offerCatalogObjective.getOfferCatalogCharacteristics());
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
    return catalogObjectiveID.hashCode();
  }

  /****************************************************************************
  *
  *  OfferCatalogCharacteristic
  *
  ****************************************************************************/

  public static class OfferCatalogCharacteristic
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

    private OfferCatalogCharacteristic(String catalogCharacteristicID, String singletonValue, List<String> listValues)
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

    OfferCatalogCharacteristic(JSONObject jsonRoot) throws GUIManagerException
    {
      //
      //  catalog characteristic
      //

      this.catalogCharacteristicID = JSONUtilities.decodeString(jsonRoot, "catalogCharacteristicID", true);
      Object valueJSON = JSONUtilities.decodeString(jsonRoot, "value", false);
      if (valueJSON instanceof JSONArray)
        {
          this.singletonValue = null;
          this.listValues = decodeListValues((JSONArray) valueJSON);
        }
      else
        {
          this.singletonValue = (String) valueJSON;
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

    public static ConnectSerde<OfferCatalogCharacteristic> serde()
    {
      return new ConnectSerde<OfferCatalogCharacteristic>(schema, false, OfferCatalogCharacteristic.class, OfferCatalogCharacteristic::pack, OfferCatalogCharacteristic::unpack);
    }

    /*****************************************
    *
    *  pack
    *
    *****************************************/

    public static Object pack(Object value)
    {
      OfferCatalogCharacteristic offerCatalogCharacteristic = (OfferCatalogCharacteristic) value;
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

    public static OfferCatalogCharacteristic unpack(SchemaAndValue schemaAndValue)
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

      return new OfferCatalogCharacteristic(catalogCharacteristicID, singletonValue, listValues);
    }

    /*****************************************
    *
    *  equals
    *
    *****************************************/

    public boolean equals(Object obj)
    {
      boolean result = false;
      if (obj instanceof OfferCatalogCharacteristic)
        {
          OfferCatalogCharacteristic offerCatalogCharacteristic = (OfferCatalogCharacteristic) obj;
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
}
