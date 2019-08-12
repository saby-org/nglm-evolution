/*****************************************************************************
*
*  OfferCharacteristics.java
*
*****************************************************************************/

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

public class OfferCharacteristics
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(OfferCharacteristics.class);

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
    schemaBuilder.name("offer_characteristics");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("languageProperties", SchemaBuilder.array(OfferCharacteristicsLanguageProperty.schema()).schema());
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

  private Set<OfferCharacteristicsLanguageProperty> properties;

  /*****************************************
  *
  *  constructor -- simple
  *
  *****************************************/

  private OfferCharacteristics(Set<OfferCharacteristicsLanguageProperty> properties)
  {
    this.properties = properties;
  }

  /*****************************************
  *
  *  constructor -- external JSON
  *
  *****************************************/

  OfferCharacteristics(JSONObject jsonRoot) throws GUIManagerException
  {
    /*****************************************
    *
    *  properties
    *
    *****************************************/

    properties = decodeOfferCharacteristicProperties(JSONUtilities.decodeJSONArray(jsonRoot, "languageProperties", false));
  }

  /*****************************************
  *
  *  decodeOfferCallingChannelProperties
  *
  *****************************************/

  private Set<OfferCharacteristicsLanguageProperty> decodeOfferCharacteristicProperties(JSONArray jsonArray) throws GUIManagerException
  {
    Set<OfferCharacteristicsLanguageProperty> result = new HashSet<OfferCharacteristicsLanguageProperty>();
    if (jsonArray != null)
      {
        for (int i=0; i<jsonArray.size(); i++)
          {
            result.add(new OfferCharacteristicsLanguageProperty((JSONObject) jsonArray.get(i)));
          }
      }
    return result;
  }

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public Set<OfferCharacteristicsLanguageProperty> getOfferCharacteristicProperties() { return properties; }

  /*****************************************
  *
  *  serde
  *
  *****************************************/

  public static ConnectSerde<OfferCharacteristics> serde()
  {
    return new ConnectSerde<OfferCharacteristics>(schema, false, OfferCharacteristics.class, OfferCharacteristics::pack, OfferCharacteristics::unpack);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    OfferCharacteristics offerCharacteristic = (OfferCharacteristics) value;
    Struct struct = new Struct(schema);
    struct.put("languageProperties", packOfferCharacteristicProperties(offerCharacteristic.getOfferCharacteristicProperties()));
    return struct;
  }

  /****************************************
  *
  *  packOfferCharacteristicProperties
  *
  ****************************************/

  private static List<Object> packOfferCharacteristicProperties(Set<OfferCharacteristicsLanguageProperty> properties)
  {
    List<Object> result = new ArrayList<Object>();
    for (OfferCharacteristicsLanguageProperty offerCharacteristicProperty : properties)
      {
        result.add(OfferCharacteristicsLanguageProperty.pack(offerCharacteristicProperty));
      }
    return result;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static OfferCharacteristics unpack(SchemaAndValue schemaAndValue)
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
    Set<OfferCharacteristicsLanguageProperty> properties = unpackOfferCharacteristicProperties(schema.field("languageProperties").schema(), valueStruct.get("languageProperties"));
    
    //
    //  return
    //

    return new OfferCharacteristics(properties);
  }

  /*****************************************
  *
  *  unpackOfferCharacteristicProperties
  *
  *****************************************/

  private static Set<OfferCharacteristicsLanguageProperty> unpackOfferCharacteristicProperties(Schema schema, Object value)
  {
    //
    //  get schema for OfferCharacteristicProperty
    //

    Schema propertySchema = schema.valueSchema();
    
    //
    //  unpack
    //

    Set<OfferCharacteristicsLanguageProperty> result = new HashSet<OfferCharacteristicsLanguageProperty>();
    List<Object> valueArray = (List<Object>) value;
    for (Object property : valueArray)
      {
        result.add(OfferCharacteristicsLanguageProperty.unpack(new SchemaAndValue(propertySchema, property)));
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
    if (obj instanceof OfferCharacteristics)
      {
        OfferCharacteristics offerCallingChannel = (OfferCharacteristics) obj;
        result = true;
        result = result && Objects.equals(properties, offerCallingChannel.getOfferCharacteristicProperties());
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
    return properties.hashCode();
  }

  /****************************************************************************
  *
  *  OfferCharacteristicsLanguageProperty
  *
  ****************************************************************************/

  public static class OfferCharacteristicsLanguageProperty
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
      schemaBuilder.name("offer_characteristics_language_property");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
      schemaBuilder.field("languageID", Schema.STRING_SCHEMA);
      schemaBuilder.field("properties", SchemaBuilder.array(OfferCharacteristicsProperty.schema()).schema());
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

    private String languageID;
    private Set<OfferCharacteristicsProperty> properties;

    /*****************************************
    *
    *  constructor -- simple
    *
    *****************************************/

    private OfferCharacteristicsLanguageProperty(String languageID, Set<OfferCharacteristicsProperty> properties)
    {
      this.languageID = languageID;
      this.properties = properties;
    }

    /*****************************************
    *
    *  constructor -- external JSON
    *
    *****************************************/

    OfferCharacteristicsLanguageProperty(JSONObject jsonRoot) throws GUIManagerException
    {
      //
      //  basic fields
      //

      this.languageID = JSONUtilities.decodeString(jsonRoot, "languageID", false);
      this.properties = decodeOfferCharacteristicsProperties(JSONUtilities.decodeJSONArray(jsonRoot, "properties", false));

      //
      //  validate 
      //

    }
    
    /*****************************************
    *
    *  decodeOfferCharacteristicsProperties
    *
    *****************************************/

    private Set<OfferCharacteristicsProperty> decodeOfferCharacteristicsProperties(JSONArray jsonArray) throws GUIManagerException
    {
      Set<OfferCharacteristicsProperty> result = new HashSet<OfferCharacteristicsProperty>();
      if (jsonArray != null)
        {
          for (int i=0; i<jsonArray.size(); i++)
            {
              result.add(new OfferCharacteristicsProperty((JSONObject) jsonArray.get(i)));
            }
        }
      return result;
    }

    /*****************************************
    *
    *  accessors
    *
    *****************************************/

    public String getLanguageID() { return languageID; }
    public Set<OfferCharacteristicsProperty> getProperties() { return properties; }

    /*****************************************
    *
    *  serde
    *
    *****************************************/

    public static ConnectSerde<OfferCharacteristicsLanguageProperty> serde()
    {
      return new ConnectSerde<OfferCharacteristicsLanguageProperty>(schema, false, OfferCharacteristicsLanguageProperty.class, OfferCharacteristicsLanguageProperty::pack, OfferCharacteristicsLanguageProperty::unpack);
    }

    /*****************************************
    *
    *  pack
    *
    *****************************************/

    public static Object pack(Object value)
    {
      OfferCharacteristicsLanguageProperty offerCharacteristicsLanguageProperty = (OfferCharacteristicsLanguageProperty) value;
      Struct struct = new Struct(schema);
      struct.put("languageID", offerCharacteristicsLanguageProperty.getLanguageID());
      struct.put("properties", packOfferCharacteristicsProperties(offerCharacteristicsLanguageProperty.getProperties()));
      return struct;
    }
    
    /****************************************
    *
    *  packOfferCharacteristicsProperties
    *
    ****************************************/

    private static List<Object> packOfferCharacteristicsProperties(Set<OfferCharacteristicsProperty> offerCharacteristicsProperties)
    {
      List<Object> result = new ArrayList<Object>();
      for (OfferCharacteristicsProperty offerCharacteristicsProperty : offerCharacteristicsProperties)
        {
          result.add(OfferCharacteristicsProperty.pack(offerCharacteristicsProperty));
        }
      return result;
    }
    
    /*****************************************
    *
    *  unpack
    *
    *****************************************/

    public static OfferCharacteristicsLanguageProperty unpack(SchemaAndValue schemaAndValue)
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
      String languageID = valueStruct.getString("languageID");
      Set<OfferCharacteristicsProperty> properties = unpackOfferCharacteristicsProperties(schema.field("properties").schema(), valueStruct.get("properties"));
      
      //
      //  validate
      //

      //
      //  return
      //

      return new OfferCharacteristicsLanguageProperty(languageID, properties);
    }

    /*****************************************
    *
    *  unpackOfferObjectives
    *
    *****************************************/

    private static Set<OfferCharacteristicsProperty> unpackOfferCharacteristicsProperties(Schema schema, Object value)
    {
      //
      //  get schema for OfferObjective
      //

      Schema offerCharacteristicsPropertySchema = schema.valueSchema();

      //
      //  unpack
      //

      Set<OfferCharacteristicsProperty> result = new HashSet<OfferCharacteristicsProperty>();
      List<Object> valueArray = (List<Object>) value;
      for (Object offerCharacteristicsProperty : valueArray)
        {
          result.add(OfferCharacteristicsProperty.unpack(new SchemaAndValue(offerCharacteristicsPropertySchema, offerCharacteristicsProperty)));
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
      if (obj instanceof OfferCharacteristicsLanguageProperty)
        {
          OfferCharacteristicsLanguageProperty offerCallingChannelProperty = (OfferCharacteristicsLanguageProperty) obj;
          result = true;
          result = result && Objects.equals(languageID, offerCallingChannelProperty.getLanguageID());
          result = result && Objects.equals(properties, offerCallingChannelProperty.getProperties());
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
      return properties.hashCode();
    }
  }


  /****************************************************************************
  *
  *  OfferCharacteristicsProperty
  *
  ****************************************************************************/

  public static class OfferCharacteristicsProperty
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
      schemaBuilder.name("offer_characteristics_property");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
      schemaBuilder.field("catalogCharacteristicID", Schema.STRING_SCHEMA);
      schemaBuilder.field("catalogCharacteristicName", Schema.STRING_SCHEMA);
      schemaBuilder.field("value", Schema.STRING_SCHEMA);
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
    private String catalogCharacteristicName;
    private String value;

    /*****************************************
    *
    *  constructor -- simple
    *
    *****************************************/

    private OfferCharacteristicsProperty(String catalogCharacteristicID, String catalogCharacteristicName, String value)
    {
      this.catalogCharacteristicID = catalogCharacteristicID;
      this.catalogCharacteristicName = catalogCharacteristicName;
      this.value = value;
    }

    /*****************************************
    *
    *  constructor -- external JSON
    *
    *****************************************/

    OfferCharacteristicsProperty(JSONObject jsonRoot) throws GUIManagerException
    {
      //
      //  basic fields
      //

      this.catalogCharacteristicID = JSONUtilities.decodeString(jsonRoot, "catalogCharacteristicID", false);
      this.catalogCharacteristicName = JSONUtilities.decodeString(jsonRoot, "catalogCharacteristicName", false);
      this.value = JSONUtilities.decodeString(jsonRoot, "value", false);

      //
      //  validate 
      //
    }
    
    /*****************************************
    *
    *  accessors
    *
    *****************************************/

    public String getCatalogCharacteristicID() { return catalogCharacteristicID; }
    public String getCatalogCharacteristicName() { return catalogCharacteristicName; }
    public String getValue() { return value; }

    /*****************************************
    *
    *  serde
    *
    *****************************************/

    public static ConnectSerde<OfferCharacteristicsProperty> serde()
    {
      return new ConnectSerde<OfferCharacteristicsProperty>(schema, false, OfferCharacteristicsProperty.class, OfferCharacteristicsProperty::pack, OfferCharacteristicsProperty::unpack);
    }

    /*****************************************
    *
    *  pack
    *
    *****************************************/

    public static Object pack(Object value)
    {
      OfferCharacteristicsProperty offerCharacteristicsProperty = (OfferCharacteristicsProperty) value;
      Struct struct = new Struct(schema);
      struct.put("catalogCharacteristicID", offerCharacteristicsProperty.getCatalogCharacteristicID());
      struct.put("catalogCharacteristicName", offerCharacteristicsProperty.getCatalogCharacteristicName());
      struct.put("value", offerCharacteristicsProperty.getValue());
      return struct;
    }

    /*****************************************
    *
    *  unpack
    *
    *****************************************/

    public static OfferCharacteristicsProperty unpack(SchemaAndValue schemaAndValue)
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
      String catalogCharacteristicName = valueStruct.getString("catalogCharacteristicName");
      String value2 = valueStruct.getString("value");

      //
      //  validate
      //

      //
      //  return
      //

      return new OfferCharacteristicsProperty(catalogCharacteristicID, catalogCharacteristicName, value2);
    }

    /*****************************************
    *
    *  equals
    *
    *****************************************/

    public boolean equals(Object obj)
    {
      boolean result = false;
      if (obj instanceof OfferCharacteristicsProperty)
        {
          OfferCharacteristicsProperty offerCharacteristicsProperty = (OfferCharacteristicsProperty) obj;
          result = true;
          result = result && Objects.equals(catalogCharacteristicID, offerCharacteristicsProperty.getCatalogCharacteristicID());
          result = result && Objects.equals(catalogCharacteristicName, offerCharacteristicsProperty.getCatalogCharacteristicName());
          result = result && Objects.equals(value, offerCharacteristicsProperty.getValue());
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
      return value.hashCode();
    }
  }

  
}
