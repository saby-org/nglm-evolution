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
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionDataType;
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

  public OfferCharacteristics(Set<OfferCharacteristicsLanguageProperty> properties)
  {
    this.properties = properties;
  }

  /*****************************************
  *
  *  constructor -- external JSON
  *
  *****************************************/

  OfferCharacteristics(JSONObject jsonRoot, CatalogCharacteristicService catalogCharacteristicService) throws GUIManagerException
  {
    properties = (jsonRoot != null) ? decodeOfferCharacteristicProperties(JSONUtilities.decodeJSONArray(jsonRoot, "languageProperties", false), catalogCharacteristicService) : new HashSet<OfferCharacteristicsLanguageProperty>();
  }

  /*****************************************
  *
  *  decodeOfferCallingChannelProperties
  *
  *****************************************/

  private Set<OfferCharacteristicsLanguageProperty> decodeOfferCharacteristicProperties(JSONArray jsonArray, CatalogCharacteristicService catalogCharacteristicService) throws GUIManagerException
  {
    Set<OfferCharacteristicsLanguageProperty> result = new HashSet<OfferCharacteristicsLanguageProperty>();
    if (jsonArray != null)
      {
        for (int i=0; i<jsonArray.size(); i++)
          {
            result.add(new OfferCharacteristicsLanguageProperty((JSONObject) jsonArray.get(i), catalogCharacteristicService));
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
  
  /*****************************************
  *
  *  toJSONObject
  *
  *****************************************/
  
  public JSONObject toJSONObject()
  {
    JSONObject result = new JSONObject();
    JSONArray array = new JSONArray();
    if(this.properties != null)
      {
        for(OfferCharacteristicsLanguageProperty prop : this.properties)
          {
            array.add(prop.toJSONObject());
          }
      }
    result.put("languageProperties", array);
    return result;
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

    public OfferCharacteristicsLanguageProperty(String languageID, Set<OfferCharacteristicsProperty> properties)
    {
      this.languageID = languageID;
      this.properties = properties;
    }

    /*****************************************
    *
    *  constructor -- external JSON
    *
    *****************************************/

    OfferCharacteristicsLanguageProperty(JSONObject jsonRoot, CatalogCharacteristicService catalogCharacteristicService) throws GUIManagerException
    {
      //
      //  basic fields
      //

      this.languageID = JSONUtilities.decodeString(jsonRoot, "languageID", false);
      this.properties = decodeOfferCharacteristicsProperties(JSONUtilities.decodeJSONArray(jsonRoot, "properties", false), catalogCharacteristicService);

      //
      //  validate 
      //

    }
    
    /*****************************************
    *
    *  decodeOfferCharacteristicsProperties
    *
    *****************************************/

    private Set<OfferCharacteristicsProperty> decodeOfferCharacteristicsProperties(JSONArray jsonArray, CatalogCharacteristicService catalogCharacteristicService) throws GUIManagerException
    {
      Set<OfferCharacteristicsProperty> result = new HashSet<OfferCharacteristicsProperty>();
      if (jsonArray != null)
        {
          for (int i=0; i<jsonArray.size(); i++)
            {
              result.add(new OfferCharacteristicsProperty((JSONObject) jsonArray.get(i), catalogCharacteristicService));
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
    
    /*****************************************
    *
    *  toJSONObject
    *
    *****************************************/
    
    public JSONObject toJSONObject()
    {
      JSONObject result = new JSONObject();
      JSONArray array = new JSONArray();
      if(this.properties != null)
        {
          for(OfferCharacteristicsProperty prop : this.properties)
            {
              array.add(prop.toJSONObject());
            }
        }
      result.put("languageID", this.languageID);
      result.put("properties", array);
      return result;
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
      schemaBuilder.field("value", ParameterMap.schema());
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
    private ParameterMap value;

    /*****************************************
    *
    *  constructor -- simple
    *
    *****************************************/

    public OfferCharacteristicsProperty(String catalogCharacteristicID, String catalogCharacteristicName, ParameterMap value)
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

    OfferCharacteristicsProperty(JSONObject jsonRoot, CatalogCharacteristicService catalogCharacteristicService) throws GUIManagerException
    {
      //
      //  basic fields
      //

      this.catalogCharacteristicID = JSONUtilities.decodeString(jsonRoot, "catalogCharacteristicID", false);
      this.catalogCharacteristicName = JSONUtilities.decodeString(jsonRoot, "catalogCharacteristicName", false);
      CatalogCharacteristic catalogCharacteristic = catalogCharacteristicService.getActiveCatalogCharacteristic(catalogCharacteristicID, SystemTime.getCurrentTime());
      CriterionDataType dataType = (catalogCharacteristic != null) ? catalogCharacteristic.getDataType() : CriterionDataType.Unknown;

      //
      //  parse value
      //

      Object value = null;
      switch (dataType)
        {
          case IntegerCriterion:
            value = JSONUtilities.decodeInteger(jsonRoot, "value", false);
            break;

          case DoubleCriterion:
            value = JSONUtilities.decodeDouble(jsonRoot, "value", false);
            break;

          case StringCriterion:
            value = JSONUtilities.decodeString(jsonRoot, "value", false);
            break;

          case DateCriterion:
            value = GUIManagedObject.parseDateField(JSONUtilities.decodeString(jsonRoot, "value", false));
            break;

          case BooleanCriterion:
            value = JSONUtilities.decodeBoolean(jsonRoot, "value", false);
            break;

          case StringSetCriterion:
            JSONArray jsonArrayString = JSONUtilities.decodeJSONArray(jsonRoot, "value", false);
            Set<Object> stringSetValue = new HashSet<Object>();
            for (int i=0; i<jsonArrayString.size(); i++)
              {
                stringSetValue.add(jsonArrayString.get(i));
              }
            value = stringSetValue;
            break;

          case IntegerSetCriterion:
            JSONArray jsonArrayInteger = JSONUtilities.decodeJSONArray(jsonRoot, "value", false);
            Set<Object> integerSetValue = new HashSet<Object>();
            for (int i=0; i<jsonArrayInteger.size(); i++)
              {
                integerSetValue.add(new Integer(((Number) jsonArrayInteger.get(i)).intValue()));
              }
            value = integerSetValue;
            break;

          case AniversaryCriterion:
          case TimeCriterion:
          default:
            throw new GUIManagerException("unsupported catalogCharacteristic", catalogCharacteristicID);
        }

      //
      //  store in singleton parameterMap
      //

      this.value = new ParameterMap();
      this.value.put("value", value);
    }
    
    /*****************************************
    *
    *  accessors
    *
    *****************************************/

    public String getCatalogCharacteristicID() { return catalogCharacteristicID; }
    public String getCatalogCharacteristicName() { return catalogCharacteristicName; }
    public Object getValue() { return value.get("value"); }
    private ParameterMap getParameterMap() { return value; }

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
      struct.put("value", ParameterMap.pack(offerCharacteristicsProperty.getParameterMap()));
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
      ParameterMap parameterMap = ParameterMap.unpack(new SchemaAndValue(schema.field("value").schema(), valueStruct.get("value")));

      //
      //  validate
      //

      //
      //  return
      //

      return new OfferCharacteristicsProperty(catalogCharacteristicID, catalogCharacteristicName, parameterMap);
    }

    /*****************************************
    *
    *  equalsNonRobustly
    *
    *****************************************/

    public boolean equalsNonRobustly(Object obj)
    {
      boolean result = false;
      if (obj instanceof OfferCharacteristicsProperty)
        {
          OfferCharacteristicsProperty offerCharacteristicsProperty = (OfferCharacteristicsProperty) obj;
          log.info("RAJ K equalsNonRobustly matching {} with param val {}", this.toJSONObject(), offerCharacteristicsProperty.toJSONObject());
          result = true;
          result = result && Objects.equals(catalogCharacteristicID, offerCharacteristicsProperty.getCatalogCharacteristicID());
          log.info("RAJ K equalsNonRobustly ID matched {}", result);
          if (result && getValue() instanceof Set)
            {
              Set<Object> thisValue = (Set<Object>) getValue();
              Set<Object> reqValue = (Set<Object>) offerCharacteristicsProperty.getValue();
              result = result && thisValue.stream().filter(reqValue::contains).count() > 0L;
              log.info("RAJ K equalsNonRobustly SET matched {}", result);
            }
          else if (result)
            {
              Set<Object> reqValue = (Set<Object>) offerCharacteristicsProperty.getValue();
              result = result && reqValue.contains(getValue());
              log.info("RAJ K equalsNonRobustly normal result is {}", result);
            
            }
        }
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
      if (obj instanceof OfferCharacteristicsProperty)
        {
          OfferCharacteristicsProperty offerCharacteristicsProperty = (OfferCharacteristicsProperty) obj;
          result = true;
          result = result && Objects.equals(catalogCharacteristicID, offerCharacteristicsProperty.getCatalogCharacteristicID());
          result = result && Objects.equals(catalogCharacteristicName, offerCharacteristicsProperty.getCatalogCharacteristicName());
          result = result && Objects.equals(value, offerCharacteristicsProperty.getParameterMap());
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
    
    /*****************************************
    *
    *  toJSONObject
    *
    *****************************************/
    
    public JSONObject toJSONObject()
    {
      JSONObject result = new JSONObject();
      result.put("catalogCharacteristicID", this.catalogCharacteristicID);
      result.put("catalogCharacteristicName", this.catalogCharacteristicName);
      return result;
    }
  }
}
