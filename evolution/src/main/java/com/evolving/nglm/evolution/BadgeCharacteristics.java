/*****************************************************************************
*
*  BadgeCharacteristics.java
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
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionDataType;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class BadgeCharacteristics
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(BadgeCharacteristics.class);

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
    schemaBuilder.name("badge_characteristics");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("languageProperties", SchemaBuilder.array(BadgeCharacteristicsLanguageProperty.schema()).schema());
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

  private Set<BadgeCharacteristicsLanguageProperty> properties;

  /*****************************************
  *
  *  constructor -- simple
  *
  *****************************************/

  public BadgeCharacteristics(Set<BadgeCharacteristicsLanguageProperty> properties)
  {
    this.properties = properties;
  }

  /*****************************************
  *
  *  constructor -- external JSON
  *
  *****************************************/

  BadgeCharacteristics(JSONObject jsonRoot, CatalogCharacteristicService catalogCharacteristicService) throws GUIManagerException
  {
    properties = (jsonRoot != null) ? decodeBadgeCharacteristicProperties(JSONUtilities.decodeJSONArray(jsonRoot, "languageProperties", false), catalogCharacteristicService) : new HashSet<BadgeCharacteristicsLanguageProperty>();
  }

  /*****************************************
  *
  *  decodeBadgeCallingChannelProperties
  *
  *****************************************/

  private Set<BadgeCharacteristicsLanguageProperty> decodeBadgeCharacteristicProperties(JSONArray jsonArray, CatalogCharacteristicService catalogCharacteristicService) throws GUIManagerException
  {
    Set<BadgeCharacteristicsLanguageProperty> result = new HashSet<BadgeCharacteristicsLanguageProperty>();
    if (jsonArray != null)
      {
        for (int i=0; i<jsonArray.size(); i++)
          {
            result.add(new BadgeCharacteristicsLanguageProperty((JSONObject) jsonArray.get(i), catalogCharacteristicService));
          }
      }
    return result;
  }

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public Set<BadgeCharacteristicsLanguageProperty> getBadgeCharacteristicProperties() { return properties; }

  /*****************************************
  *
  *  serde
  *
  *****************************************/

  public static ConnectSerde<BadgeCharacteristics> serde()
  {
    return new ConnectSerde<BadgeCharacteristics>(schema, false, BadgeCharacteristics.class, BadgeCharacteristics::pack, BadgeCharacteristics::unpack);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    BadgeCharacteristics badgeCharacteristic = (BadgeCharacteristics) value;
    Struct struct = new Struct(schema);
    struct.put("languageProperties", packBadgeCharacteristicProperties(badgeCharacteristic.getBadgeCharacteristicProperties()));
    return struct;
  }

  /****************************************
  *
  *  packBadgeCharacteristicProperties
  *
  ****************************************/

  private static List<Object> packBadgeCharacteristicProperties(Set<BadgeCharacteristicsLanguageProperty> properties)
  {
    List<Object> result = new ArrayList<Object>();
    for (BadgeCharacteristicsLanguageProperty badgeCharacteristicProperty : properties)
      {
        result.add(BadgeCharacteristicsLanguageProperty.pack(badgeCharacteristicProperty));
      }
    return result;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static BadgeCharacteristics unpack(SchemaAndValue schemaAndValue)
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
    Set<BadgeCharacteristicsLanguageProperty> properties = unpackBadgeCharacteristicProperties(schema.field("languageProperties").schema(), valueStruct.get("languageProperties"));
    
    //
    //  return
    //

    return new BadgeCharacteristics(properties);
  }

  /*****************************************
  *
  *  unpackBadgeCharacteristicProperties
  *
  *****************************************/

  private static Set<BadgeCharacteristicsLanguageProperty> unpackBadgeCharacteristicProperties(Schema schema, Object value)
  {
    //
    //  get schema for BadgeCharacteristicProperty
    //

    Schema propertySchema = schema.valueSchema();
    
    //
    //  unpack
    //

    Set<BadgeCharacteristicsLanguageProperty> result = new HashSet<BadgeCharacteristicsLanguageProperty>();
    List<Object> valueArray = (List<Object>) value;
    for (Object property : valueArray)
      {
        result.add(BadgeCharacteristicsLanguageProperty.unpack(new SchemaAndValue(propertySchema, property)));
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
    if (obj instanceof BadgeCharacteristics)
      {
        BadgeCharacteristics badgeCallingChannel = (BadgeCharacteristics) obj;
        result = true;
        result = result && Objects.equals(properties, badgeCallingChannel.getBadgeCharacteristicProperties());
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
        for(BadgeCharacteristicsLanguageProperty prop : this.properties)
          {
            array.add(prop.toJSONObject());
          }
      }
    result.put("languageProperties", array);
    return result;
  }
  

  /****************************************************************************
  *
  *  BadgeCharacteristicsLanguageProperty
  *
  ****************************************************************************/

  public static class BadgeCharacteristicsLanguageProperty
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
      schemaBuilder.name("badge_characteristics_language_property");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
      schemaBuilder.field("languageID", Schema.STRING_SCHEMA);
      schemaBuilder.field("properties", SchemaBuilder.array(BadgeCharacteristicsProperty.schema()).schema());
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
    private Set<BadgeCharacteristicsProperty> properties;

    /*****************************************
    *
    *  constructor -- simple
    *
    *****************************************/

    public BadgeCharacteristicsLanguageProperty(String languageID, Set<BadgeCharacteristicsProperty> properties)
    {
      this.languageID = languageID;
      this.properties = properties;
    }

    /*****************************************
    *
    *  constructor -- external JSON
    *
    *****************************************/

    BadgeCharacteristicsLanguageProperty(JSONObject jsonRoot, CatalogCharacteristicService catalogCharacteristicService) throws GUIManagerException
    {
      //
      //  basic fields
      //

      this.languageID = JSONUtilities.decodeString(jsonRoot, "languageID", false);
      this.properties = decodeBadgeCharacteristicsProperties(JSONUtilities.decodeJSONArray(jsonRoot, "properties", false), catalogCharacteristicService);

      //
      //  validate 
      //

    }
    
    /*****************************************
    *
    *  decodeBadgeCharacteristicsProperties
    *
    *****************************************/

    private Set<BadgeCharacteristicsProperty> decodeBadgeCharacteristicsProperties(JSONArray jsonArray, CatalogCharacteristicService catalogCharacteristicService) throws GUIManagerException
    {
      Set<BadgeCharacteristicsProperty> result = new HashSet<BadgeCharacteristicsProperty>();
      if (jsonArray != null)
        {
          for (int i=0; i<jsonArray.size(); i++)
            {
              result.add(new BadgeCharacteristicsProperty((JSONObject) jsonArray.get(i), catalogCharacteristicService));
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
    public Set<BadgeCharacteristicsProperty> getProperties() { return properties; }

    /*****************************************
    *
    *  serde
    *
    *****************************************/

    public static ConnectSerde<BadgeCharacteristicsLanguageProperty> serde()
    {
      return new ConnectSerde<BadgeCharacteristicsLanguageProperty>(schema, false, BadgeCharacteristicsLanguageProperty.class, BadgeCharacteristicsLanguageProperty::pack, BadgeCharacteristicsLanguageProperty::unpack);
    }

    /*****************************************
    *
    *  pack
    *
    *****************************************/

    public static Object pack(Object value)
    {
      BadgeCharacteristicsLanguageProperty badgeCharacteristicsLanguageProperty = (BadgeCharacteristicsLanguageProperty) value;
      Struct struct = new Struct(schema);
      struct.put("languageID", badgeCharacteristicsLanguageProperty.getLanguageID());
      struct.put("properties", packBadgeCharacteristicsProperties(badgeCharacteristicsLanguageProperty.getProperties()));
      return struct;
    }
    
    /****************************************
    *
    *  packBadgeCharacteristicsProperties
    *
    ****************************************/

    private static List<Object> packBadgeCharacteristicsProperties(Set<BadgeCharacteristicsProperty> badgeCharacteristicsProperties)
    {
      List<Object> result = new ArrayList<Object>();
      for (BadgeCharacteristicsProperty badgeCharacteristicsProperty : badgeCharacteristicsProperties)
        {
          result.add(BadgeCharacteristicsProperty.pack(badgeCharacteristicsProperty));
        }
      return result;
    }
    
    /*****************************************
    *
    *  unpack
    *
    *****************************************/

    public static BadgeCharacteristicsLanguageProperty unpack(SchemaAndValue schemaAndValue)
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
      Set<BadgeCharacteristicsProperty> properties = unpackBadgeCharacteristicsProperties(schema.field("properties").schema(), valueStruct.get("properties"));
      
      //
      //  validate
      //

      //
      //  return
      //

      return new BadgeCharacteristicsLanguageProperty(languageID, properties);
    }

    /*****************************************
    *
    *  unpackBadgeObjectives
    *
    *****************************************/

    private static Set<BadgeCharacteristicsProperty> unpackBadgeCharacteristicsProperties(Schema schema, Object value)
    {
      //
      //  get schema for BadgeObjective
      //

      Schema badgeCharacteristicsPropertySchema = schema.valueSchema();

      //
      //  unpack
      //

      Set<BadgeCharacteristicsProperty> result = new HashSet<BadgeCharacteristicsProperty>();
      List<Object> valueArray = (List<Object>) value;
      for (Object badgeCharacteristicsProperty : valueArray)
        {
          result.add(BadgeCharacteristicsProperty.unpack(new SchemaAndValue(badgeCharacteristicsPropertySchema, badgeCharacteristicsProperty)));
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
      if (obj instanceof BadgeCharacteristicsLanguageProperty)
        {
          BadgeCharacteristicsLanguageProperty badgeCallingChannelProperty = (BadgeCharacteristicsLanguageProperty) obj;
          result = true;
          result = result && Objects.equals(languageID, badgeCallingChannelProperty.getLanguageID());
          result = result && Objects.equals(properties, badgeCallingChannelProperty.getProperties());
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
          for(BadgeCharacteristicsProperty prop : this.properties)
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
  *  BadgeCharacteristicsProperty
  *
  ****************************************************************************/

  public static class BadgeCharacteristicsProperty
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
      schemaBuilder.name("badge_characteristics_property");
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

    public BadgeCharacteristicsProperty(String catalogCharacteristicID, String catalogCharacteristicName, ParameterMap value)
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

    BadgeCharacteristicsProperty(JSONObject jsonRoot, CatalogCharacteristicService catalogCharacteristicService) throws GUIManagerException
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

    public static ConnectSerde<BadgeCharacteristicsProperty> serde()
    {
      return new ConnectSerde<BadgeCharacteristicsProperty>(schema, false, BadgeCharacteristicsProperty.class, BadgeCharacteristicsProperty::pack, BadgeCharacteristicsProperty::unpack);
    }

    /*****************************************
    *
    *  pack
    *
    *****************************************/

    public static Object pack(Object value)
    {
      BadgeCharacteristicsProperty badgeCharacteristicsProperty = (BadgeCharacteristicsProperty) value;
      Struct struct = new Struct(schema);
      struct.put("catalogCharacteristicID", badgeCharacteristicsProperty.getCatalogCharacteristicID());
      struct.put("catalogCharacteristicName", badgeCharacteristicsProperty.getCatalogCharacteristicName());
      struct.put("value", ParameterMap.pack(badgeCharacteristicsProperty.getParameterMap()));
      return struct;
    }

    /*****************************************
    *
    *  unpack
    *
    *****************************************/

    public static BadgeCharacteristicsProperty unpack(SchemaAndValue schemaAndValue)
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

      return new BadgeCharacteristicsProperty(catalogCharacteristicID, catalogCharacteristicName, parameterMap);
    }

    /*****************************************
    *
    *  equalsNonRobustly
    *
    *****************************************/

    public boolean equalsNonRobustly(Object obj)
    {
      boolean result = false;
      if (obj instanceof BadgeCharacteristicsProperty)
        {
          BadgeCharacteristicsProperty badgeCharacteristicsProperty = (BadgeCharacteristicsProperty) obj;
          result = true;
          result = result && Objects.equals(catalogCharacteristicID, badgeCharacteristicsProperty.getCatalogCharacteristicID());
          if (result && getValue() instanceof Set)
            {
              Set<Object> thisValue = (Set<Object>) getValue();
              Set<Object> reqValue = (Set<Object>) badgeCharacteristicsProperty.getValue();
              result = result && thisValue.stream().filter(reqValue::contains).count() > 0L;
            }
          else if (result)
            {
              Set<Object> reqValue = (Set<Object>) badgeCharacteristicsProperty.getValue();
              result = result && reqValue.contains(getValue());
            
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
      if (obj instanceof BadgeCharacteristicsProperty)
        {
          BadgeCharacteristicsProperty badgeCharacteristicsProperty = (BadgeCharacteristicsProperty) obj;
          result = true;
          result = result && Objects.equals(catalogCharacteristicID, badgeCharacteristicsProperty.getCatalogCharacteristicID());
          result = result && Objects.equals(catalogCharacteristicName, badgeCharacteristicsProperty.getCatalogCharacteristicName());
          result = result && Objects.equals(value, badgeCharacteristicsProperty.getParameterMap());
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
