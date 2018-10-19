/*****************************************************************************
*
*  OfferCallingChannel.java
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

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import com.evolving.nglm.core.SystemTime;

import java.nio.charset.StandardCharsets;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class OfferCallingChannel
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(OfferCallingChannel.class);

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
    schemaBuilder.name("offer_calling_channel");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("callingChannelID", Schema.STRING_SCHEMA);
    schemaBuilder.field("properties", SchemaBuilder.array(OfferCallingChannelProperty.schema()).schema());
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

  private String callingChannelID;
  private Set<OfferCallingChannelProperty> properties;

  /*****************************************
  *
  *  constructor -- simple
  *
  *****************************************/

  private OfferCallingChannel(String callingChannelID, Set<OfferCallingChannelProperty> properties)
  {
    this.callingChannelID = callingChannelID;
    this.properties = properties;
  }

  /*****************************************
  *
  *  constructor -- external JSON
  *
  *****************************************/

  OfferCallingChannel(JSONObject jsonRoot) throws GUIManagerException
  {
    /*****************************************
    *
    *  channel
    *
    *****************************************/

    this.callingChannelID = JSONUtilities.decodeString(jsonRoot, "callingChannelID", true);

    /*****************************************
    *
    *  properties
    *
    *****************************************/

    //
    //  unprocessed properties
    //

    Set<OfferCallingChannelProperty> commonProperties = decodeOfferCallingChannelCommonProperties(JSONUtilities.decodeJSONArray(jsonRoot, "commonProperties", false));
    Map<String,Set<OfferCallingChannelProperty>> unprocessedLanguageProperties = decodeOfferCallingChannelLanguageProperties(JSONUtilities.decodeJSONArray(jsonRoot, "languageProperties", false));

    //
    //  identify all languageProperties
    //

    Map<CallingChannelProperty,OfferCallingChannelProperty> languageProperties = new HashMap<CallingChannelProperty,OfferCallingChannelProperty>();
    for (Set<OfferCallingChannelProperty> offerCallingChannelPropertiesForLanguage : unprocessedLanguageProperties.values())
      {
        for (OfferCallingChannelProperty offerCallingChannelProperty : offerCallingChannelPropertiesForLanguage)
          {
            if (! languageProperties.containsKey(offerCallingChannelProperty.getProperty()))
              {
                languageProperties.put(offerCallingChannelProperty.getProperty(), new OfferCallingChannelProperty(offerCallingChannelProperty.getProperty(), null, new HashMap<String,String>()));
              }
          }
      }

    //
    //  populate textValues
    //

    for (String languageID : unprocessedLanguageProperties.keySet())
      {
        Set<OfferCallingChannelProperty> offerCallingChannelPropertiesForLanguage = unprocessedLanguageProperties.get(languageID);
        for (OfferCallingChannelProperty unprocessedOfferCallingChannelProperty : offerCallingChannelPropertiesForLanguage)
          {
            OfferCallingChannelProperty offerCallingChannelProperty = languageProperties.get(unprocessedOfferCallingChannelProperty.getProperty());
            offerCallingChannelProperty.getTextValues().put(languageID, unprocessedOfferCallingChannelProperty.getPropertyValue());
          }
      }
    
    //
    //  rationalize
    //

    this.properties = new HashSet<OfferCallingChannelProperty>();
    this.properties.addAll(commonProperties);
    this.properties.addAll(languageProperties.values());
  }

  /*****************************************
  *
  *  decodeOfferCallingChannelProperties
  *
  *****************************************/

  private Set<OfferCallingChannelProperty> decodeOfferCallingChannelProperties(JSONArray jsonArray) throws GUIManagerException
  {
    Set<OfferCallingChannelProperty> result = new HashSet<OfferCallingChannelProperty>();
    if (jsonArray != null)
      {
        for (int i=0; i<jsonArray.size(); i++)
          {
            result.add(new OfferCallingChannelProperty((JSONObject) jsonArray.get(i)));
          }
      }
    return result;
  }

  /*****************************************
  *
  *  decodeOfferCallingChannelCommonProperties
  *
  *****************************************/

  private Set<OfferCallingChannelProperty> decodeOfferCallingChannelCommonProperties(JSONArray jsonArray) throws GUIManagerException
  {
    return decodeOfferCallingChannelProperties(jsonArray);
  }

  /*****************************************
  *
  *  decodeOfferCallingChannelLanguageProperties
  *
  *****************************************/

  private Map<String,Set<OfferCallingChannelProperty>> decodeOfferCallingChannelLanguageProperties(JSONArray jsonArray) throws GUIManagerException
  {
    Map<String,Set<OfferCallingChannelProperty>> result = new HashMap<String,Set<OfferCallingChannelProperty>>();
    if (jsonArray != null)
      {
        for (int i=0; i<jsonArray.size(); i++)
          {
            JSONObject languagePropertiesElement = (JSONObject) jsonArray.get(i);
            String languageID = JSONUtilities.decodeString(languagePropertiesElement, "languageID", true);
            Set<OfferCallingChannelProperty> properties = decodeOfferCallingChannelProperties(JSONUtilities.decodeJSONArray(languagePropertiesElement, "properties", false));
            result.put(languageID, properties);
          }
      }
    return result;
  }

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getCallingChannelID() { return callingChannelID; }
  public Set<OfferCallingChannelProperty> getOfferCallingChannelProperties() { return properties; }

  /*****************************************
  *
  *  serde
  *
  *****************************************/

  public static ConnectSerde<OfferCallingChannel> serde()
  {
    return new ConnectSerde<OfferCallingChannel>(schema, false, OfferCallingChannel.class, OfferCallingChannel::pack, OfferCallingChannel::unpack);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    OfferCallingChannel offerCallingChannel = (OfferCallingChannel) value;
    Struct struct = new Struct(schema);
    struct.put("callingChannelID", offerCallingChannel.getCallingChannelID());
    struct.put("properties", packOfferCallingChannelProperties(offerCallingChannel.getOfferCallingChannelProperties()));
    return struct;
  }

  /****************************************
  *
  *  packOfferCallingChannelProperties
  *
  ****************************************/

  private static List<Object> packOfferCallingChannelProperties(Set<OfferCallingChannelProperty> properties)
  {
    List<Object> result = new ArrayList<Object>();
    for (OfferCallingChannelProperty offerCallingChannelProperty : properties)
      {
        result.add(OfferCallingChannelProperty.pack(offerCallingChannelProperty));
      }
    return result;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static OfferCallingChannel unpack(SchemaAndValue schemaAndValue)
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
    String callingChannelID = valueStruct.getString("callingChannelID");
    Set<OfferCallingChannelProperty> properties = unpackOfferCallingChannelProperties(schema.field("properties").schema(), valueStruct.get("properties"));
    
    //
    //  return
    //

    return new OfferCallingChannel(callingChannelID, properties);
  }

  /*****************************************
  *
  *  unpackOfferCallingChannelProperties
  *
  *****************************************/

  private static Set<OfferCallingChannelProperty> unpackOfferCallingChannelProperties(Schema schema, Object value)
  {
    //
    //  get schema for OfferCallingChannelProperty
    //

    Schema propertySchema = schema.valueSchema();
    
    //
    //  unpack
    //

    Set<OfferCallingChannelProperty> result = new HashSet<OfferCallingChannelProperty>();
    List<Object> valueArray = (List<Object>) value;
    for (Object property : valueArray)
      {
        result.add(OfferCallingChannelProperty.unpack(new SchemaAndValue(propertySchema, property)));
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
    if (obj instanceof OfferCallingChannel)
      {
        OfferCallingChannel offerCallingChannel = (OfferCallingChannel) obj;
        result = true;
        result = result && Objects.equals(callingChannelID, offerCallingChannel.getCallingChannelID());
        result = result && Objects.equals(properties, offerCallingChannel.getOfferCallingChannelProperties());
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
    return callingChannelID.hashCode();
  }

  /****************************************************************************
  *
  *  OfferCallingChannelProperty
  *
  ****************************************************************************/

  public static class OfferCallingChannelProperty
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
      schemaBuilder.name("offer_presentation_channel_property");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
      schemaBuilder.field("callingChannelPropertyID", Schema.STRING_SCHEMA);
      schemaBuilder.field("propertyValue", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("textValues", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).name("offer_presentation_channel_property_textvalues").schema());
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

    private CallingChannelProperty property;
    private String propertyValue;
    private Map<String,String> textValues;

    /*****************************************
    *
    *  constructor -- simple
    *
    *****************************************/

    private OfferCallingChannelProperty(CallingChannelProperty property, String propertyValue, Map<String,String> textValues)
    {
      this.property = property;
      this.propertyValue = propertyValue;
      this.textValues = textValues;
    }

    /*****************************************
    *
    *  constructor -- external JSON
    *
    *****************************************/

    OfferCallingChannelProperty(JSONObject jsonRoot) throws GUIManagerException
    {
      //
      //  basic fields
      //

      this.property = Deployment.getCallingChannelProperties().get(JSONUtilities.decodeString(jsonRoot, "callingChannelPropertyID", true));
      String propertyName = JSONUtilities.decodeString(jsonRoot, "callingChannelPropertyName", true);
      this.propertyValue = JSONUtilities.decodeString(jsonRoot, "propertyValue", false);
      this.textValues = decodeTextValues(JSONUtilities.decodeJSONArray(jsonRoot, "textValues", false));

      //
      //  validate 
      //

      if (this.property == null) throw new GUIManagerException("property not supported", JSONUtilities.decodeString(jsonRoot, "callingChannelPropertyID", true));
      if (! Objects.equals(this.property.getName(),propertyName)) throw new GUIManagerException("propertyName mismatch", "(" + this.property.getName() + ", " + JSONUtilities.decodeString(jsonRoot, "callingChannelPropertyName", true) + ")");
    }
    
    /*****************************************
    *
    *  decodeTextValues
    *
    *****************************************/

    private Map<String,String> decodeTextValues(JSONArray jsonArray)
    {
      Map<String,String> result = new HashMap<String,String>();
      if (jsonArray != null)
        {
          for (int i=0; i<jsonArray.size(); i++)
            {
              JSONObject textEntry = (JSONObject) jsonArray.get(i);
              String languageID = JSONUtilities.decodeString(textEntry, "languageID", true);
              String propertyValue = JSONUtilities.decodeString(textEntry, "propertyValue", false);
              result.put(languageID, propertyValue);
            }
        }
      return result;
    }

    /*****************************************
    *
    *  accessors
    *
    *****************************************/

    public CallingChannelProperty getProperty() { return property; }
    public String getPropertyValue() { return propertyValue; }
    public Map<String,String> getTextValues() { return textValues; }
    public String getValue(String language) { return (propertyValue != null) ? propertyValue : textValues.get(Deployment.getSupportedLanguageID(language)); }

    /*****************************************
    *
    *  serde
    *
    *****************************************/

    public static ConnectSerde<OfferCallingChannelProperty> serde()
    {
      return new ConnectSerde<OfferCallingChannelProperty>(schema, false, OfferCallingChannelProperty.class, OfferCallingChannelProperty::pack, OfferCallingChannelProperty::unpack);
    }

    /*****************************************
    *
    *  pack
    *
    *****************************************/

    public static Object pack(Object value)
    {
      OfferCallingChannelProperty offerCallingChannelProperty = (OfferCallingChannelProperty) value;
      Struct struct = new Struct(schema);
      struct.put("callingChannelPropertyID", offerCallingChannelProperty.getProperty().getID());
      struct.put("propertyValue", offerCallingChannelProperty.getPropertyValue());
      struct.put("textValues", offerCallingChannelProperty.getTextValues());
      return struct;
    }

    /*****************************************
    *
    *  unpack
    *
    *****************************************/

    public static OfferCallingChannelProperty unpack(SchemaAndValue schemaAndValue)
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
      CallingChannelProperty property = Deployment.getCallingChannelProperties().get(valueStruct.getString("callingChannelPropertyID"));
      String propertyValue = valueStruct.getString("propertyValue");
      Map<String,String> textValues = (Map<String,String>) valueStruct.get("textValues");

      //
      //  validate
      //

      if (property == null) throw new SerializationException("property not supported: " + valueStruct.getString("callingChannelPropertyID"));

      //
      //  return
      //

      return new OfferCallingChannelProperty(property, propertyValue, textValues);
    }

    /*****************************************
    *
    *  equals
    *
    *****************************************/

    public boolean equals(Object obj)
    {
      boolean result = false;
      if (obj instanceof OfferCallingChannelProperty)
        {
          OfferCallingChannelProperty offerCallingChannelProperty = (OfferCallingChannelProperty) obj;
          result = true;
          result = result && Objects.equals(property, offerCallingChannelProperty.getProperty());
          result = result && Objects.equals(propertyValue, offerCallingChannelProperty.getPropertyValue());
          result = result && Objects.equals(textValues, offerCallingChannelProperty.getTextValues());
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
      return property.hashCode();
    }
  }
}
