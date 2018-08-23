/*****************************************************************************
*
*  OfferPresentationChannel.java
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
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class OfferPresentationChannel
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(OfferPresentationChannel.class);

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
    schemaBuilder.name("offer_presentation_channel");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("presentationChannelID", Schema.STRING_SCHEMA);
    schemaBuilder.field("properties", SchemaBuilder.array(OfferPresentationChannelProperty.schema()).schema());
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

  private String presentationChannelID;
  private Set<OfferPresentationChannelProperty> properties;

  /*****************************************
  *
  *  constructor -- simple
  *
  *****************************************/

  private OfferPresentationChannel(String presentationChannelID, Set<OfferPresentationChannelProperty> properties)
  {
    this.presentationChannelID = presentationChannelID;
    this.properties = properties;
  }

  /*****************************************
  *
  *  constructor -- external JSON
  *
  *****************************************/

  OfferPresentationChannel(JSONObject jsonRoot) throws GUIManagerException
  {
    /*****************************************
    *
    *  channel
    *
    *****************************************/

    this.presentationChannelID = JSONUtilities.decodeString(jsonRoot, "presentationChannelID", true);

    /*****************************************
    *
    *  properties
    *
    *****************************************/

    //
    //  unprocessed properties
    //

    Set<OfferPresentationChannelProperty> commonProperties = decodeOfferPresentationChannelCommonProperties(JSONUtilities.decodeJSONArray(jsonRoot, "commonProperties", false));
    Map<String,Set<OfferPresentationChannelProperty>> unprocessedLanguageProperties = decodeOfferPresentationChannelLanguageProperties(JSONUtilities.decodeJSONArray(jsonRoot, "languageProperties", false));

    //
    //  identify all languageProperties
    //

    Map<PresentationChannelProperty,OfferPresentationChannelProperty> languageProperties = new HashMap<PresentationChannelProperty,OfferPresentationChannelProperty>();
    for (Set<OfferPresentationChannelProperty> offerPresentationChannelPropertiesForLanguage : unprocessedLanguageProperties.values())
      {
        for (OfferPresentationChannelProperty offerPresentationChannelProperty : offerPresentationChannelPropertiesForLanguage)
          {
            if (! languageProperties.containsKey(offerPresentationChannelProperty.getProperty()))
              {
                languageProperties.put(offerPresentationChannelProperty.getProperty(), new OfferPresentationChannelProperty(offerPresentationChannelProperty.getProperty(), null, new HashMap<String,String>()));
              }
          }
      }

    //
    //  populate textValues
    //

    for (String languageID : unprocessedLanguageProperties.keySet())
      {
        Set<OfferPresentationChannelProperty> offerPresentationChannelPropertiesForLanguage = unprocessedLanguageProperties.get(languageID);
        for (OfferPresentationChannelProperty unprocessedOfferPresentationChannelProperty : offerPresentationChannelPropertiesForLanguage)
          {
            OfferPresentationChannelProperty offerPresentationChannelProperty = languageProperties.get(unprocessedOfferPresentationChannelProperty.getProperty());
            offerPresentationChannelProperty.getTextValues().put(languageID, unprocessedOfferPresentationChannelProperty.getPropertyValue());
          }
      }
    
    //
    //  rationalize
    //

    this.properties = new HashSet<OfferPresentationChannelProperty>();
    this.properties.addAll(commonProperties);
    this.properties.addAll(languageProperties.values());
  }

  /*****************************************
  *
  *  decodeOfferPresentationChannelProperties
  *
  *****************************************/

  private Set<OfferPresentationChannelProperty> decodeOfferPresentationChannelProperties(JSONArray jsonArray) throws GUIManagerException
  {
    Set<OfferPresentationChannelProperty> result = new HashSet<OfferPresentationChannelProperty>();
    if (jsonArray != null)
      {
        for (int i=0; i<jsonArray.size(); i++)
          {
            result.add(new OfferPresentationChannelProperty((JSONObject) jsonArray.get(i)));
          }
      }
    return result;
  }

  /*****************************************
  *
  *  decodeOfferPresentationChannelCommonProperties
  *
  *****************************************/

  private Set<OfferPresentationChannelProperty> decodeOfferPresentationChannelCommonProperties(JSONArray jsonArray) throws GUIManagerException
  {
    return decodeOfferPresentationChannelProperties(jsonArray);
  }

  /*****************************************
  *
  *  decodeOfferPresentationChannelLanguageProperties
  *
  *****************************************/

  private Map<String,Set<OfferPresentationChannelProperty>> decodeOfferPresentationChannelLanguageProperties(JSONArray jsonArray) throws GUIManagerException
  {
    Map<String,Set<OfferPresentationChannelProperty>> result = new HashMap<String,Set<OfferPresentationChannelProperty>>();
    if (jsonArray != null)
      {
        for (int i=0; i<jsonArray.size(); i++)
          {
            JSONObject languagePropertiesElement = (JSONObject) jsonArray.get(i);
            String languageID = JSONUtilities.decodeString(languagePropertiesElement, "languageID", true);
            Set<OfferPresentationChannelProperty> properties = decodeOfferPresentationChannelProperties(JSONUtilities.decodeJSONArray(languagePropertiesElement, "properties", false));
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

  public String getPresentationChannelID() { return presentationChannelID; }
  public Set<OfferPresentationChannelProperty> getOfferPresentationChannelProperties() { return properties; }

  /*****************************************
  *
  *  serde
  *
  *****************************************/

  public static ConnectSerde<OfferPresentationChannel> serde()
  {
    return new ConnectSerde<OfferPresentationChannel>(schema, false, OfferPresentationChannel.class, OfferPresentationChannel::pack, OfferPresentationChannel::unpack);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    OfferPresentationChannel offerPresentationChannel = (OfferPresentationChannel) value;
    Struct struct = new Struct(schema);
    struct.put("presentationChannelID", offerPresentationChannel.getPresentationChannelID());
    struct.put("properties", packOfferPresentationChannelProperties(offerPresentationChannel.getOfferPresentationChannelProperties()));
    return struct;
  }

  /****************************************
  *
  *  packOfferPresentationChannelProperties
  *
  ****************************************/

  private static List<Object> packOfferPresentationChannelProperties(Set<OfferPresentationChannelProperty> properties)
  {
    List<Object> result = new ArrayList<Object>();
    for (OfferPresentationChannelProperty offerPresentationChannelProperty : properties)
      {
        result.add(OfferPresentationChannelProperty.pack(offerPresentationChannelProperty));
      }
    return result;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static OfferPresentationChannel unpack(SchemaAndValue schemaAndValue)
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
    String presentationChannelID = valueStruct.getString("presentationChannelID");
    Set<OfferPresentationChannelProperty> properties = unpackOfferPresentationChannelProperties(schema.field("properties").schema(), valueStruct.get("properties"));
    
    //
    //  return
    //

    return new OfferPresentationChannel(presentationChannelID, properties);
  }

  /*****************************************
  *
  *  unpackOfferPresentationChannelProperties
  *
  *****************************************/

  private static Set<OfferPresentationChannelProperty> unpackOfferPresentationChannelProperties(Schema schema, Object value)
  {
    //
    //  get schema for OfferPresentationChannelProperty
    //

    Schema propertySchema = schema.valueSchema();
    
    //
    //  unpack
    //

    Set<OfferPresentationChannelProperty> result = new HashSet<OfferPresentationChannelProperty>();
    List<Object> valueArray = (List<Object>) value;
    for (Object property : valueArray)
      {
        result.add(OfferPresentationChannelProperty.unpack(new SchemaAndValue(propertySchema, property)));
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
    if (obj instanceof OfferPresentationChannel)
      {
        OfferPresentationChannel offerPresentationChannel = (OfferPresentationChannel) obj;
        result = true;
        result = result && Objects.equals(presentationChannelID, offerPresentationChannel.getPresentationChannelID());
        result = result && Objects.equals(properties, offerPresentationChannel.getOfferPresentationChannelProperties());
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
    return presentationChannelID.hashCode();
  }

  /****************************************************************************
  *
  *  OfferPresentationChannelProperty
  *
  ****************************************************************************/

  public static class OfferPresentationChannelProperty
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
      schemaBuilder.field("presentationChannelPropertyID", Schema.STRING_SCHEMA);
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

    private PresentationChannelProperty property;
    private String propertyValue;
    private Map<String,String> textValues;

    /*****************************************
    *
    *  constructor -- simple
    *
    *****************************************/

    private OfferPresentationChannelProperty(PresentationChannelProperty property, String propertyValue, Map<String,String> textValues)
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

    OfferPresentationChannelProperty(JSONObject jsonRoot) throws GUIManagerException
    {
      //
      //  basic fields
      //

      this.property = Deployment.getPresentationChannelProperties().get(JSONUtilities.decodeString(jsonRoot, "presentationChannelPropertyID", true));
      String propertyName = JSONUtilities.decodeString(jsonRoot, "presentationChannelPropertyName", true);
      this.propertyValue = JSONUtilities.decodeString(jsonRoot, "propertyValue", false);
      this.textValues = decodeTextValues(JSONUtilities.decodeJSONArray(jsonRoot, "textValues", false));

      //
      //  validate 
      //

      if (this.property == null) throw new GUIManagerException("property not supported", JSONUtilities.decodeString(jsonRoot, "presentationChannelPropertyID", true));
      if (! Objects.equals(this.property.getName(),propertyName)) throw new GUIManagerException("propertyName mismatch", "(" + this.property.getName() + ", " + JSONUtilities.decodeString(jsonRoot, "presentationChannelPropertyName", true) + ")");
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

    public PresentationChannelProperty getProperty() { return property; }
    public String getPropertyValue() { return propertyValue; }
    public Map<String,String> getTextValues() { return textValues; }
    public String getValue(String language) { return (propertyValue != null) ? propertyValue : textValues.get(Deployment.getSupportedLanguageID(language)); }

    /*****************************************
    *
    *  serde
    *
    *****************************************/

    public static ConnectSerde<OfferPresentationChannelProperty> serde()
    {
      return new ConnectSerde<OfferPresentationChannelProperty>(schema, false, OfferPresentationChannelProperty.class, OfferPresentationChannelProperty::pack, OfferPresentationChannelProperty::unpack);
    }

    /*****************************************
    *
    *  pack
    *
    *****************************************/

    public static Object pack(Object value)
    {
      OfferPresentationChannelProperty offerPresentationChannelProperty = (OfferPresentationChannelProperty) value;
      Struct struct = new Struct(schema);
      struct.put("presentationChannelPropertyID", offerPresentationChannelProperty.getProperty().getID());
      struct.put("propertyValue", offerPresentationChannelProperty.getPropertyValue());
      struct.put("textValues", offerPresentationChannelProperty.getTextValues());
      return struct;
    }

    /*****************************************
    *
    *  unpack
    *
    *****************************************/

    public static OfferPresentationChannelProperty unpack(SchemaAndValue schemaAndValue)
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
      PresentationChannelProperty property = Deployment.getPresentationChannelProperties().get(valueStruct.getString("presentationChannelPropertyID"));
      String propertyValue = valueStruct.getString("propertyValue");
      Map<String,String> textValues = (Map<String,String>) valueStruct.get("textValues");

      //
      //  validate
      //

      if (property == null) throw new SerializationException("property not supported: " + valueStruct.getString("presentationChannelPropertyID"));

      //
      //  return
      //

      return new OfferPresentationChannelProperty(property, propertyValue, textValues);
    }

    /*****************************************
    *
    *  equals
    *
    *****************************************/

    public boolean equals(Object obj)
    {
      boolean result = false;
      if (obj instanceof OfferPresentationChannelProperty)
        {
          OfferPresentationChannelProperty offerPresentationChannelProperty = (OfferPresentationChannelProperty) obj;
          result = true;
          result = result && Objects.equals(property, offerPresentationChannelProperty.getProperty());
          result = result && Objects.equals(propertyValue, offerPresentationChannelProperty.getPropertyValue());
          result = result && Objects.equals(textValues, offerPresentationChannelProperty.getTextValues());
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
