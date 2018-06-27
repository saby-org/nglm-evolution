/*****************************************************************************
*
*  PresentationChannelProperty.java
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

public class PresentationChannelProperty
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(PresentationChannelProperty.class);

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
    schemaBuilder.name("presentation_channel_property");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("presentationChannel", Schema.STRING_SCHEMA);
    schemaBuilder.field("properties", SchemaBuilder.array(Property.schema()).schema());
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

  private PresentationChannel presentationChannel;
  private Set<Property> properties;

  /*****************************************
  *
  *  constructor -- simple
  *
  *****************************************/

  private PresentationChannelProperty(PresentationChannel presentationChannel, Set<Property> properties)
  {
    this.presentationChannel = presentationChannel;
    this.properties = properties;
  }

  /*****************************************
  *
  *  constructor -- external JSON
  *
  *****************************************/

  PresentationChannelProperty(JSONObject jsonRoot) throws GUIManagerException
  {
    //
    //  basic fields
    //

    this.presentationChannel = Deployment.getPresentationChannels().get(JSONUtilities.decodeString(jsonRoot, "presentationChannel", true));
    this.properties = decodeProperties(JSONUtilities.decodeJSONArray(jsonRoot, "properties", true));

    //
    //  validate 
    //
    
    if (this.presentationChannel == null) throw new GUIManagerException("unknown presentationChannel", JSONUtilities.decodeString(jsonRoot, "presentationChannel", true));
  }

  /*****************************************
  *
  *  decodeProperties
  *
  *****************************************/

  private Set<Property> decodeProperties(JSONArray jsonArray) throws GUIManagerException
  {
    Set<Property> result = new HashSet<Property>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        result.add(new Property((JSONObject) jsonArray.get(i)));
      }
    return result;
  }

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public PresentationChannel getPresentationChannel() { return presentationChannel; }
  public Set<Property> getProperties() { return properties; }

  /*****************************************
  *
  *  serde
  *
  *****************************************/

  public static ConnectSerde<PresentationChannelProperty> serde()
  {
    return new ConnectSerde<PresentationChannelProperty>(schema, false, PresentationChannelProperty.class, PresentationChannelProperty::pack, PresentationChannelProperty::unpack);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    PresentationChannelProperty presentationChannelProperty = (PresentationChannelProperty) value;
    Struct struct = new Struct(schema);
    struct.put("presentationChannel", presentationChannelProperty.getPresentationChannel().getPresentationChannelName());
    struct.put("properties", packProperties(presentationChannelProperty.getProperties()));
    return struct;
  }

  /****************************************
  *
  *  packProperties
  *
  ****************************************/

  private static List<Object> packProperties(Set<Property> properties)
  {
    List<Object> result = new ArrayList<Object>();
    for (Property property : properties)
      {
        result.add(Property.pack(property));
      }
    return result;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static PresentationChannelProperty unpack(SchemaAndValue schemaAndValue)
  {
    //
    //  data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? schema.version() : null;

    //
    //  unpack
    //

    Struct valueStruct = (Struct) value;
    PresentationChannel presentationChannel = Deployment.getPresentationChannels().get(valueStruct.getString("presentationChannel"));
    Set<Property> properties = unpackProperties(schema.field("properties").schema(), valueStruct.get("properties"));
    
    //
    //  validate
    //

    if (presentationChannel == null) throw new SerializationException("unknown presentationChannel: " + valueStruct.getString("presentationChannel"));

    //
    //  return
    //

    return new PresentationChannelProperty(presentationChannel, properties);
  }

  /*****************************************
  *
  *  unpackProperties
  *
  *****************************************/

  private static Set<Property> unpackProperties(Schema schema, Object value)
  {
    //
    //  get schema for Property
    //

    Schema propertySchema = schema.valueSchema();
    
    //
    //  unpack
    //

    Set<Property> result = new HashSet<Property>();
    List<Object> valueArray = (List<Object>) value;
    for (Object property : valueArray)
      {
        result.add(Property.unpack(new SchemaAndValue(propertySchema, property)));
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
    if (obj instanceof PresentationChannelProperty)
      {
        PresentationChannelProperty presentationChannelProperty = (PresentationChannelProperty) obj;
        result = true;
        result = result && Objects.equals(presentationChannel, presentationChannelProperty.getPresentationChannel());
        result = result && Objects.equals(properties, presentationChannelProperty.getProperties());
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
    return presentationChannel.hashCode();
  }

  /****************************************************************************
  *
  *  Property
  *
  ****************************************************************************/

  public static class Property
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
      schemaBuilder.name("presentation_channel_property_property");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
      schemaBuilder.field("language", Schema.STRING_SCHEMA);
      schemaBuilder.field("name", Schema.STRING_SCHEMA);
      schemaBuilder.field("shortDescription", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("longDescription", Schema.OPTIONAL_STRING_SCHEMA);
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

    private SupportedLanguage language;
    private String name;
    private String shortDescription;
    private String longDescription;

    /*****************************************
    *
    *  constructor -- simple
    *
    *****************************************/

    private Property(SupportedLanguage language, String name, String shortDescription, String longDescription)
    {
      this.language = language;
      this.name = name;
      this.shortDescription = shortDescription;
      this.longDescription = longDescription;
    }

    /*****************************************
    *
    *  constructor -- external JSON
    *
    *****************************************/

    Property(JSONObject jsonRoot) throws GUIManagerException
    {
      //
      //  basic fields
      //

      this.language = Deployment.getSupportedLanguages().get(JSONUtilities.decodeString(jsonRoot, "language", true));
      this.name = JSONUtilities.decodeString(jsonRoot, "offerName", true);
      this.shortDescription = JSONUtilities.decodeString(jsonRoot, "offerShortDescription", false);
      this.longDescription = JSONUtilities.decodeString(jsonRoot, "offerLongDescription", false);

      //
      //  validate 
      //

      if (this.language == null) throw new GUIManagerException("language not supported", JSONUtilities.decodeString(jsonRoot, "language", true));
    }
    
    /*****************************************
    *
    *  accessors
    *
    *****************************************/

    public SupportedLanguage getLanguage() { return language; }
    public String getName() { return name; }
    public String getShortDescription() { return shortDescription; }
    public String getLongDescription() { return longDescription; }

    /*****************************************
    *
    *  serde
    *
    *****************************************/

    public static ConnectSerde<Property> serde()
    {
      return new ConnectSerde<Property>(schema, false, Property.class, Property::pack, Property::unpack);
    }

    /*****************************************
    *
    *  pack
    *
    *****************************************/

    public static Object pack(Object value)
    {
      Property property = (Property) value;
      Struct struct = new Struct(schema);
      struct.put("language", property.getLanguage().getSupportedLanguageName());
      struct.put("name", property.getName());
      struct.put("shortDescription", property.getShortDescription());
      struct.put("longDescription", property.getLongDescription());
      return struct;
    }

    /*****************************************
    *
    *  unpack
    *
    *****************************************/

    public static Property unpack(SchemaAndValue schemaAndValue)
    {
      //
      //  data
      //

      Schema schema = schemaAndValue.schema();
      Object value = schemaAndValue.value();
      Integer schemaVersion = (schema != null) ? schema.version() : null;

      //
      //  unpack
      //

      Struct valueStruct = (Struct) value;
      SupportedLanguage language = Deployment.getSupportedLanguages().get(valueStruct.getString("language"));
      String name = valueStruct.getString("name");
      String shortDescription = valueStruct.getString("shortDescription");
      String longDescription = valueStruct.getString("longDescription");

      //
      //  validate
      //

      if (language == null) throw new SerializationException("language not supported: " + valueStruct.getString("language"));

      //
      //  return
      //

      return new Property(language, name, shortDescription, longDescription);
    }

    /*****************************************
    *
    *  equals
    *
    *****************************************/

    public boolean equals(Object obj)
    {
      boolean result = false;
      if (obj instanceof Property)
        {
          Property property = (Property) obj;
          result = true;
          result = result && Objects.equals(language, property.getLanguage());
          result = result && Objects.equals(name, property.getName());
          result = result && Objects.equals(shortDescription, property.getShortDescription());
          result = result && Objects.equals(longDescription, property.getLongDescription());
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
      return name.hashCode();
    }
  }
}
