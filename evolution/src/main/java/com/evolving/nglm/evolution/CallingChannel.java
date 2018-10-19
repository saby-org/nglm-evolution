/*****************************************************************************
*
*  CallingChannel.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.EvaluationCriterion.CriterionContext;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.SchemaUtilities;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;

public class CallingChannel extends GUIManagedObject
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
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("calling_channel");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("mandatoryCallingChannelPropertyIDs", SchemaBuilder.array(Schema.STRING_SCHEMA));
    schemaBuilder.field("optionalCallingChannelPropertyIDs", SchemaBuilder.array(Schema.STRING_SCHEMA));
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<CallingChannel> serde = new ConnectSerde<CallingChannel>(schema, false, CallingChannel.class, CallingChannel::pack, CallingChannel::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<CallingChannel> serde() { return serde; }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private Set<CallingChannelProperty> mandatoryCallingChannelProperties;
  private Set<CallingChannelProperty> optionalCallingChannelProperties;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public String getCallingChannelID() { return getGUIManagedObjectID(); }
  public Set<CallingChannelProperty> getMandatoryCallingChannelProperties() { return mandatoryCallingChannelProperties; }
  public Set<CallingChannelProperty> getOptionalCallingChannelProperties() { return optionalCallingChannelProperties; }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public CallingChannel(SchemaAndValue schemaAndValue, Set<CallingChannelProperty> mandatoryCallingChannelProperties, Set<CallingChannelProperty> optionalCallingChannelProperties)
  {
    super(schemaAndValue);
    this.mandatoryCallingChannelProperties = mandatoryCallingChannelProperties;
    this.optionalCallingChannelProperties = optionalCallingChannelProperties;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    CallingChannel callingChannel = (CallingChannel) value;
    Struct struct = new Struct(schema);
    packCommon(struct, callingChannel);
    struct.put("mandatoryCallingChannelPropertyIDs", packCallingChannelProperties(callingChannel.getMandatoryCallingChannelProperties()));
    struct.put("optionalCallingChannelPropertyIDs", packCallingChannelProperties(callingChannel.getOptionalCallingChannelProperties()));
    return struct;
  }
  
  /*****************************************
  *
  *  packCallingChannelProperties
  *
  *****************************************/

  private static List<Object> packCallingChannelProperties(Set<CallingChannelProperty> callingChannelProperties)
  {
    List<Object> result = new ArrayList<Object>();
    for (CallingChannelProperty callingChannelProperty : callingChannelProperties)
      {
        result.add(callingChannelProperty.getID());
      }
    return result;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static CallingChannel unpack(SchemaAndValue schemaAndValue)
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
    Set<CallingChannelProperty> mandatoryCallingChannelProperties = unpackCallingChannelProperties((List<String>) valueStruct.get("mandatoryCallingChannelPropertyIDs"));
    Set<CallingChannelProperty> optionalCallingChannelProperties = unpackCallingChannelProperties((List<String>) valueStruct.get("optionalCallingChannelPropertyIDs"));
    
    //
    //  return
    //

    return new CallingChannel(schemaAndValue, mandatoryCallingChannelProperties, optionalCallingChannelProperties);
  }

  /*****************************************
  *
  *  unpackCallingChannelProperties
  *
  *****************************************/

  private static Set<CallingChannelProperty> unpackCallingChannelProperties(List<String> callingChannelPropertyIDs)
  {
    Set<CallingChannelProperty> result = new HashSet<CallingChannelProperty>();
    for (String callingChannelPropertyID : callingChannelPropertyIDs)
      {
        CallingChannelProperty callingChannelProperty = Deployment.getCallingChannelProperties().get(callingChannelPropertyID);
        if (callingChannelProperty == null) throw new SerializationException("unknown callingChannelProperty: " + callingChannelPropertyID);
        result.add(callingChannelProperty);
      }
    return result;
  }

  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public CallingChannel(JSONObject jsonRoot, long epoch, GUIManagedObject existingCallingChannelUnchecked) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingCallingChannelUnchecked != null) ? existingCallingChannelUnchecked.getEpoch() : epoch);

    /*****************************************
    *
    *  existingCallingChannel
    *
    *****************************************/

    CallingChannel existingCallingChannel = (existingCallingChannelUnchecked != null && existingCallingChannelUnchecked instanceof CallingChannel) ? (CallingChannel) existingCallingChannelUnchecked : null;
    
    /*****************************************
    *
    *  attributes
    *
    *****************************************/

    this.mandatoryCallingChannelProperties = decodeCallingChannelProperties(JSONUtilities.decodeJSONArray(jsonRoot, "properties", true), true);
    this.optionalCallingChannelProperties = decodeCallingChannelProperties(JSONUtilities.decodeJSONArray(jsonRoot, "properties", true), false);

    /*****************************************
    *
    *  validate
    *
    *****************************************/

    if (getRawEffectiveStartDate() != null) throw new GUIManagerException("unsupported start date", JSONUtilities.decodeString(jsonRoot, "effectiveStartDate", false));
    if (getRawEffectiveEndDate() != null) throw new GUIManagerException("unsupported end date", JSONUtilities.decodeString(jsonRoot, "effectiveEndDate", false));

    /*****************************************
    *
    *  epoch
    *
    *****************************************/

    if (epochChanged(existingCallingChannel))
      {
        this.setEpoch(epoch);
      }
  }

  /*****************************************
  *
  *  decodeCallingChannelProperties
  *
  *****************************************/

  private Set<CallingChannelProperty> decodeCallingChannelProperties(JSONArray jsonArray, boolean returnMandatory) throws GUIManagerException, JSONUtilitiesException
  {
    Set<CallingChannelProperty> result = new HashSet<CallingChannelProperty>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        JSONObject propertyDeclaration = (JSONObject) jsonArray.get(i);
        CallingChannelProperty callingChannelProperty = Deployment.getCallingChannelProperties().get(JSONUtilities.decodeString(propertyDeclaration, "callingChannelPropertyID", true));
        String callingChannelPropertyName = JSONUtilities.decodeString(propertyDeclaration, "callingChannelPropertyName", true);
        boolean mandatory = JSONUtilities.decodeBoolean(propertyDeclaration, "mandatory", Boolean.FALSE);
        if (callingChannelProperty == null) throw new GUIManagerException("unknown callingChannelProperty", JSONUtilities.decodeString(propertyDeclaration, "callingChannelPropertyID", true));
        if (! Objects.equals(callingChannelPropertyName, callingChannelProperty.getName())) throw new GUIManagerException("propertyName mismatch", "(" + callingChannelProperty.getName() + ", " + callingChannelPropertyName);
        if (mandatory == returnMandatory) result.add(callingChannelProperty);
      }
    return result;
  }

  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(CallingChannel existingCallingChannel)
  {
    if (existingCallingChannel != null && existingCallingChannel.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingCallingChannel.getGUIManagedObjectID());
        epochChanged = epochChanged || ! Objects.equals(mandatoryCallingChannelProperties, existingCallingChannel.getMandatoryCallingChannelProperties());
        epochChanged = epochChanged || ! Objects.equals(optionalCallingChannelProperties, existingCallingChannel.getOptionalCallingChannelProperties());
        return epochChanged;
      }
    else
      {
        return true;
      }
  }
}
