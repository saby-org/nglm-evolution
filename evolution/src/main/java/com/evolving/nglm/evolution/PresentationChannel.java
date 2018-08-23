/*****************************************************************************
*
*  PresentationChannel.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.EvaluationCriterion.CriterionContext;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.SchemaUtilities;

import com.rii.utilities.JSONUtilities;
import com.rii.utilities.JSONUtilities.JSONUtilitiesException;
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

public class PresentationChannel extends GUIManagedObject
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
    schemaBuilder.name("presentation_channel");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("mandatoryPresentationChannelPropertyIDs", SchemaBuilder.array(Schema.STRING_SCHEMA));
    schemaBuilder.field("optionalPresentationChannelPropertyIDs", SchemaBuilder.array(Schema.STRING_SCHEMA));
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<PresentationChannel> serde = new ConnectSerde<PresentationChannel>(schema, false, PresentationChannel.class, PresentationChannel::pack, PresentationChannel::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<PresentationChannel> serde() { return serde; }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private Set<PresentationChannelProperty> mandatoryPresentationChannelProperties;
  private Set<PresentationChannelProperty> optionalPresentationChannelProperties;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public String getPresentationChannelID() { return getGUIManagedObjectID(); }
  public Set<PresentationChannelProperty> getMandatoryPresentationChannelProperties() { return mandatoryPresentationChannelProperties; }
  public Set<PresentationChannelProperty> getOptionalPresentationChannelProperties() { return optionalPresentationChannelProperties; }

  /*****************************************
  *
  *  getSummaryJSONRepresentation
  *
  *****************************************/

  @Override JSONObject getSummaryJSONRepresentation()
  {
    JSONObject jsonRepresentation = new JSONObject();
    jsonRepresentation.putAll(getJSONRepresentation());
    jsonRepresentation.remove("properties");
    return jsonRepresentation;
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public PresentationChannel(SchemaAndValue schemaAndValue, Set<PresentationChannelProperty> mandatoryPresentationChannelProperties, Set<PresentationChannelProperty> optionalPresentationChannelProperties)
  {
    super(schemaAndValue);
    this.mandatoryPresentationChannelProperties = mandatoryPresentationChannelProperties;
    this.optionalPresentationChannelProperties = optionalPresentationChannelProperties;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    PresentationChannel presentationChannel = (PresentationChannel) value;
    Struct struct = new Struct(schema);
    packCommon(struct, presentationChannel);
    struct.put("mandatoryPresentationChannelPropertyIDs", packPresentationChannelProperties(presentationChannel.getMandatoryPresentationChannelProperties()));
    struct.put("optionalPresentationChannelPropertyIDs", packPresentationChannelProperties(presentationChannel.getOptionalPresentationChannelProperties()));
    return struct;
  }
  
  /*****************************************
  *
  *  packPresentationChannelProperties
  *
  *****************************************/

  private static List<Object> packPresentationChannelProperties(Set<PresentationChannelProperty> presentationChannelProperties)
  {
    List<Object> result = new ArrayList<Object>();
    for (PresentationChannelProperty presentationChannelProperty : presentationChannelProperties)
      {
        result.add(presentationChannelProperty.getID());
      }
    return result;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static PresentationChannel unpack(SchemaAndValue schemaAndValue)
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
    Set<PresentationChannelProperty> mandatoryPresentationChannelProperties = unpackPresentationChannelProperties((List<String>) valueStruct.get("mandatoryPresentationChannelPropertyIDs"));
    Set<PresentationChannelProperty> optionalPresentationChannelProperties = unpackPresentationChannelProperties((List<String>) valueStruct.get("optionalPresentationChannelPropertyIDs"));
    
    //
    //  return
    //

    return new PresentationChannel(schemaAndValue, mandatoryPresentationChannelProperties, optionalPresentationChannelProperties);
  }

  /*****************************************
  *
  *  unpackPresentationChannelProperties
  *
  *****************************************/

  private static Set<PresentationChannelProperty> unpackPresentationChannelProperties(List<String> presentationChannelPropertyIDs)
  {
    Set<PresentationChannelProperty> result = new HashSet<PresentationChannelProperty>();
    for (String presentationChannelPropertyID : presentationChannelPropertyIDs)
      {
        PresentationChannelProperty presentationChannelProperty = Deployment.getPresentationChannelProperties().get(presentationChannelPropertyID);
        if (presentationChannelProperty == null) throw new SerializationException("unknown presentationChannelProperty: " + presentationChannelPropertyID);
        result.add(presentationChannelProperty);
      }
    return result;
  }

  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public PresentationChannel(JSONObject jsonRoot, long epoch, GUIManagedObject existingPresentationChannelUnchecked) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingPresentationChannelUnchecked != null) ? existingPresentationChannelUnchecked.getEpoch() : epoch);

    /*****************************************
    *
    *  existingPresentationChannel
    *
    *****************************************/

    PresentationChannel existingPresentationChannel = (existingPresentationChannelUnchecked != null && existingPresentationChannelUnchecked instanceof PresentationChannel) ? (PresentationChannel) existingPresentationChannelUnchecked : null;
    
    /*****************************************
    *
    *  attributes
    *
    *****************************************/

    this.mandatoryPresentationChannelProperties = decodePresentationChannelProperties(JSONUtilities.decodeJSONArray(jsonRoot, "properties", true), true);
    this.optionalPresentationChannelProperties = decodePresentationChannelProperties(JSONUtilities.decodeJSONArray(jsonRoot, "properties", true), false);

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

    if (epochChanged(existingPresentationChannel))
      {
        this.setEpoch(epoch);
      }
  }

  /*****************************************
  *
  *  decodePresentationChannelProperties
  *
  *****************************************/

  private Set<PresentationChannelProperty> decodePresentationChannelProperties(JSONArray jsonArray, boolean returnMandatory) throws GUIManagerException, JSONUtilitiesException
  {
    Set<PresentationChannelProperty> result = new HashSet<PresentationChannelProperty>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        JSONObject propertyDeclaration = (JSONObject) jsonArray.get(i);
        PresentationChannelProperty presentationChannelProperty = Deployment.getPresentationChannelProperties().get(JSONUtilities.decodeString(propertyDeclaration, "presentationChannelPropertyID", true));
        String presentationChannelPropertyName = JSONUtilities.decodeString(propertyDeclaration, "presentationChannelPropertyName", true);
        boolean mandatory = JSONUtilities.decodeBoolean(propertyDeclaration, "mandatory", Boolean.FALSE);
        if (presentationChannelProperty == null) throw new GUIManagerException("unknown presentationChannelProperty", JSONUtilities.decodeString(propertyDeclaration, "presentationChannelPropertyID", true));
        if (! Objects.equals(presentationChannelPropertyName, presentationChannelProperty.getName())) throw new GUIManagerException("propertyName mismatch", "(" + presentationChannelProperty.getName() + ", " + presentationChannelPropertyName);
        if (mandatory == returnMandatory) result.add(presentationChannelProperty);
      }
    return result;
  }

  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(PresentationChannel existingPresentationChannel)
  {
    if (existingPresentationChannel != null && existingPresentationChannel.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingPresentationChannel.getGUIManagedObjectID());
        epochChanged = epochChanged || ! Objects.equals(mandatoryPresentationChannelProperties, existingPresentationChannel.getMandatoryPresentationChannelProperties());
        epochChanged = epochChanged || ! Objects.equals(optionalPresentationChannelProperties, existingPresentationChannel.getOptionalPresentationChannelProperties());
        return epochChanged;
      }
    else
      {
        return true;
      }
  }
}
