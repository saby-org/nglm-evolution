/*****************************************************************************
*
*  CriterionField.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.EvaluationCriterion.CriterionDataType;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.DeploymentManagedObject;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.ServerRuntimeException;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import com.evolving.nglm.core.SystemTime;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.common.errors.SerializationException;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class CriterionField extends DeploymentManagedObject
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
    schemaBuilder.name("criterion_field");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("jsonRepresentation", Schema.STRING_SCHEMA);
    schemaBuilder.field("fieldDataType", Schema.STRING_SCHEMA);
    schemaBuilder.field("esField", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("criterionFieldRetriever", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("internalOnly", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("tagFormat", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("tagMaxLength", Schema.OPTIONAL_INT32_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<CriterionField> serde = new ConnectSerde<CriterionField>(schema, false, CriterionField.class, CriterionField::pack, CriterionField::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<CriterionField> serde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private CriterionDataType fieldDataType;
  private String esField;
  private String criterionFieldRetriever;
  private boolean internalOnly;
  private String tagFormat;
  private Integer tagMaxLength;

  //
  //  calculated
  //

  private MethodHandle retriever = null;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public CriterionDataType getFieldDataType() { return fieldDataType; }
  public String getESField() { return esField; }
  public String getCriterionFieldRetriever() { return criterionFieldRetriever; }
  public boolean getInternalOnly() { return internalOnly; }
  public String getTagFormat() { return tagFormat; }
  public Integer getTagMaxLength() { return tagMaxLength; }

  /*****************************************
  *
  *  special fields
  *
  *****************************************/

  public static final String EvaluationDateField = "evaluation.date";
  public static final String EventNameField = "node.parameter.eventname";

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public CriterionField(JSONObject jsonRoot) throws GUIManagerException
  {
    //
    //  super
    //

    super(jsonRoot);

    //
    //  data
    //

    this.fieldDataType = CriterionDataType.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "dataType", true));
    this.esField = JSONUtilities.decodeString(jsonRoot, "esField", false);
    this.criterionFieldRetriever = JSONUtilities.decodeString(jsonRoot, "retriever", false);
    this.internalOnly = JSONUtilities.decodeBoolean(jsonRoot, "internalOnly", Boolean.FALSE);
    this.tagFormat = JSONUtilities.decodeString(jsonRoot, "tagFormat", false);
    this.tagMaxLength = JSONUtilities.decodeInteger(jsonRoot, "tagMaxLength", false);

    //
    //  retriever
    //

    if (this.criterionFieldRetriever != null)
      {
        try
          {
            MethodType methodType = MethodType.methodType(Object.class, SubscriberEvaluationRequest.class, String.class);
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            this.retriever = lookup.findStatic(Deployment.getCriterionFieldRetrieverClass(), criterionFieldRetriever, methodType);
          }
        catch (NoSuchMethodException | IllegalAccessException e)
          {
            throw new GUIManagerException(e);
          }
      }
  }

  /*****************************************
  *
  *  constructor -- constructed
  *
  *****************************************/

  public CriterionField(CriterionField criterionField, String id, String criterionFieldRetriever, boolean internalOnly, String tagFormat, Integer tagMaxLength) throws GUIManagerException
  {
    this(generateCriterionField(criterionField, id, criterionFieldRetriever, internalOnly, tagFormat, tagMaxLength));
  }

  //
  //  constructor -- constructed with default display
  //

  private static JSONObject generateCriterionField(CriterionField criterionField, String id, String criterionFieldRetriever, boolean internalOnly, String tagFormat, Integer tagMaxLength)
  {
    JSONObject criterionFieldJSON = (JSONObject) criterionField.getJSONRepresentation().clone();
    criterionFieldJSON.put("id", id);
    criterionFieldJSON.put("retriever", criterionFieldRetriever);
    criterionFieldJSON.put("esField", null);
    criterionFieldJSON.put("internalOnly", internalOnly);
    criterionFieldJSON.put("tagFormat", tagFormat);
    criterionFieldJSON.put("tagMaxLength", tagMaxLength);
    return criterionFieldJSON;
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  private CriterionField(JSONObject jsonRepresentation, CriterionDataType fieldDataType, String esField, String criterionFieldRetriever, boolean internalOnly, String tagFormat, Integer tagMaxLength)
  {
    //
    //  super
    //

    super(jsonRepresentation);


    //
    //  data
    //

    this.fieldDataType = fieldDataType;
    this.esField = esField;
    this.criterionFieldRetriever = criterionFieldRetriever;
    this.internalOnly = internalOnly;
    this.tagFormat = tagFormat;
    this.tagMaxLength = tagMaxLength;

    //
    //  retriever
    //

    if (this.criterionFieldRetriever != null)
      {
        try
          {
            MethodType methodType = MethodType.methodType(Object.class, SubscriberEvaluationRequest.class, String.class);
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            retriever = lookup.findStatic(Deployment.getCriterionFieldRetrieverClass(), criterionFieldRetriever, methodType);
          }
        catch (NoSuchMethodException | IllegalAccessException e)
          {
            throw new SerializationException("invalid criterionField retriever", e);
          }
      }
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    CriterionField criterionField = (CriterionField) value;
    Struct struct = new Struct(schema);
    struct.put("jsonRepresentation", criterionField.getJSONRepresentation().toString());
    struct.put("fieldDataType", criterionField.getFieldDataType().getExternalRepresentation());
    struct.put("esField", criterionField.getESField());
    struct.put("criterionFieldRetriever", criterionField.getCriterionFieldRetriever());
    struct.put("internalOnly", criterionField.getInternalOnly());
    struct.put("tagFormat", criterionField.getTagFormat());
    struct.put("tagMaxLength", criterionField.getTagMaxLength());
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static CriterionField unpack(SchemaAndValue schemaAndValue)
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
    JSONObject jsonRepresentation = parseRepresentation(valueStruct.getString("jsonRepresentation"));
    CriterionDataType fieldDataType = CriterionDataType.fromExternalRepresentation(valueStruct.getString("fieldDataType"));
    String esField = valueStruct.getString("esField");
    String criterionFieldRetriever = valueStruct.getString("criterionFieldRetriever");
    boolean internalOnly = valueStruct.getBoolean("internalOnly");
    String tagFormat = valueStruct.getString("tagFormat");
    Integer tagMaxLength = valueStruct.getInt32("tagMaxLength");

    //
    //  return
    //

    return new CriterionField(jsonRepresentation, fieldDataType, esField, criterionFieldRetriever, internalOnly, tagFormat, tagMaxLength);
  }

  /*****************************************
  *
  *  parseRepresentation
  *
  *****************************************/

  private static JSONObject parseRepresentation(String jsonString) throws JSONUtilitiesException
  {
    JSONObject result = null;
    try
      {
        result = (JSONObject) (new JSONParser()).parse(jsonString);
      }
    catch (org.json.simple.parser.ParseException e)
      {
        throw new JSONUtilitiesException("jsonRepresentation", e);
      }
    return result;
  }

  /*****************************************
  *
  *  retrieve
  *
  *****************************************/

  public Object retrieve(SubscriberEvaluationRequest evaluationRequest)
  {
    if (retriever != null)
      {
        try
          {
            return retriever.invokeExact(evaluationRequest, this.getID());
          }
        catch (RuntimeException | Error e)
          {
            throw e;
          }
        catch (Throwable e)
          {
            throw new ServerRuntimeException(e);
          }
      }
    else
      {
        throw new UnsupportedOperationException();
      }
  }

  /*****************************************
  *
  *  resolveTagFormat
  *
  *****************************************/

  public String resolveTagFormat()
  {
    /*****************************************
    *
    *  tagFormat from criterionField
    *
    *****************************************/

    String tagFormat = this.getTagFormat();

    /*****************************************
    *
    *  tagFormat default (if necessary)
    *
    *****************************************/

    if (tagFormat == null)
      {
        switch (this.getFieldDataType())
          {
            case IntegerCriterion:
              tagFormat = "#0";
              break;

            case DoubleCriterion:
              tagFormat = "#0.00";
              break;

            case DateCriterion:
              tagFormat = "date,short";
              break;
          }
      }

    /*****************************************
    *
    *  result
    *
    *****************************************/

    String result = "";
    if (tagFormat != null)
      {
        switch (this.getFieldDataType())
          {
            case IntegerCriterion:
            case DoubleCriterion:
              result = "," + "number" + "," + tagFormat;
              break;

            case DateCriterion:
              result = "," + tagFormat;
              break;
          }
      }
    return result;
  }

  /*****************************************
  *
  *  resolveTagMaxLength
  *
  *****************************************/

  public int resolveTagMaxLength()
  {
    return (tagMaxLength != null) ? tagMaxLength.intValue() : 20;
  }

  /*****************************************
  *
  *  equals
  *
  *****************************************/

  public boolean equals(Object obj)
  {
    boolean result = false;
    if (obj instanceof CriterionField)
      {
        CriterionField criterionField = (CriterionField) obj;
        result = super.equals(obj);
        result = result && Objects.equals(fieldDataType, criterionField.getFieldDataType());
        result = result && Objects.equals(esField, criterionField.getESField());
        result = result && Objects.equals(criterionFieldRetriever, criterionField.getCriterionFieldRetriever());
        result = result && internalOnly == criterionField.getInternalOnly();
        result = result && Objects.equals(tagFormat, criterionField.getTagFormat());
        result = result && Objects.equals(tagMaxLength, criterionField.getTagMaxLength());
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
    return super.hashCode();
  }
}
