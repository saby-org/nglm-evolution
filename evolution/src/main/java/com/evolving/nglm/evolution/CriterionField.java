/*****************************************************************************
*
*  CriterionField.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.EvaluationCriterion.CriterionDataType;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionException;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class CriterionField extends DeploymentManagedObject
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(CriterionField.class);

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
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(3));
    schemaBuilder.field("jsonRepresentation", Schema.STRING_SCHEMA);
    schemaBuilder.field("fieldDataType", Schema.STRING_SCHEMA);
    schemaBuilder.field("mandatoryParameter", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("generateDimension", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("esField", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("criterionFieldRetriever", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("expressionValuedParameter", SchemaBuilder.bool().defaultValue(false).schema());
    schemaBuilder.field("internalOnly", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("tagFormat", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("tagMaxLength", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("profileChangeEvent", SchemaBuilder.bool().defaultValue(false).schema());
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
  private boolean mandatoryParameter;
  private boolean generateDimension;
  private String esField;
  private String criterionFieldRetriever;
  private boolean expressionValuedParameter;
  private boolean internalOnly;
  private String tagFormat;
  private Integer tagMaxLength;
  private boolean profileChangeEvent;

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
  public boolean getMandatoryParameter() { return mandatoryParameter; }
  public boolean getGenerateDimension() { return generateDimension; }
  public String getESField() { return esField; }
  public String getCriterionFieldRetriever() { return criterionFieldRetriever; }
  public boolean getExpressionValuedParameter() { return expressionValuedParameter; }
  public boolean getInternalOnly() { return internalOnly; }
  public String getTagFormat() { return tagFormat; }
  public Integer getTagMaxLength() { return tagMaxLength; }
  public boolean getProfileChangeEvent() { return profileChangeEvent; }

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
    this.mandatoryParameter = JSONUtilities.decodeBoolean(jsonRoot, "mandatory", Boolean.FALSE);
    this.generateDimension = JSONUtilities.decodeBoolean(jsonRoot, "generateDimension", Boolean.FALSE);
    this.esField = JSONUtilities.decodeString(jsonRoot, "esField", false);
    this.criterionFieldRetriever = JSONUtilities.decodeString(jsonRoot, "retriever", false);
    this.expressionValuedParameter = JSONUtilities.decodeBoolean(jsonRoot, "expressionValuedParameter", Boolean.FALSE);
    this.internalOnly = JSONUtilities.decodeBoolean(jsonRoot, "internalOnly", Boolean.FALSE);
    this.tagFormat = JSONUtilities.decodeString(jsonRoot, "tagFormat", false);
    this.tagMaxLength = JSONUtilities.decodeInteger(jsonRoot, "tagMaxLength", false);
    this.profileChangeEvent = JSONUtilities.decodeBoolean(jsonRoot, "profileChangeEvent", Boolean.FALSE);

    //
    //  expressionValuedParameter
    //

    if (this.expressionValuedParameter)
      {
        switch (this.fieldDataType)
          {
            case IntegerCriterion:
            case DoubleCriterion:
            case StringCriterion:
            case BooleanCriterion:
            case DateCriterion:
            case EvaluationCriteriaParameter:
            case SMSMessageParameter:
            case EmailMessageParameter:
            case PushMessageParameter:
              break;

            default:
              throw new GUIManagerException("unsupported parameter expression type", this.fieldDataType.getExternalRepresentation());
          }
      }

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
  *  constructor -- ContextVariable
  *
  *****************************************/

  public CriterionField(ContextVariable contextVariable) throws GUIManagerException
  {
    this(generateCriterionField(contextVariable));
  }

  //
  //  constructor -- context variable
  //

  private static JSONObject generateCriterionField(ContextVariable contextVariable)
  {
    Map<String,Object> criterionFieldJSON = new HashMap<String,Object>();
    criterionFieldJSON.put("id", contextVariable.getID());
    criterionFieldJSON.put("name", contextVariable.getName());
    criterionFieldJSON.put("display", contextVariable.getName());
    criterionFieldJSON.put("dataType", contextVariable.getType().getExternalRepresentation());
    criterionFieldJSON.put("retriever", "getJourneyParameter");
    return JSONUtilities.encodeObject(criterionFieldJSON);
  }

  /*****************************************
  *
  *  constructor -- message tag
  *
  *****************************************/

  public CriterionField(String tagName) throws GUIManagerException
  {
    this(generateCriterionField(tagName));
  }

  //
  //  constructor -- tag name
  //

  private static JSONObject generateCriterionField(String tagName)
  {
    Map<String,Object> criterionFieldJSON = new HashMap<String,Object>();
    criterionFieldJSON.put("id", generateTagID(tagName));
    criterionFieldJSON.put("name", tagName);
    criterionFieldJSON.put("display", tagName);
    criterionFieldJSON.put("dataType", CriterionDataType.StringCriterion.getExternalRepresentation());
    criterionFieldJSON.put("expressionValuedParameter", true);
    criterionFieldJSON.put("retriever", "getSubscriberMessageParameterTag");
    return JSONUtilities.encodeObject(criterionFieldJSON);
  }

  //
  //  generateTagID
  //

  private static String generateTagID(String name)
  {
    //
    //  initialize result
    //
    
    StringBuilder result = new StringBuilder();
    result.append("tag");

    //
    //  append words in name to result
    //
    
    Pattern p = Pattern.compile("[a-zA-Z0-9]+");
    Matcher m = p.matcher(name);
    while (m.find())
      {
        result.append(".");
        result.append(m.group(0).toLowerCase());
      }

    //
    //  return
    //

    return result.toString();
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

  private CriterionField(JSONObject jsonRepresentation, CriterionDataType fieldDataType, boolean mandatoryParameter, boolean generateDimension, String esField, String criterionFieldRetriever, boolean expressionValuedParameter, boolean internalOnly, String tagFormat, Integer tagMaxLength, boolean profileChangeEvent)
  {
    //
    //  super
    //

    super(jsonRepresentation);


    //
    //  data
    //

    this.fieldDataType = fieldDataType;
    this.mandatoryParameter = mandatoryParameter;
    this.generateDimension = generateDimension;
    this.esField = esField;
    this.criterionFieldRetriever = criterionFieldRetriever;
    this.expressionValuedParameter = expressionValuedParameter;
    this.internalOnly = internalOnly;
    this.tagFormat = tagFormat;
    this.tagMaxLength = tagMaxLength;
    this.profileChangeEvent = profileChangeEvent;

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
    struct.put("mandatoryParameter", criterionField.getMandatoryParameter());
    struct.put("generateDimension", criterionField.getGenerateDimension());
    struct.put("esField", criterionField.getESField());
    struct.put("criterionFieldRetriever", criterionField.getCriterionFieldRetriever());
    struct.put("expressionValuedParameter", criterionField.getExpressionValuedParameter());
    struct.put("internalOnly", criterionField.getInternalOnly());
    struct.put("tagFormat", criterionField.getTagFormat());
    struct.put("tagMaxLength", criterionField.getTagMaxLength());
    struct.put("profileChangeEvent", criterionField.getProfileChangeEvent());
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
    boolean mandatoryParameter = valueStruct.getBoolean("mandatoryParameter");
    boolean generateDimension = (schemaVersion >= 3) ? valueStruct.getBoolean("generateDimension") : false;
    String esField = valueStruct.getString("esField");
    String criterionFieldRetriever = valueStruct.getString("criterionFieldRetriever");
    boolean expressionValuedParameter = (schemaVersion >= 2) ? valueStruct.getBoolean("expressionValuedParameter") : false;
    boolean internalOnly = valueStruct.getBoolean("internalOnly");
    String tagFormat = valueStruct.getString("tagFormat");
    Integer tagMaxLength = valueStruct.getInt32("tagMaxLength");
    boolean profileChangeEvent = (schemaVersion >= 2) ? valueStruct.getBoolean("profileChangeEvent") : false;

    //
    //  return
    //

    return new CriterionField(jsonRepresentation, fieldDataType, mandatoryParameter, generateDimension, esField, criterionFieldRetriever, expressionValuedParameter, internalOnly, tagFormat, tagMaxLength, profileChangeEvent);
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
  *  retrieveNormalized
  *
  *****************************************/

  public Object retrieveNormalized(SubscriberEvaluationRequest evaluationRequest)
  {
    /****************************************
    *
    *  retrieve fieldValue
    *
    ****************************************/

    Object criterionFieldValue;
    try
      {
        /*****************************************
        *
        *  retreive criterionFieldValue
        *
        *****************************************/

        criterionFieldValue = this.retrieve(evaluationRequest);
        
        /*****************************************
        *
        *  validate dataType
        *
        *****************************************/

        if (criterionFieldValue != null)
          {
            switch (this.getFieldDataType())
              {
                case IntegerCriterion:
                  if (criterionFieldValue instanceof Integer) criterionFieldValue = new Long(((Integer) criterionFieldValue).longValue());
                  if (! (criterionFieldValue instanceof Long)) throw new CriterionException("criterionField " + this + " expected integer retrieved " + criterionFieldValue.getClass());
                  break;

                case DoubleCriterion:
                  if (criterionFieldValue instanceof Number && ! (criterionFieldValue instanceof Double)) criterionFieldValue = new Double(((Number) criterionFieldValue).doubleValue());
                  if (! (criterionFieldValue instanceof Double)) throw new CriterionException("criterionField " + this + " expected double retrieved " + criterionFieldValue.getClass());
                  break;

                case StringCriterion:
                  if (! (criterionFieldValue instanceof String)) throw new CriterionException("criterionField " + this + " expected string retrieved " + criterionFieldValue.getClass());
                  break;

                case BooleanCriterion:
                  if (! (criterionFieldValue instanceof Boolean)) throw new CriterionException("criterionField " + this + " expected boolean retrieved " + criterionFieldValue.getClass());
                  break;

                case DateCriterion:
                  if (! (criterionFieldValue instanceof Date)) throw new CriterionException("criterionField " + this + " expected date retrieved " + criterionFieldValue.getClass());
                  break;

                case StringSetCriterion:
                  if (! (criterionFieldValue instanceof Set)) throw new CriterionException("criterionField " + this + " expected set retrieved " + criterionFieldValue.getClass());
                  for (Object object : (Set<Object>) criterionFieldValue)
                    {
                      if (! (object instanceof String)) throw new CriterionException("criterionField " + this + " expected set of string retrieved " + object.getClass());
                    }
                  break;
              }
          }

        /*****************************************
        *
        *  normalize
        *
        *****************************************/

        if (criterionFieldValue != null)
          {
            switch (this.getFieldDataType())
              {
                case StringCriterion:
                  String stringFieldValue = (String) criterionFieldValue;
                  criterionFieldValue = (stringFieldValue != null) ? stringFieldValue.toLowerCase() : stringFieldValue;
                  break;
                  
                case StringSetCriterion:
                  Set<String> normalizedStringSetFieldValue = new HashSet<String>();
                  for (String stringValue : (Set<String>) criterionFieldValue)
                    {
                      normalizedStringSetFieldValue.add((stringValue != null) ? stringValue.toLowerCase(): (String) stringValue);
                    }
                  criterionFieldValue = normalizedStringSetFieldValue;
                  break;
              }
          }
      }
    catch (CriterionException e)
      {
        log.info("invalid criterion field {}", this.getID());
        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.info(stackTraceWriter.toString());
        return null;
      }

    /*****************************************
    *
    *  return
    *
    *****************************************/

    return criterionFieldValue;
  }

  /*****************************************
  *
  *  resolveTagFormat
  *
  *****************************************/

  public String resolveTagFormat(CriterionDataType formatDataType)
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
        switch (formatDataType)
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
        switch (formatDataType)
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

  //
  //  w/ effective type
  //

  public int resolveTagMaxLength(CriterionDataType formatDataType)
  {
    /*****************************************
    *
    *  tagMaxLength -- set explicitly
    *
    *****************************************/

    if (tagMaxLength != null)
      {
        return tagMaxLength.intValue();
      }

    /*****************************************
    *
    *  tagMaxLength -- default
    *
    *****************************************/

    switch (formatDataType)
      {
        case StringCriterion:
          return 20;

        case BooleanCriterion:
          return 6;

        case IntegerCriterion:
          return 6;
          
        case DoubleCriterion:
          return 9;
          
        case DateCriterion:
          return 20;

        default:
          return 20;
      }
  }

  //
  //  w/ default type
  //

  public int resolveTagMaxLength()
  {
    return resolveTagMaxLength(this.getFieldDataType());
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
        result = result && Objects.equals(mandatoryParameter, criterionField.getMandatoryParameter());
        result = result && Objects.equals(generateDimension, criterionField.getGenerateDimension());
        result = result && Objects.equals(esField, criterionField.getESField());
        result = result && Objects.equals(criterionFieldRetriever, criterionField.getCriterionFieldRetriever());
        result = result && expressionValuedParameter == criterionField.getExpressionValuedParameter();
        result = result && internalOnly == criterionField.getInternalOnly();
        result = result && Objects.equals(tagFormat, criterionField.getTagFormat());
        result = result && Objects.equals(tagMaxLength, criterionField.getTagMaxLength());
        result = result && profileChangeEvent == criterionField.getProfileChangeEvent();
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
