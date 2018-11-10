/*****************************************************************************
*
*  ParameterMap.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.ServerRuntimeException;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ParameterMap extends HashMap<String,Object>
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
    schemaBuilder.name("parameter_map");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("nullParameters", SchemaBuilder.array(Schema.STRING_SCHEMA).schema());
    schemaBuilder.field("emptySetParameters", SchemaBuilder.array(Schema.STRING_SCHEMA).schema());
    schemaBuilder.field("emptyListParameters", SchemaBuilder.array(Schema.STRING_SCHEMA).schema());
    schemaBuilder.field("integerParameters", SchemaBuilder.map(Schema.STRING_SCHEMA,Schema.INT32_SCHEMA).schema());
    schemaBuilder.field("doubleParameters", SchemaBuilder.map(Schema.STRING_SCHEMA,Schema.FLOAT64_SCHEMA).schema());
    schemaBuilder.field("stringParameters", SchemaBuilder.map(Schema.STRING_SCHEMA,Schema.STRING_SCHEMA).schema());
    schemaBuilder.field("booleanParameters", SchemaBuilder.map(Schema.STRING_SCHEMA,Schema.BOOLEAN_SCHEMA).schema());
    schemaBuilder.field("dateParameters", SchemaBuilder.map(Schema.STRING_SCHEMA,Timestamp.SCHEMA).schema());
    schemaBuilder.field("stringSetParameters", SchemaBuilder.map(Schema.STRING_SCHEMA,SchemaBuilder.array(Schema.STRING_SCHEMA)).schema());
    schemaBuilder.field("evaluationCriteriaParameters", SchemaBuilder.map(Schema.STRING_SCHEMA, SchemaBuilder.array(EvaluationCriterion.schema())).schema());
    schemaBuilder.field("smsMessageParameters", SchemaBuilder.map(Schema.STRING_SCHEMA, SMSMessage.schema()).schema());
    schema = schemaBuilder.build();
  };
  
  //
  //  serde
  //

  private static ConnectSerde<ParameterMap> serde = new ConnectSerde<ParameterMap>(schema, false, ParameterMap.class, ParameterMap::pack, ParameterMap::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<ParameterMap> serde() { return serde; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public ParameterMap()
  {
    super();
  }

  /*****************************************
  *
  *  constructor -- initialized
  *
  *****************************************/

  public ParameterMap(Map<String,Object> parameters)
  {
    super(parameters);
  }

  /*****************************************
  *
  *  constructor -- copy
  *
  *****************************************/

  public ParameterMap(ParameterMap parameterMap)
  {
    super(parameterMap);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    /*****************************************
    *
    *  header
    *
    *****************************************/

    ParameterMap parameterMap = (ParameterMap) value;
    Struct struct = new Struct(schema);

    /*****************************************
    *
    *  partition by data type
    *
    *****************************************/

    List<String> nullParameters = new ArrayList<String>();
    List<String> emptySetParameters = new ArrayList<String>();
    List<String> emptyListParameters = new ArrayList<String>();
    Map<String,Integer> integerParameters = new HashMap<String,Integer>();
    Map<String,Double> doubleParameters = new HashMap<String,Double>();
    Map<String,String> stringParameters = new HashMap<String,String>();
    Map<String,Boolean> booleanParameters = new HashMap<String,Boolean>();
    Map<String,Date> dateParameters = new HashMap<String,Date>();
    Map<String,List<String>> stringSetParameters = new HashMap<String,List<String>>();
    Map<String,List<EvaluationCriterion>> evaluationCriteriaParameters = new HashMap<String,List<EvaluationCriterion>>();
    Map<String,SMSMessage> smsMessageParameters = new HashMap<String,SMSMessage>();

    //
    //  partition
    //

    for (String key : parameterMap.keySet())
      {
        Object parameterValue = parameterMap.get(key);
        if (parameterValue == null)
          nullParameters.add(key);
        else if (parameterValue instanceof Set && ((Set) parameterValue).size() == 0)
          emptySetParameters.add(key);
        else if (parameterValue instanceof List && ((List) parameterValue).size() == 0)
          emptyListParameters.add(key);
        else if (parameterValue instanceof Integer)
          integerParameters.put(key, (Integer) parameterValue);
        else if (parameterValue instanceof Double)
          doubleParameters.put(key, (Double) parameterValue);
        else if (parameterValue instanceof String)
          stringParameters.put(key, (String) parameterValue);
        else if (parameterValue instanceof Boolean)
          booleanParameters.put(key, (Boolean) parameterValue);
        else if (parameterValue instanceof Date)
          dateParameters.put(key, (Date) parameterValue);
        else if (parameterValue instanceof Set && ((Set) parameterValue).iterator().next() instanceof String)
          stringSetParameters.put(key, new ArrayList<String>((Set<String>) parameterValue));
        else if (parameterValue instanceof List && ((List) parameterValue).iterator().next() instanceof EvaluationCriterion)
          evaluationCriteriaParameters.put(key, new ArrayList<EvaluationCriterion>((List<EvaluationCriterion>) parameterValue));
        else if (parameterValue instanceof SMSMessage)
          smsMessageParameters.put(key, (SMSMessage) parameterValue);
        else
          throw new ServerRuntimeException("invalid parameterMap data type: " + parameterValue.getClass());
      }

    /*****************************************
    *
    *  pack
    *
    *****************************************/

    struct.put("nullParameters", nullParameters);
    struct.put("emptySetParameters", emptySetParameters);
    struct.put("emptyListParameters", emptyListParameters);
    struct.put("integerParameters", integerParameters);
    struct.put("doubleParameters", doubleParameters);
    struct.put("stringParameters", stringParameters);
    struct.put("booleanParameters", booleanParameters);
    struct.put("dateParameters", dateParameters);
    struct.put("stringSetParameters", stringSetParameters);
    struct.put("evaluationCriteriaParameters", packEvaluationCriteriaParameters(evaluationCriteriaParameters));
    struct.put("smsMessageParameters", packSMSMessageParameters(smsMessageParameters));

    /*****************************************
    *
    *  return
    *
    *****************************************/

    return struct;
  }

  /*****************************************
  *
  *  packEvaluationCriteriaParameters
  *
  *****************************************/

  private static Map<String,List<Object>> packEvaluationCriteriaParameters(Map<String,List<EvaluationCriterion>> evaluationCriteriaParameters)
  {
    Map<String,List<Object>> result = new HashMap<String,List<Object>>();
    for (String parameterName : evaluationCriteriaParameters.keySet())
      {
        List<Object> packedEvaluationCriteria = new ArrayList<Object>();
        List<EvaluationCriterion> evaluationCriteria = evaluationCriteriaParameters.get(parameterName);
        for (EvaluationCriterion evaluationCriterion : evaluationCriteria)
          {
            packedEvaluationCriteria.add(EvaluationCriterion.pack(evaluationCriterion));
          }
        result.put(parameterName, packedEvaluationCriteria);
      }
    return result;
  }

  /*****************************************
  *
  *  packSMSMessageParameters
  *
  *****************************************/

  private static Map<String,Object> packSMSMessageParameters(Map<String,SMSMessage> smsMessageParameters)
  {
    Map<String,Object> result = new HashMap<String,Object>();
    for (String parameterName : smsMessageParameters.keySet())
      {
        result.put(parameterName, SMSMessage.pack(smsMessageParameters.get(parameterName)));
      }
    return result;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static ParameterMap unpack(SchemaAndValue schemaAndValue)
  {
    /*****************************************
    *
    *  header
    *
    *****************************************/

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;

    /*****************************************
    *
    *  unpack
    *
    *****************************************/

    Struct valueStruct = (Struct) value;
    List<String> nullParameters = (List<String>) valueStruct.get("nullParameters");
    List<String> emptySetParameters = (List<String>) valueStruct.get("emptySetParameters");
    List<String> emptyListParameters = (List<String>) valueStruct.get("emptyListParameters");
    Map<String,Integer> integerParameters = (Map<String,Integer>) valueStruct.get("integerParameters");
    Map<String,Double> doubleParameters = (Map<String,Double>) valueStruct.get("doubleParameters");
    Map<String,String> stringParameters = (Map<String,String>) valueStruct.get("stringParameters");
    Map<String,Boolean> booleanParameters = (Map<String,Boolean>) valueStruct.get("booleanParameters");
    Map<String,Date> dateParameters = (Map<String,Date>) valueStruct.get("dateParameters");
    Map<String,List<String>> stringSetParameters = (Map<String,List<String>>) valueStruct.get("stringSetParameters");
    Map<String,List<EvaluationCriterion>> evaluationCriteriaParameters = unpackEvaluationCriteriaParameters(schema.field("evaluationCriteriaParameters").schema(), (Map<String,List<Object>>) valueStruct.get("evaluationCriteriaParameters"));
    Map<String,SMSMessage> smsMessageParameters = unpackSMSMessageParameters(schema.field("smsMessageParameters").schema(), (Map<String,Object>) valueStruct.get("smsMessageParameters"));

    /*****************************************
    *
    *  result
    *
    *****************************************/

    ParameterMap result = new ParameterMap();
    for (String key : nullParameters) result.put(key,null);
    for (String key : emptySetParameters) result.put(key,new HashSet<Object>());
    for (String key : emptyListParameters) result.put(key,new ArrayList<Object>());
    for (String key : integerParameters.keySet()) result.put(key,integerParameters.get(key));
    for (String key : doubleParameters.keySet()) result.put(key,doubleParameters.get(key));
    for (String key : stringParameters.keySet()) result.put(key,stringParameters.get(key));
    for (String key : booleanParameters.keySet()) result.put(key,booleanParameters.get(key));
    for (String key : dateParameters.keySet()) result.put(key,dateParameters.get(key));
    for (String key : stringSetParameters.keySet()) result.put(key,new HashSet<String>(stringSetParameters.get(key)));
    for (String key : evaluationCriteriaParameters.keySet()) result.put(key,new ArrayList<EvaluationCriterion>(evaluationCriteriaParameters.get(key)));
    for (String key : smsMessageParameters.keySet()) result.put(key,smsMessageParameters.get(key));
    
    /*****************************************
    *
    *  return
    *
    *****************************************/

    return result;
  }

  /*****************************************
  *
  *  unpackEvaluationCriteriaParameters
  *
  *****************************************/

  public static Map<String,List<EvaluationCriterion>> unpackEvaluationCriteriaParameters(Schema schema, Map<String,List<Object>> value)
  {
    //
    //  get schema
    //

    Schema evaluationCriterionSchema = schema.valueSchema().valueSchema();

    //
    //  unpack
    //

    Map<String,List<EvaluationCriterion>> result = new HashMap<String,List<EvaluationCriterion>>();
    for (String key : value.keySet())
      {
        List<EvaluationCriterion> evaluationCriteria = new ArrayList<EvaluationCriterion>();
        List<Object> packedEvaluationCriteria = value.get(key);
        for (Object packedEvaluationCriterion : packedEvaluationCriteria)
          {
            EvaluationCriterion evaluationCriterion = EvaluationCriterion.unpack(new SchemaAndValue(evaluationCriterionSchema, packedEvaluationCriterion));
            evaluationCriteria.add(evaluationCriterion);
          }
        result.put(key, evaluationCriteria);
      }

    //
    //  return
    //

    return result;
  }

  /*****************************************
  *
  *  unpackSMSMessageParameters
  *
  *****************************************/

  public static Map<String,SMSMessage> unpackSMSMessageParameters(Schema schema, Map<String,Object> value)
  {
    //
    //  get schema
    //

    Schema smsMessageSchema = schema.valueSchema();

    //
    //  unpack
    //

    Map<String,SMSMessage> result = new HashMap<String,SMSMessage>();
    for (String key : value.keySet())
      {
        result.put(key, SMSMessage.unpack(new SchemaAndValue(smsMessageSchema, value.get(key))));
      }

    //
    //  return
    //

    return result;
  }
}
