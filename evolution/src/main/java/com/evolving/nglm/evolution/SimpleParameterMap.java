/*****************************************************************************
*
*  SimpleParameterMap.java
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

public class SimpleParameterMap extends HashMap<String,Object>
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
    schemaBuilder.name("simple_parameter_map");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(2));
    schemaBuilder.field("nullParameters", SchemaBuilder.array(Schema.STRING_SCHEMA).schema());
    schemaBuilder.field("emptySetParameters", SchemaBuilder.array(Schema.STRING_SCHEMA).schema());
    schemaBuilder.field("emptyListParameters", SchemaBuilder.array(Schema.STRING_SCHEMA).schema());
    schemaBuilder.field("integerParameters", SchemaBuilder.map(Schema.STRING_SCHEMA,Schema.INT32_SCHEMA).name("parameter_map_integers").schema());
    schemaBuilder.field("longParameters", SchemaBuilder.map(Schema.STRING_SCHEMA,Schema.INT64_SCHEMA).name("parameter_map_longs").schema());
    schemaBuilder.field("doubleParameters", SchemaBuilder.map(Schema.STRING_SCHEMA,Schema.FLOAT64_SCHEMA).name("parameter_map_doubles").schema());
    schemaBuilder.field("stringParameters", SchemaBuilder.map(Schema.STRING_SCHEMA,Schema.STRING_SCHEMA).name("parameter_map_strings").schema());
    schemaBuilder.field("booleanParameters", SchemaBuilder.map(Schema.STRING_SCHEMA,Schema.BOOLEAN_SCHEMA).name("parameter_map_booleans").schema());
    schemaBuilder.field("dateParameters", SchemaBuilder.map(Schema.STRING_SCHEMA,Timestamp.SCHEMA).name("parameter_map_dates").schema());
    schemaBuilder.field("stringSetParameters", SchemaBuilder.map(Schema.STRING_SCHEMA,SchemaBuilder.array(Schema.STRING_SCHEMA)).name("parameter_map_stringsets").schema());
    schemaBuilder.field("integerSetParameters", SchemaBuilder.map(Schema.STRING_SCHEMA,SchemaBuilder.array(Schema.INT32_SCHEMA)).name("parameter_map_integersets").schema());
    schemaBuilder.field("evaluationCriteriaParameters", SchemaBuilder.map(Schema.STRING_SCHEMA, SchemaBuilder.array(EvaluationCriterion.schema())).name("parameter_map_criteria").schema());
    schemaBuilder.field("parameterExpressionParameters", SchemaBuilder.map(Schema.STRING_SCHEMA, ParameterExpression.schema()).name("parameter_map_expression").schema());
    schema = schemaBuilder.build();
  };
  
  //
  //  serde
  //

  private static ConnectSerde<SimpleParameterMap> serde = new ConnectSerde<SimpleParameterMap>(schema, false, SimpleParameterMap.class, SimpleParameterMap::pack, SimpleParameterMap::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<SimpleParameterMap> serde() { return serde; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public SimpleParameterMap()
  {
    super();
  }

  /*****************************************
  *
  *  constructor -- initialized
  *
  *****************************************/

  public SimpleParameterMap(Map<String,Object> parameters)
  {
    super(parameters);
  }

  /*****************************************
  *
  *  constructor -- copy
  *
  *****************************************/

  public SimpleParameterMap(SimpleParameterMap parameterMap)
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

    SimpleParameterMap parameterMap = (SimpleParameterMap) value;
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
    Map<String,Long> longParameters = new HashMap<String,Long>();
    Map<String,Double> doubleParameters = new HashMap<String,Double>();
    Map<String,String> stringParameters = new HashMap<String,String>();
    Map<String,Boolean> booleanParameters = new HashMap<String,Boolean>();
    Map<String,Date> dateParameters = new HashMap<String,Date>();
    Map<String,List<String>> stringSetParameters = new HashMap<String,List<String>>();
    Map<String,List<Integer>> integerSetParameters = new HashMap<String,List<Integer>>();
    Map<String,List<EvaluationCriterion>> evaluationCriteriaParameters = new HashMap<String,List<EvaluationCriterion>>();
    Map<String,ParameterExpression> parameterExpressionParameters = new HashMap<String,ParameterExpression>();;

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
        else if (parameterValue instanceof Long)
          longParameters.put(key, (Long) parameterValue);
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
        else if (parameterValue instanceof Set && ((Set) parameterValue).iterator().next() instanceof Integer)
          integerSetParameters.put(key, new ArrayList<Integer>((Set<Integer>) parameterValue));
        else if (parameterValue instanceof List && ((List) parameterValue).iterator().next() instanceof EvaluationCriterion)
          evaluationCriteriaParameters.put(key, new ArrayList<EvaluationCriterion>((List<EvaluationCriterion>) parameterValue));
        else if (parameterValue instanceof ParameterExpression)
          parameterExpressionParameters.put(key, (ParameterExpression) parameterValue);
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
    struct.put("longParameters", longParameters);
    struct.put("doubleParameters", doubleParameters);
    struct.put("stringParameters", stringParameters);
    struct.put("booleanParameters", booleanParameters);
    struct.put("dateParameters", dateParameters);
    struct.put("stringSetParameters", stringSetParameters);
    struct.put("integerSetParameters", integerSetParameters);
    struct.put("evaluationCriteriaParameters", packEvaluationCriteriaParameters(evaluationCriteriaParameters));
    struct.put("parameterExpressionParameters", packParameterExpressionParameters(parameterExpressionParameters));

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
  *  packParameterExpressionParameters
  *
  *****************************************/

  private static Map<String,Object> packParameterExpressionParameters(Map<String,ParameterExpression> parameterExpressionParameters)
  {
    Map<String,Object> result = new HashMap<String,Object>();
    for (String parameterName : parameterExpressionParameters.keySet())
      {
        result.put(parameterName, ParameterExpression.pack(parameterExpressionParameters.get(parameterName)));
      }
    return result;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static SimpleParameterMap unpack(SchemaAndValue schemaAndValue)
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
    Map<String,Long> longParameters = (schemaVersion >= 2) ? (Map<String,Long>) valueStruct.get("longParameters") : new HashMap<String,Long>();
    Map<String,Double> doubleParameters = (Map<String,Double>) valueStruct.get("doubleParameters");
    Map<String,String> stringParameters = (Map<String,String>) valueStruct.get("stringParameters");
    Map<String,Boolean> booleanParameters = (Map<String,Boolean>) valueStruct.get("booleanParameters");
    Map<String,Date> dateParameters = (Map<String,Date>) valueStruct.get("dateParameters");
    Map<String,List<String>> stringSetParameters = (Map<String,List<String>>) valueStruct.get("stringSetParameters");
    Map<String,List<Integer>> integerSetParameters = (Map<String,List<Integer>>) valueStruct.get("integerSetParameters");
    Map<String,List<EvaluationCriterion>> evaluationCriteriaParameters = unpackEvaluationCriteriaParameters(schema.field("evaluationCriteriaParameters").schema(), (Map<String,List<Object>>) valueStruct.get("evaluationCriteriaParameters"));
    Map<String,ParameterExpression> parameterExpressionParameters = (schemaVersion >= 2) ? unpackParameterExpressionParameters(schema.field("parameterExpressionParameters").schema(), (Map<String,Object>) valueStruct.get("parameterExpressionParameters")) : new HashMap<String,ParameterExpression>();

    /*****************************************
    *
    *  result
    *
    *****************************************/

    SimpleParameterMap result = new SimpleParameterMap();
    for (String key : nullParameters) result.put(key,null);
    for (String key : emptySetParameters) result.put(key,new HashSet<Object>());
    for (String key : emptyListParameters) result.put(key,new ArrayList<Object>());
    for (String key : integerParameters.keySet()) result.put(key,integerParameters.get(key));
    for (String key : longParameters.keySet()) result.put(key,longParameters.get(key));
    for (String key : doubleParameters.keySet()) result.put(key,doubleParameters.get(key));
    for (String key : stringParameters.keySet()) result.put(key,stringParameters.get(key));
    for (String key : booleanParameters.keySet()) result.put(key,booleanParameters.get(key));
    for (String key : dateParameters.keySet()) result.put(key,dateParameters.get(key));
    for (String key : stringSetParameters.keySet()) result.put(key,new HashSet<String>(stringSetParameters.get(key)));
    for (String key : integerSetParameters.keySet()) result.put(key,new HashSet<Integer>(integerSetParameters.get(key)));
    for (String key : evaluationCriteriaParameters.keySet()) result.put(key,new ArrayList<EvaluationCriterion>(evaluationCriteriaParameters.get(key)));
    for (String key : parameterExpressionParameters.keySet()) result.put(key,parameterExpressionParameters.get(key));
    
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
  *  unpackParameterExpressionParameters
  *
  *****************************************/

  public static Map<String,ParameterExpression> unpackParameterExpressionParameters(Schema schema, Map<String,Object> value)
  {
    //
    //  get schema
    //

    Schema parameterExpressionSchema = schema.valueSchema();

    //
    //  unpack
    //

    Map<String,ParameterExpression> result = new HashMap<String,ParameterExpression>();
    for (String key : value.keySet())
      {
        result.put(key, ParameterExpression.unpack(new SchemaAndValue(parameterExpressionSchema, value.get(key))));
      }

    //
    //  return
    //

    return result;
  }
}
