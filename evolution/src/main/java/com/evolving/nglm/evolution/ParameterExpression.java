/*****************************************
*
*  ParameterExpression.java
*
*****************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionException;
import com.evolving.nglm.evolution.EvolutionUtilities.TimeUnit;
import com.evolving.nglm.evolution.Expression.ExpressionContext;
import com.evolving.nglm.evolution.Expression.ExpressionDataType;
import com.evolving.nglm.evolution.Expression.ExpressionEvaluationException;
import com.evolving.nglm.evolution.Expression.ExpressionParseException;
import com.evolving.nglm.evolution.Expression.ExpressionReader;
import com.evolving.nglm.evolution.Expression.ExpressionTypeCheckException;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import kafka.log.Log;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParameterExpression
{
  
  //
  //  logger
  //

  protected static final Logger log = LoggerFactory.getLogger(ParameterExpression.class);
  
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
    schemaBuilder.name("parameter_expression");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(2));
    schemaBuilder.field("criterionContext", CriterionContext.schema());
    schemaBuilder.field("expressionString", Schema.STRING_SCHEMA);
    schemaBuilder.field("baseTimeUnit", Schema.STRING_SCHEMA);
    schemaBuilder.field("tenantID", Schema.INT16_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<ParameterExpression> serde = new ConnectSerde<ParameterExpression>(schema, false, ParameterExpression.class, ParameterExpression::pack, ParameterExpression::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<ParameterExpression> serde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private CriterionContext criterionContext;
  private String expressionString;
  private TimeUnit baseTimeUnit;
  private int tenantID;

  //
  //  derived
  //

  private Expression expression;

  /*****************************************
  *
  *  constructor -- simple
  *
  *****************************************/

  private ParameterExpression(CriterionContext criterionContext, String expressionString, TimeUnit baseTimeUnit, int tenantID)
  {
    this.criterionContext = criterionContext;
    this.expressionString = expressionString;
    this.baseTimeUnit = baseTimeUnit;
    this.tenantID = tenantID;
  }

  /*****************************************
  *
  *  constructor -- external JSON
  *
  *****************************************/

  public ParameterExpression(JSONObject jsonRoot, CriterionContext criterionContext, int tenantID) throws GUIManagerException
  {
    //
    //  basic fields
    //

    this.criterionContext = criterionContext;
    this.expressionString = JSONUtilities.decodeString(jsonRoot, "expression", false);
    this.baseTimeUnit = TimeUnit.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "timeUnit", "(unknown)"));
    this.tenantID = tenantID;

    //
    //  parse
    //

    try
      {
        parseParameterExpression(tenantID);
      }
    catch (ExpressionParseException|ExpressionTypeCheckException e)
      {
        throw new GUIManagerException(e);
      }
  }
  
  public ParameterExpression(String expression, String timeUnit, CriterionContext criterionContext, int tenantID) throws GUIManagerException
  {
    //
    //  basic fields
    //

    this.criterionContext = criterionContext;
    this.expressionString = expression;
    this.baseTimeUnit = TimeUnit.fromExternalRepresentation(timeUnit != null ? timeUnit : "(unknown)");
    this.tenantID = tenantID;

    //
    //  parse
    //

    try
      {
        parseParameterExpression(tenantID);
      }
    catch (ExpressionParseException|ExpressionTypeCheckException e)
      {
        throw new GUIManagerException(e);
      }
  }

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public CriterionContext getCriterionContext() { return criterionContext; }
  public String getExpressionString() { return expressionString; }
  public TimeUnit getBaseTimeUnit() { return baseTimeUnit; }
  public Expression getExpression() { return expression; }
  public ExpressionDataType getType() { return expression.getType(); }
  public int getTenantID() { return tenantID; }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    ParameterExpression parameterExpression = (ParameterExpression) value;
    Struct struct = new Struct(schema);
    struct.put("criterionContext", CriterionContext.pack(parameterExpression.getCriterionContext()));
    struct.put("expressionString", parameterExpression.getExpressionString());
    struct.put("baseTimeUnit", parameterExpression.getBaseTimeUnit().getExternalRepresentation());
    struct.put("tenantID", (short)parameterExpression.getTenantID());
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static ParameterExpression unpack(SchemaAndValue schemaAndValue)
  {
    //
    //  data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;

    //
    //  unpack all but argument
    //

    Struct valueStruct = (Struct) value;
    CriterionContext criterionContext = CriterionContext.unpack(new SchemaAndValue(schema.field("criterionContext").schema(), valueStruct.get("criterionContext")));
    String expressionString = valueStruct.getString("expressionString");
    TimeUnit baseTimeUnit = TimeUnit.fromExternalRepresentation(valueStruct.getString("baseTimeUnit"));
    int tenantID = schema.field("tenantID") != null ? valueStruct.getInt16("tenantID") : 1; // by default tenantID = 1

    //
    //  construct 
    //

    ParameterExpression result = new ParameterExpression(criterionContext, expressionString, baseTimeUnit, tenantID);

    //
    //  parse
    //

    try
      {
        result.parseParameterExpression(tenantID);
      }
    catch (ExpressionParseException|ExpressionTypeCheckException e)
      {
        throw new SerializationException("invalid parameter expression " + expressionString, e);
      }

    //
    //  return
    //

    return result;
  }

  /*****************************************
  *
  *  parseParameterExpression
  *
  *****************************************/

  public void parseParameterExpression(int tenantID) throws ExpressionParseException, ExpressionTypeCheckException
  {
    ExpressionReader expressionReader = new ExpressionReader(criterionContext, expressionString, baseTimeUnit, tenantID);
    expression = expressionReader.parse(ExpressionContext.Parameter, tenantID);
  }
}
