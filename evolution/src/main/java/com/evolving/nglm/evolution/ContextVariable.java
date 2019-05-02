/*****************************************
*
*  ContextVariable.java
*
*****************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionDataType;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionException;
import com.evolving.nglm.evolution.EvaluationCriterion.TimeUnit;
import com.evolving.nglm.evolution.Expression.ExpressionDataType;
import com.evolving.nglm.evolution.Expression.ExpressionEvaluationException;
import com.evolving.nglm.evolution.Expression.ExpressionParseException;
import com.evolving.nglm.evolution.Expression.ExpressionReader;
import com.evolving.nglm.evolution.Expression.ExpressionTypeCheckException;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import org.json.simple.JSONObject;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ContextVariable
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
    schemaBuilder.name("context_variable");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("id", Schema.STRING_SCHEMA);
    schemaBuilder.field("name", Schema.STRING_SCHEMA);
    schemaBuilder.field("criterionContext", CriterionContext.schema());
    schemaBuilder.field("expressionString", Schema.STRING_SCHEMA);
    schemaBuilder.field("baseTimeUnit", Schema.STRING_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<ContextVariable> serde = new ConnectSerde<ContextVariable>(schema, false, ContextVariable.class, ContextVariable::pack, ContextVariable::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<ContextVariable> serde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private String id;
  private String name;
  private CriterionContext criterionContext;
  private String expressionString;
  private TimeUnit baseTimeUnit;

  //
  //  derived
  //

  private boolean validated;
  private Expression expression;
  private CriterionDataType type;

  /*****************************************
  *
  *  constructor -- simple
  *
  *****************************************/

  private ContextVariable(String id, String name, String expressionString, TimeUnit baseTimeUnit)
  {
    this.id = id;
    this.name = name;
    this.criterionContext = null;
    this.expressionString = expressionString;
    this.baseTimeUnit = baseTimeUnit;
    this.validated = false;
    this.expression = null;
    this.type = CriterionDataType.Unknown;
  }

  /*****************************************
  *
  *  constructor -- external JSON
  *
  *****************************************/

  public ContextVariable(JSONObject jsonRoot) throws GUIManagerException
  {
    this.name = JSONUtilities.decodeString(jsonRoot, "name", true);
    this.id  = generateID(this.name);
    this.criterionContext = null;
    JSONObject jsonValue = JSONUtilities.decodeJSONObject(jsonRoot, "value", false);
    this.expressionString = (jsonValue != null) ? JSONUtilities.decodeString(jsonValue, "expression", false) : null;
    this.baseTimeUnit = (jsonValue != null) ? TimeUnit.fromExternalRepresentation(JSONUtilities.decodeString(jsonValue, "timeUnit", "(unknown)")) : TimeUnit.Unknown;
    this.validated = false;
    this.expression = null;
    this.type = CriterionDataType.Unknown;
  }

  /*****************************************
  *
  *  accessors
  *
  *****************************************/


  public String getID() { return id; }
  public String getName() { return name; }
  public CriterionContext getCriterionContext() { return criterionContext; }
  public String getExpressionString() { return expressionString; }
  public TimeUnit getBaseTimeUnit() { return baseTimeUnit; }
  public boolean getValidated() { return validated; }
  public Expression getExpression() { return expression; }
  public CriterionDataType getType() { return type; }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    //
    //  sanity test
    //

    ContextVariable contextVariable = (ContextVariable) value;
    if (! contextVariable.getValidated()) throw new ServerRuntimeException("unvalidated ContextVariable");

    //
    //  pack
    //

    Struct struct = new Struct(schema);
    struct.put("id", contextVariable.getID());
    struct.put("name", contextVariable.getName());
    struct.put("criterionContext", CriterionContext.pack(contextVariable.getCriterionContext()));
    struct.put("expressionString", contextVariable.getExpressionString());
    struct.put("baseTimeUnit", contextVariable.getBaseTimeUnit().getExternalRepresentation());
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static ContextVariable unpack(SchemaAndValue schemaAndValue)
  {
    //
    //  data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;

    //
    //  unpack all but expression
    //

    Struct valueStruct = (Struct) value;
    String id = valueStruct.getString("id");
    String name = valueStruct.getString("name");
    CriterionContext criterionContext = CriterionContext.unpack(new SchemaAndValue(schema.field("criterionContext").schema(), valueStruct.get("criterionContext")));
    String expressionString = valueStruct.getString("expressionString");
    TimeUnit baseTimeUnit = TimeUnit.fromExternalRepresentation(valueStruct.getString("baseTimeUnit"));

    //
    //  construct 
    //

    ContextVariable result = new ContextVariable(id, name, expressionString, baseTimeUnit);

    //
    //  validate
    //

    try
      {
        result.validate(criterionContext);
      }
    catch (GUIManagerException e)
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
  *  generateID
  *
  *****************************************/

  private String generateID(String name)
  {
    //
    //  initialize result
    //
    
    StringBuilder result = new StringBuilder();
    result.append("variable");

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
  *  validate
  *
  *****************************************/

  public void validate(CriterionContext criterionContext) throws GUIManagerException
  {
    try
      {
        //
        //  parse
        //

        ExpressionReader expressionReader = new ExpressionReader(criterionContext, this.expressionString, this.baseTimeUnit);
        Expression expression = expressionReader.parse();
        if (expression == null) throw new GUIManagerException("no expression", this.expressionString);

        //
        //  validate type
        //

        switch (expression.getType())
          {
            case IntegerExpression:
              this.type = CriterionDataType.IntegerCriterion;
              break;
            case DoubleExpression:
              this.type = CriterionDataType.DoubleCriterion;
              break;
            case StringExpression:
              this.type = CriterionDataType.StringCriterion;
              break;
            case BooleanExpression:
              this.type = CriterionDataType.BooleanCriterion;
              break;
            case DateExpression:
              this.type = CriterionDataType.DateCriterion;
              break;
            case StringSetExpression:
              this.type = CriterionDataType.StringSetCriterion;
              break;
            default:
              throw new GUIManagerException("unsupported context variable type", expression.getType().toString());
          }

        //
        //  set validated fields
        //

        this.expression = expression;
        this.criterionContext = criterionContext;
        this.validated = true;
      }
    catch (ExpressionParseException|ExpressionTypeCheckException e)
      {
        throw new GUIManagerException(e);
      }
  }
}
