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
import com.evolving.nglm.evolution.CriterionField.VariableType;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionDataType;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionException;
import com.evolving.nglm.evolution.EvolutionUtilities.TimeUnit;
import com.evolving.nglm.evolution.Expression.ExpressionContext;
import com.evolving.nglm.evolution.Expression.ExpressionDataType;
import com.evolving.nglm.evolution.Expression.ExpressionEvaluationException;
import com.evolving.nglm.evolution.Expression.ExpressionParseException;
import com.evolving.nglm.evolution.Expression.ExpressionReader;
import com.evolving.nglm.evolution.Expression.ExpressionTypeCheckException;
import com.evolving.nglm.evolution.Expression.ReferenceExpression;
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
  *  enum
  *
  *****************************************/

  public enum Assignment
  {
    Direct("="),
    Increment("+="),
    Round("round"),
    RoundUp("roundUp"),
    RoundDown("roundDown"),
    DaysUntil("daysUntil"),
    MonthsUntil("monthsUntil"),
    DaysSince("daysSince"),
    MonthsSince("monthsSince"),
    FirstWord("firstWord"),
    SecondWord("secondWord"),
    ThirdWord("thirdWord"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private Assignment(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static Assignment fromExternalRepresentation(String externalRepresentation) { for (Assignment enumeratedValue : Assignment.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }

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
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(3));
    schemaBuilder.field("id", Schema.STRING_SCHEMA);
    schemaBuilder.field("name", Schema.STRING_SCHEMA);
    schemaBuilder.field("criterionContext", CriterionContext.schema());
    schemaBuilder.field("expressionString", Schema.STRING_SCHEMA);
    schemaBuilder.field("assignment", SchemaBuilder.string().defaultValue("=").schema());
    schemaBuilder.field("baseTimeUnit", Schema.STRING_SCHEMA);
    schemaBuilder.field("variableType", SchemaBuilder.string().defaultValue("local").schema());
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
  private Assignment assignment;
  private TimeUnit baseTimeUnit;
  private VariableType variableType;

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

  private ContextVariable(String id, String name, String expressionString, Assignment assignment, TimeUnit baseTimeUnit, VariableType variableType)
  {
    this.id = id;
    this.name = name;
    this.criterionContext = null;
    this.expressionString = expressionString;
    this.assignment = assignment;
    this.baseTimeUnit = baseTimeUnit;
    this.variableType = variableType;
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
    this.criterionContext = null;
    JSONObject jsonValue = JSONUtilities.decodeJSONObject(jsonRoot, "value", false);
    this.expressionString = (jsonValue != null) ? JSONUtilities.decodeString(jsonValue, "expression", false) : null;
    this.assignment = (jsonValue != null) ? Assignment.fromExternalRepresentation(JSONUtilities.decodeString(jsonValue, "assignment", "=")) : Assignment.Direct;
    this.baseTimeUnit = (jsonValue != null) ? TimeUnit.fromExternalRepresentation(JSONUtilities.decodeString(jsonValue, "timeUnit", "(unknown)")) : TimeUnit.Unknown;
    this.variableType = (jsonValue != null) ? (JSONUtilities.decodeBoolean(jsonValue, "isParameter", Boolean.FALSE) ? VariableType.Parameter : VariableType.Local) : VariableType.Local;
    this.validated = false;
    this.expression = null;
    this.type = CriterionDataType.Unknown;
    String expressionTypeFromJSON = JSONUtilities.decodeString(jsonValue, "expressionType", false);
    if(expressionTypeFromJSON != null) {
      
      // gotten a expressionType from GUI with possible values: 
      //      integer
      //      double
      //      date
      //      time
      //      string
      
      this.type = CriterionDataType.fromExternalRepresentation(expressionTypeFromJSON);
    }
    
    this.id = generateID(this.name);
  }
  
  /*****************************************
  *
  *  constructor -- external JSON (file Variable)
  *
  *****************************************/

  public ContextVariable(JSONObject jsonRoot, boolean isFileVariable) throws GUIManagerException
  {
    this.name = JSONUtilities.decodeString(jsonRoot, "name", true);
    this.criterionContext = null;
    JSONObject jsonValue = JSONUtilities.decodeJSONObject(jsonRoot, "value", false);
    this.expressionString = (jsonValue != null) ? JSONUtilities.decodeString(jsonValue, "expression", false) : null;
    this.assignment = (jsonValue != null) ? Assignment.fromExternalRepresentation(JSONUtilities.decodeString(jsonValue, "assignment", "=")) : Assignment.Direct;
    this.baseTimeUnit = (jsonValue != null) ? TimeUnit.fromExternalRepresentation(JSONUtilities.decodeString(jsonValue, "timeUnit", "(unknown)")) : TimeUnit.Unknown;
    this.variableType = (jsonValue != null) ? (JSONUtilities.decodeBoolean(jsonValue, "isParameter", Boolean.FALSE) ? VariableType.Parameter : VariableType.Local) : VariableType.Local;
    this.validated = false;
    this.expression = null;
    this.type = CriterionDataType.Unknown;
    String expressionTypeFromJSON = JSONUtilities.decodeString(jsonValue, "expressionType", false);
    if(expressionTypeFromJSON != null) {
      
      // gotten a expressionType from GUI with possible values: 
      //      integer
      //      double
      //      date
      //      time
      //      string
      
      this.type = CriterionDataType.fromExternalRepresentation(expressionTypeFromJSON);
    }
    
    if (isFileVariable) this.id = "variablefile" + "." + name;
    else this.id = generateID(this.name);
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
  public Assignment getAssignment() { return assignment; }
  public TimeUnit getBaseTimeUnit() { return baseTimeUnit; }
  public VariableType getVariableType() { return variableType; }
  public boolean getValidated() { return validated; }
  public Expression getExpression() { return expression; }
  public CriterionDataType getType() { return type; }
  public String getTagFormat() { return (expression != null) ? expression.getEffectiveTagFormat() : null; }
  public Integer getTagMaxLength() { return (expression != null) ? expression.getEffectiveTagMaxLength() : null; }

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
    struct.put("assignment", contextVariable.getAssignment().getExternalRepresentation());
    struct.put("baseTimeUnit", contextVariable.getBaseTimeUnit().getExternalRepresentation());
    struct.put("variableType", contextVariable.getVariableType().getExternalRepresentation());
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
    Assignment assignment = (schemaVersion >= 2) ? Assignment.fromExternalRepresentation(valueStruct.getString("assignment")) : Assignment.Direct;
    TimeUnit baseTimeUnit = TimeUnit.fromExternalRepresentation(valueStruct.getString("baseTimeUnit"));
    VariableType variableType = (schemaVersion >= 3) ? VariableType.fromExternalRepresentation(valueStruct.getString("variableType")) : VariableType.Local;

    //
    //  construct 
    //

    ContextVariable result = new ContextVariable(id, name, expressionString, assignment, baseTimeUnit, variableType);

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
    switch (variableType)
      {
        case Local:
        case JourneyResult:
          result.append("variable");
          break;
        case Parameter:
          result.append("workflow.parameter");
          break;
      }

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

  public void validate(CriterionContext nodeOnlyCriterionContext, CriterionContext nodeWithJourneyResultCriterionContext) throws GUIManagerException
  {
    switch (variableType)
      {
        case Local:
        case JourneyResult:

          /*****************************************
          *
          *  validate -- nodeOnly context
          *
          *****************************************/

          boolean validated = false;
          try
            {
              validate(nodeOnlyCriterionContext);
              variableType = VariableType.Local;
              validated = true;
            }
          catch (GUIManagerException e)
            {
              //
              //  igore
              //
            }

          /*****************************************
          *
          *  try again with journey result context (if necessary)
          *
          *****************************************/

          if (! validated)
            {
              try
                {
                  validate(nodeWithJourneyResultCriterionContext);
                  variableType = VariableType.JourneyResult;
                }
              catch (GUIManagerException e)
                {
                  throw e;
                }
            }

          /*****************************************
          *
          *  break
          *
          *****************************************/

          break;

        case Parameter:
          
          /*****************************************
          *
          *  validate -- nodeOnly context
          *
          *****************************************/

          validate(nodeOnlyCriterionContext);
          break;
      }
  }

  /*****************************************
  *
  *  validate
  *
  *****************************************/

  private void validate(CriterionContext criterionContext) throws GUIManagerException
  {
    switch (variableType)
      {
        case Local:
        case JourneyResult:
          try
            {
              //
              //  assignment
              //

              String expressionString = this.expressionString;
              switch (assignment)
                {
                  case Increment:
                    expressionString = this.getID() + "+" + "(" + expressionString + ")";
                    break;
                  case Round:
                  case RoundUp:
                  case RoundDown:
                  case DaysUntil:
                  case MonthsUntil:
                  case DaysSince:
                  case MonthsSince:
                  case FirstWord:
                  case SecondWord:
                  case ThirdWord:
                    break;
                  default:
                    // NO-OP
                    break;
                }

              //
              //  parse expression
              //

              ExpressionReader expressionReader = new ExpressionReader(criterionContext, expressionString, this.baseTimeUnit);
              Expression expression = expressionReader.parse(ExpressionContext.ContextVariable);
              if (expression == null) throw new GUIManagerException("no expression", expressionString);

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
                  case OpaqueReferenceExpression:
                    this.type = ((ReferenceExpression) expression).getCriterionDataType();
                    break;
                    
                  case TimeExpression:
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
          break;

        case Parameter:
          {
            //
            //  parse expression
            //

            ExpressionReader expressionReader = new ExpressionReader(criterionContext, expressionString, this.baseTimeUnit);
            Expression expression = expressionReader.parse(ExpressionContext.ContextVariable);
            if (expression == null) throw new GUIManagerException("no expression", expressionString);

            //
            //  validate type
            //

            if (expression.isConstant() && expression.evaluateConstant() instanceof String)
              {
                this.type = CriterionDataType.fromExternalRepresentation((String) (expression.evaluateConstant()));
                switch (this.type)
                  {
                    case IntegerCriterion:
                    case DoubleCriterion:
                    case StringCriterion:
                    case BooleanCriterion:
                    case DateCriterion:
                      break;

                    default:
                      throw new GUIManagerException("unsupported context variable parameter type", (String) expression.evaluateConstant());
                  }
              }
            else
              {
                throw new GUIManagerException("malformed context variable parameter", "(n/a)");
              }

            //
            //  set validated fields
            //

            this.expression = expression;
            this.criterionContext = criterionContext;
            this.validated = true;
          }
          break;
      }
  }
}
