/*****************************************************************************
*
*  Expression.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.EvaluationCriterion.CriterionDataType;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionException;
import com.evolving.nglm.evolution.EvolutionUtilities.TimeUnit;
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

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;

/*****************************************
*
*  class Expression
*
*****************************************/

public abstract class Expression
{
  /*****************************************************************************
  *
  *  enum
  *
  *****************************************************************************/

  //
  //  enum ExpressionContext
  //

  public enum ExpressionContext
  {
    Criterion,
    Parameter,
    ContextVariable
  }

  //
  //  enum Token
  //

  private enum Token
  {
    //
    //  identifiers
    //

    IDENTIFIER,
    FUNCTION_CALL,

    //
    //  constant literals
    //

    INTEGER,
    DOUBLE,
    STRING,
    BOOLEAN,

    //
    //  operators
    //

    PLUS,
    MINUS,
    MULTIPLY,
    DIVIDE,

    //
    //  syntax
    //

    COMMA,
    LEFT_PAREN,
    RIGHT_PAREN,
    LEFT_BRACKET,
    RIGHT_BRACKET,
    INVALID_CHAR,
    INVALID_IDENTIFIER,
    END_OF_INPUT
  }

  //
  //  ExpressionDataType
  //

  public enum ExpressionDataType
  {
    IntegerExpression,
    DoubleExpression,
    StringExpression,
    BooleanExpression,
    DateExpression,
    IntegerSetExpression,
    StringSetExpression,
    EmptySetExpression,
    OpaqueReferenceExpression,
    NoArgument;
  }

  //
  //  ExpressionOperator
  //

  public enum ExpressionOperator
  {
    PlusOperator(Token.PLUS),
    MinusOperator(Token.MINUS),
    MultiplyOperator(Token.MULTIPLY),
    DivideOperator(Token.DIVIDE),
    UnknownOperator(Token.INVALID_CHAR);
    private Token operatorName;
    private ExpressionOperator(Token operatorName) { this.operatorName = operatorName; }
    public Token getOperatorName() { return operatorName; }
    public static ExpressionOperator fromOperatorName(Token operatorName) { for (ExpressionOperator enumeratedValue : ExpressionOperator.values()) { if (enumeratedValue.getOperatorName() == operatorName) return enumeratedValue; } return UnknownOperator; }
  }
  
  //
  //  ExpressionFunction
  //

  public enum ExpressionFunction
  {
    DateConstantFunction("dateConstant"),
    DateAddFunction("dateAdd"),
    UnknownFunction("(unknown)");
    private String functionName;
    private ExpressionFunction(String functionName) { this.functionName = functionName; }
    public String getFunctionName() { return functionName; }
    public static ExpressionFunction fromFunctionName(String functionName) { for (ExpressionFunction enumeratedValue : ExpressionFunction.values()) { if (enumeratedValue.getFunctionName().equalsIgnoreCase(functionName)) return enumeratedValue; } return UnknownFunction; }
  }
  
  /*****************************************
  *
  *  data
  *
  *****************************************/

  protected ExpressionDataType type;
  protected String nodeID;
  protected String tagFormat;
  protected Integer tagMaxLength;

  /*****************************************
  *
  *  abstract
  *
  *****************************************/

  public abstract void typeCheck(ExpressionContext expressionContext, TimeUnit baseTimeUnit);
  public abstract int assignNodeID(int preorderNumber);
  public boolean isConstant() { return false; }
  protected abstract Object evaluate(SubscriberEvaluationRequest subscriberEvaluationRequest, TimeUnit baseTimeUnit);
  public abstract void esQuery(StringBuilder script, TimeUnit baseTimeUnit) throws CriterionException;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public ExpressionDataType getType() { return type; }
  public String getNodeID() { return nodeID; }
  public String getTagFormat() { return tagFormat; }
  public Integer getTagMaxLength() { return tagMaxLength; }
  public String getEffectiveTagFormat() { return (tagFormat != errorTagFormat) ? tagFormat : null; }
  public Integer getEffectiveTagMaxLength() { return (tagMaxLength != errorTagMaxLength) ? tagMaxLength : null; }

  /*****************************************
  *
  *  setters
  *
  *****************************************/

  public void setType(ExpressionDataType type) { this.type = type; }
  public void setNodeID(int preorderNumber) { this.nodeID = Integer.toString(preorderNumber); }
  public void setTagFormat(String tagFormat) { this.tagFormat = tagFormat; }
  public void setTagMaxLength(Integer tagMaxLength) { this.tagMaxLength = tagMaxLength; }

  /*****************************************
  *
  *  evaluateExpression
  *
  *****************************************/

  public Object evaluateExpression(SubscriberEvaluationRequest subscriberEvaluationRequest, TimeUnit baseTimeUnit)
  {
    Object result;
    try
      {
        result = evaluate(subscriberEvaluationRequest, baseTimeUnit);
      }
    catch (ExpressionNullException e)
      {
        result = null;
      }
    return result;
  }

  /*****************************************
  *
  *  errorConstants
  *
  *****************************************/

  private static String errorTagFormat = new String("(error)");
  private static Integer errorTagMaxLength = new Integer(0);

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  protected Expression()
  {
    this.type = null;
    this.nodeID = null;
    this.tagFormat = null;
    this.tagMaxLength = null;
  }

  /*****************************************
  *
  *  class ConstantExpression
  *
  *****************************************/

  public static class ConstantExpression extends Expression
  {
    /*****************************************
    *
    *  data
    *
    *****************************************/

    private Object constant;

    /*****************************************
    *
    *  typeCheck
    *
    *****************************************/

    @Override public void typeCheck(ExpressionContext expressionContext, TimeUnit baseTimeUnit) { }

    /*****************************************
    *
    *  assignNodeID
    *
    *****************************************/

    @Override public int assignNodeID(int preorderNumber)
    {
      setNodeID(preorderNumber);
      return preorderNumber;
    }

    /*****************************************
    *
    *  isConstant
    *
    *****************************************/

    @Override public boolean isConstant() { return true; }

    /*****************************************
    *
    *  evaluate
    *
    *****************************************/

    @Override protected Object evaluate(SubscriberEvaluationRequest subscriberEvaluationRequest, TimeUnit baseTimeUnit)
    {
      return constant;
    }

    /*****************************************
    *
    *  esQuery
    *
    *****************************************/

    @Override public void esQuery(StringBuilder script, TimeUnit baseTimeUnit) throws CriterionException
    {
      switch (getType())
        {
          case IntegerExpression:
            script.append("def right_" + getNodeID() + " = " + ((Number) constant).toString() + "; ");
            break;

          case DoubleExpression:
            script.append("def right_" + getNodeID() + " = " + ((Double) constant).toString() + "; ");
            break;

          case StringExpression:
            script.append("def right_" + getNodeID() + " = '" + ((String) constant) + "'; ");
            break;

          case BooleanExpression:
            script.append("def right_" + getNodeID() + " = " + ((Boolean) constant).toString() + "; ");
            break;

          case DateExpression:
            DateFormat scriptDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
            script.append("def rightSF_" + getNodeID() + " = new SimpleDateFormat(\"yyyy-MM-dd'T'HH:mm:ss.SSSX\"); ");
            script.append("def rightDT_" + getNodeID() + " = rightSF_" + getNodeID() + ".parse(\"" + scriptDateFormat.format((Date) constant) + "\"); ");
            script.append("def rightCalendar_" + getNodeID() + " = rightSF_" + getNodeID() + ".getCalendar(); ");
            script.append("rightCalendar_" + getNodeID() + ".setTime(rightDT_" + getNodeID() + "); ");
            script.append("def rightInstant_" + getNodeID() + " = rightCalendar_" + getNodeID() + ".toInstant(); ");
            script.append("def right_" + getNodeID() + " = LocalDateTime.ofInstant(rightInstant_" + getNodeID() + ", ZoneOffset.UTC); ");
            break;

          case StringSetExpression:
            script.append("ArrayList right_" + getNodeID() + " = new ArrayList(); ");
            for (Object item : (Set<Object>) constant) script.append("right_" + getNodeID() + ".add(\"" + item.toString() + "\"); ");
            break;
            
          case IntegerSetExpression:
            script.append("ArrayList right_" + getNodeID() + " = new ArrayList(); ");
            for (Object item : (Set<Object>) constant) script.append("right_" + getNodeID() + ".add(" + item.toString() + "); ");
            break;

          case EmptySetExpression:
            script.append("def right_" + getNodeID() + " = new ArrayList(); ");
            break;

          default:
            throw new CriterionException("invalid criterionField datatype for esQuery");
        }
    }

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public ConstantExpression(ExpressionDataType type, Object constant)
    {
      super();
      this.constant = constant;
      this.type = type;
    }
  }

  /*****************************************
  *
  *  class ReferenceExpression
  *
  *****************************************/

  public static class ReferenceExpression extends Expression
  {
    /*****************************************
    *
    *  data
    *
    *****************************************/

    private CriterionField reference;

    /*****************************************
    *
    *  typeCheck
    *
    *****************************************/

    @Override public void typeCheck(ExpressionContext expressionContext, TimeUnit baseTimeUnit)
    {
      //
      //  type
      //

      switch (reference.getFieldDataType())
        {
          case IntegerCriterion:
            setType(ExpressionDataType.IntegerExpression);
            break;
          case DoubleCriterion:
            setType(ExpressionDataType.DoubleExpression);
            break;
          case StringCriterion:
            setType(ExpressionDataType.StringExpression);
            break;
          case BooleanCriterion:
            setType(ExpressionDataType.BooleanExpression);
            break;
          case DateCriterion:
            setType(ExpressionDataType.DateExpression);
            break;
          case StringSetCriterion:
            setType(ExpressionDataType.StringSetExpression);
            break;
          case EvaluationCriteriaParameter:
          case SMSMessageParameter:
          case EmailMessageParameter:
          case PushMessageParameter:
            setType(ExpressionDataType.OpaqueReferenceExpression);
            break;

          default:
            throw new ExpressionTypeCheckException("invariant violated");
        }

      //
      //  evaluation.date -- illegal
      //

      switch (expressionContext)
        {
          case Criterion:
            if (reference.getID().equals(CriterionField.EvaluationDateField))
              {
                throw new ExpressionTypeCheckException("illegal reference to " + CriterionField.EvaluationDateField);
              }
            break;
        }

      //
      //  tagFormat/tagMaxLength
      //

      setTagFormat(reference.getTagFormat());
      setTagMaxLength(reference.getTagMaxLength());
    }

    /*****************************************
    *
    *  getCriterionDataType
    *
    *****************************************/

    public CriterionDataType getCriterionDataType() { return reference.getFieldDataType(); }

    /*****************************************
    *
    *  assignNodeID
    *
    *****************************************/

    @Override public int assignNodeID(int preorderNumber)
    {
      setNodeID(preorderNumber);
      return preorderNumber;
    }

    /*****************************************
    *
    *  evaluate
    *
    *****************************************/

    @Override protected Object evaluate(SubscriberEvaluationRequest subscriberEvaluationRequest, TimeUnit baseTimeUnit)
    {
      //
      //  retrieve
      //

      Object referenceValue = reference.retrieve(subscriberEvaluationRequest);
      
      //
      //  null check
      //

      if (referenceValue == null) throw new ExpressionNullException(reference);

      //
      //  normalize
      //

      switch (type)
        {
          case DateExpression:
            switch (baseTimeUnit)
              {
                case Instant:
                  break;
                case Minute:
                  referenceValue = RLMDateUtils.truncate((Date) referenceValue, Calendar.MINUTE, Calendar.SUNDAY, Deployment.getBaseTimeZone());
                  break;
                case Hour:
                  referenceValue = RLMDateUtils.truncate((Date) referenceValue, Calendar.HOUR, Calendar.SUNDAY, Deployment.getBaseTimeZone());
                  break;
                case Day:
                  referenceValue = RLMDateUtils.truncate((Date) referenceValue, Calendar.DATE, Calendar.SUNDAY, Deployment.getBaseTimeZone());
                  break;
                case Week:
                  referenceValue = RLMDateUtils.truncate((Date) referenceValue, Calendar.DAY_OF_WEEK, Calendar.SUNDAY, Deployment.getBaseTimeZone());
                  break;
                case Month:
                  referenceValue = RLMDateUtils.truncate((Date) referenceValue, Calendar.MONTH, Calendar.SUNDAY, Deployment.getBaseTimeZone());
                  break;
                case Year:
                  referenceValue = RLMDateUtils.truncate((Date) referenceValue, Calendar.YEAR, Calendar.SUNDAY, Deployment.getBaseTimeZone());
                  break;
              }
            break;
        }

      //
      //  return
      //
      
      return referenceValue;
    }

    /*****************************************
    *
    *  esQuery
    *
    *****************************************/

    @Override public void esQuery(StringBuilder script, TimeUnit baseTimeUnit) throws CriterionException
    {
      /*****************************************
      *
      *  esField
      *
      *****************************************/

      String esField = reference.getESField();
      if (esField == null)
        {
          throw new CriterionException("invalid criterionField " + reference);
        }

      /*****************************************
      *
      *  script
      *
      *****************************************/

      switch (getType())
        {
          case StringExpression:
          case IntegerExpression:
          case DoubleExpression:
          case BooleanExpression:
  	    script.append("def right_" + getNodeID() + " = (doc." + esField + ".size() != 0) ? doc." + esField + "?.value : null; ");
            break;
            
          case StringSetExpression:
          case IntegerSetExpression:
  	    script.append("def right_" + getNodeID() + " = new ArrayList(); right_" + getNodeID() + ".addAll(doc." + esField + "); ");
            break;
            
          case DateExpression:
            script.append("def right_" + getNodeID() + "; ");
            script.append("if (doc." + esField + ".size() != 0) { ");
            script.append("def rightSF_" + getNodeID() + " = new SimpleDateFormat(\"yyyy-MM-dd'T'HH:mm:ss.SSSX\"); ");
            script.append("def rightMillis_" + getNodeID() + " = doc." + esField + ".value.getMillis(); ");
            script.append("def rightCalendar_" + getNodeID() +" = rightSF_" + getNodeID() + ".getCalendar(); ");
            script.append("rightCalendar_" + getNodeID() + ".setTimeInMillis(rightMillis_" + getNodeID() + "); ");
            script.append("def rightInstant_" + getNodeID() + " = rightCalendar_" + getNodeID() + ".toInstant(); ");
            script.append("def rightRaw_" + getNodeID() + " = LocalDateTime.ofInstant(rightInstant_" + getNodeID() + ", ZoneOffset.UTC); ");
            script.append(EvaluationCriterion.constructDateTruncateESScript(getNodeID(), "rightRaw", "tempRight", baseTimeUnit));
            script.append("right_" + getNodeID() + " =  tempRight; } ");
            break;

          default:
            throw new CriterionException("invalid criterionField datatype for esQuery");
        }
    }

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public ReferenceExpression(CriterionField reference)
    {
      super();
      this.reference = reference;
    }
  }

  /*****************************************
  *
  *  class OperatorExpression
  *
  *****************************************/

  public static class OperatorExpression extends Expression
  {
    /*****************************************
    *
    *  data
    *
    *****************************************/

    private ExpressionOperator operator;
    private Expression leftArgument;
    private Expression rightArgument;

    /*****************************************
    *
    *  typeCheck
    *
    *****************************************/

    @Override public void typeCheck(ExpressionContext expressionContext, TimeUnit baseTimeUnit)
    {
      /*****************************************
      *
      *  typeCheck arguments
      *
      *****************************************/

      leftArgument.typeCheck(expressionContext, baseTimeUnit);
      rightArgument.typeCheck(expressionContext, baseTimeUnit);

      /*****************************************
      *
      *  type
      *
      *****************************************/

      switch (leftArgument.getType())
        {
          case IntegerExpression:
          case DoubleExpression:
            switch (operator)
              {
                case PlusOperator:
                case MinusOperator:
                case MultiplyOperator:
                  switch (rightArgument.getType())
                    {
                      case IntegerExpression:
                      case DoubleExpression:
                        setType((leftArgument.getType() == ExpressionDataType.IntegerExpression && rightArgument.getType() == ExpressionDataType.IntegerExpression) ? ExpressionDataType.IntegerExpression : ExpressionDataType.DoubleExpression);
                        break;
                      default:
                        throw new ExpressionTypeCheckException("type exception");
                    }
                  break;

                case DivideOperator:
                  switch (rightArgument.getType())
                    {
                      case IntegerExpression:
                      case DoubleExpression:
                        setType(ExpressionDataType.DoubleExpression);
                        break;
                      default:
                        throw new ExpressionTypeCheckException("type exception");
                    }
                  break;

                default:
                  throw new ExpressionTypeCheckException("type exception");
              }
            break;
            
          case StringExpression:
            switch (operator)
              {
                case PlusOperator:
                  switch (rightArgument.getType())
                    {
                      case StringExpression:
                        setType(ExpressionDataType.StringExpression);
                        break;
                      default:
                        throw new ExpressionTypeCheckException("type exception");
                    }
                  break;
                  
                default:
                  throw new ExpressionTypeCheckException("type exception");
              }
            break;
            
          default:
            throw new ExpressionTypeCheckException("type exception");
        }

      //
      //  tagFormat
      //

      if (leftArgument.getTagFormat() == errorTagFormat || rightArgument.getTagFormat() == errorTagFormat)
        setTagFormat(errorTagFormat);
      else if (Objects.equals(leftArgument.getTagFormat(), rightArgument.getTagFormat()))
        setTagFormat(leftArgument.getTagFormat());
      else if (leftArgument.getTagFormat() == null)
        setTagFormat(rightArgument.getTagFormat());
      else if (rightArgument.getTagFormat() == null)
        setTagFormat(leftArgument.getTagFormat());
      else
        setTagFormat(errorTagFormat);

      //
      //  tagMaxLength
      //

      if (leftArgument.getTagMaxLength() == errorTagMaxLength || rightArgument.getTagMaxLength() == errorTagMaxLength)
        setTagMaxLength(errorTagMaxLength);
      else if (Objects.equals(leftArgument.getTagMaxLength(), rightArgument.getTagMaxLength()))
        setTagMaxLength(leftArgument.getTagMaxLength());
      else if (leftArgument.getTagMaxLength() == null)
        setTagMaxLength(rightArgument.getTagMaxLength());
      else if (rightArgument.getTagMaxLength() == null)
        setTagMaxLength(leftArgument.getTagMaxLength());
      else
        setTagMaxLength(errorTagMaxLength);
    }

    /*****************************************
    *
    *  assignNodeID
    *
    *****************************************/

    @Override public int assignNodeID(int preorderNumber)
    {
      setNodeID(preorderNumber);
      preorderNumber = leftArgument.assignNodeID(preorderNumber+1);
      preorderNumber = rightArgument.assignNodeID(preorderNumber+1);
      return preorderNumber;
    }

    /*****************************************
    *
    *  evaluate
    *
    *****************************************/

    @Override protected Object evaluate(SubscriberEvaluationRequest subscriberEvaluationRequest, TimeUnit baseTimeUnit)
    {
      /*****************************************
      *
      *  evaluate arguments
      *
      *****************************************/

      Object leftValue = leftArgument.evaluate(subscriberEvaluationRequest, baseTimeUnit);
      Object rightValue = rightArgument.evaluate(subscriberEvaluationRequest, baseTimeUnit);

      /*****************************************
      *
      *  evaluate operator
      *
      *****************************************/

      Object result = null;
      switch (type)
        {
          case IntegerExpression:
          case DoubleExpression:
            Number leftValueNumber = (Number) leftValue;
            Number rightValueNumber = (Number) rightValue;
            switch (operator)
              {
                case PlusOperator:
                  switch (type)
                    {
                      case IntegerExpression:
                        result = new Long(leftValueNumber.longValue() + rightValueNumber.longValue());
                        break;
                      case DoubleExpression:
                        result = new Double(leftValueNumber.doubleValue() + rightValueNumber.doubleValue());
                        break;
                    }
                  break; 
                  
                case MinusOperator:
                  switch (type)
                    {
                      case IntegerExpression:
                        result = new Long(leftValueNumber.longValue() - rightValueNumber.longValue());
                        break;
                      case DoubleExpression:
                        result = new Double(leftValueNumber.doubleValue() - rightValueNumber.doubleValue());
                        break;
                    }
                  break;

                case MultiplyOperator:
                  switch (type)
                    {
                      case IntegerExpression:
                        result = new Long(leftValueNumber.longValue() * rightValueNumber.longValue());
                        break;
                      case DoubleExpression:
                        result = new Double(leftValueNumber.doubleValue() * rightValueNumber.doubleValue());
                        break;
                    }
                  break;

                case DivideOperator:
                  result = new Double(leftValueNumber.doubleValue() / rightValueNumber.doubleValue());
                  break;
              }
            break;

          case StringExpression:
            String leftValueString = (String) leftValue;
            String rightValueString = (String) rightValue;
            result = leftValueString + rightValueString;
            break;
        }

      /*****************************************
      *
      *  return
      *
      *****************************************/

      return result;
    }

    /*****************************************
    *
    *  esQuery
    *
    *****************************************/

    @Override public void esQuery(StringBuilder script, TimeUnit baseTimeUnit) throws CriterionException
    {
      /*****************************************
      *
      *  script
      *
      *****************************************/

      //
      //  arguments
      //
      
      leftArgument.esQuery(script, baseTimeUnit);
      rightArgument.esQuery(script, baseTimeUnit);

      //
      //  operator
      //
      
      switch (operator)
        {
          case PlusOperator:
            script.append("def right_" + getNodeID() + " = right_" + leftArgument.getNodeID() + " + right_" + rightArgument.getNodeID() + "; ");
            break; 

          case MinusOperator:
            script.append("def right_" + getNodeID() + " = right_" + leftArgument.getNodeID() + " - right_" + rightArgument.getNodeID() + "; ");
            break;

          case MultiplyOperator:
            script.append("def right_" + getNodeID() + " = right_" + leftArgument.getNodeID() + " * right_" + rightArgument.getNodeID() + "; ");
            break;

          case DivideOperator:
            script.append("def right_" + getNodeID() + " = right_" + leftArgument.getNodeID() + " / right_" + rightArgument.getNodeID() + "; ");
            break;
        }
    }
    
    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public OperatorExpression(ExpressionOperator operator, Expression leftArgument, Expression rightArgument)
    {
      super();
      this.operator = operator;
      this.leftArgument = leftArgument;
      this.rightArgument = rightArgument;
    }
  }

  /*****************************************
  *
  *  class UnaryExpression
  *
  *****************************************/

  public static class UnaryExpression extends Expression
  {
    /*****************************************
    *
    *  data
    *
    *****************************************/

    private ExpressionOperator operator;
    private Expression unaryArgument;

    /*****************************************
    *
    *  typeCheck
    *
    *****************************************/

    @Override public void typeCheck(ExpressionContext expressionContext, TimeUnit baseTimeUnit)
    {
      /*****************************************
      *
      *  typeCheck arguments
      *
      *****************************************/

      unaryArgument.typeCheck(expressionContext, baseTimeUnit);

      /*****************************************
      *
      *  type
      *
      *****************************************/

      switch (unaryArgument.getType())
        {
          case IntegerExpression:
          case DoubleExpression:
            switch (operator)
              {
                case PlusOperator:
                case MinusOperator:
                  setType(unaryArgument.getType());
                  break;
                  
                default:
                  throw new ExpressionTypeCheckException("type exception");
              }
            break;

          default:
            throw new ExpressionTypeCheckException("type exception");
        }

      //
      //  tagFormat/tagMaxLength
      //

      setTagFormat(unaryArgument.getTagFormat());
      setTagMaxLength(unaryArgument.getTagMaxLength());
    }

    /*****************************************
    *
    *  assignNodeID
    *
    *****************************************/

    @Override public int assignNodeID(int preorderNumber)
    {
      setNodeID(preorderNumber);
      preorderNumber = unaryArgument.assignNodeID(preorderNumber+1);
      return preorderNumber;
    }

    /*****************************************
    *
    *  evaluate
    *
    *****************************************/

    @Override protected Object evaluate(SubscriberEvaluationRequest subscriberEvaluationRequest, TimeUnit baseTimeUnit)
    {
      /*****************************************
      *
      *  evaluate arguments
      *
      *****************************************/

      Object argumentValue = unaryArgument.evaluate(subscriberEvaluationRequest, baseTimeUnit);

      /*****************************************
      *
      *  evaluate operator
      *
      *****************************************/

      Object result = null;
      switch (type)
        {
          case IntegerExpression:
            switch (operator)
              {
                case PlusOperator:
                  result = argumentValue;
                  break;

                case MinusOperator:
                  result = new Long(-1L * ((Number) argumentValue).longValue());
                  break;
              }
            break;

          case DoubleExpression:
            switch (operator)
              {
                case PlusOperator:
                  result = argumentValue;
                  break;

                case MinusOperator:
                  result = new Double(-1.0 * ((Double) argumentValue).doubleValue());
                  break;
              }
            break;
        }

      /*****************************************
      *
      *  return
      *
      *****************************************/

      return result;
    }

    /*****************************************
    *
    *  esQuery
    *
    *****************************************/

    @Override public void esQuery(StringBuilder script, TimeUnit baseTimeUnit) throws CriterionException
    {
      /*****************************************
      *
      *  script
      *
      *****************************************/

      //
      //  argument
      //
      
      unaryArgument.esQuery(script, baseTimeUnit);
      
      //
      //  operator
      //
      
      switch (type)
        {
          case IntegerExpression:
            switch (operator)
              {
                case PlusOperator:
                  script.append("def right_" + getNodeID() + " = right_" + unaryArgument.getNodeID() + "; ");
                  break;

                case MinusOperator:
                  script.append("def right_" + getNodeID() + " = -1 * right_" + unaryArgument.getNodeID() + "; ");
                  break;
              }
            break;

          case DoubleExpression:
            switch (operator)
              {
                case PlusOperator:
                  script.append("def right_" + getNodeID() + " = right_" + unaryArgument.getNodeID() + "; ");
                  break;

                case MinusOperator:
                  script.append("def right_" + getNodeID() + " = -1.0 * right_" + unaryArgument.getNodeID() + "; ");
                  break;
              }
            break;
        }
    }
    
    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public UnaryExpression(ExpressionOperator operator, Expression unaryArgument)
    {
      super();
      this.operator = operator;
      this.unaryArgument = unaryArgument;
    }
  }

  /*****************************************
  *
  *  class FunctionCallExpression
  *
  *****************************************/

  public static class FunctionCallExpression extends Expression
  {
    /*****************************************
    *
    *  data
    *
    *****************************************/

    private ExpressionFunction function;
    private List<Expression> arguments;

    //
    //  preevaluatedResult
    //

    private Object preevaluatedResult = null;

    /*****************************************
    *
    *  typeCheck
    *
    *****************************************/

    @Override public void typeCheck(ExpressionContext expressionContext, TimeUnit baseTimeUnit)
    {
      /*****************************************
      *
      *  typeCheck arguments
      *
      *****************************************/

      for (Expression argument : arguments)
        {
          argument.typeCheck(expressionContext, baseTimeUnit);
        }

      /*****************************************
      *
      *  type
      *
      *****************************************/

      switch (function)
        {
          case DateConstantFunction:
            typeCheckDateConstantFunction(baseTimeUnit);
            break;
            
          case DateAddFunction:
            typeCheckDateAddFunction(baseTimeUnit);
            break;
            
          default:
            throw new ExpressionTypeCheckException("type exception");
        }
    }

    /*****************************************
    *
    *  typeCheckDateConstantFunction
    *
    *****************************************/

    private void typeCheckDateConstantFunction(TimeUnit baseTimeUnit)
    {
      /****************************************
      *
      *  arguments
      *
      ****************************************/
      
      //
      //  validate number of arguments
      //
      
      if (arguments.size() != 1) throw new ExpressionTypeCheckException("type exception");

      //
      //  arguments
      //
      
      Expression arg1 = (arguments.size() > 0) ? arguments.get(0) : null;

      //
      //  validate arg1
      //
      
      switch (arg1.getType())
        {
          case StringExpression:
            if (! arg1.isConstant()) throw new ExpressionTypeCheckException("type exception");
            break;

          default:
            throw new ExpressionTypeCheckException("type exception");
        }
      
      //
      //  validate baseTimeUnit
      //

      switch (baseTimeUnit)
        {
          case Unknown:
            throw new ExpressionTypeCheckException("type exception");
        }

      /****************************************
      *
      *  constant evaluation
      *
      ****************************************/
      
      String arg1_value = (String) arg1.evaluate(null, TimeUnit.Unknown);
      try
        {
          preevaluatedResult = evaluateDateConstantFunction(arg1_value, baseTimeUnit);
        }
      catch (ExpressionEvaluationException e)
        {
          throw new ExpressionTypeCheckException("type exception");
        }
      
      /****************************************
      *
      *  type
      *
      ****************************************/
      
      setType(ExpressionDataType.DateExpression);
    }

    /*****************************************
    *
    *  typeCheckDateAddFunction
    *
    *****************************************/

    private void typeCheckDateAddFunction(TimeUnit baseTimeUnit)
    {
      /****************************************
      *
      *  arguments
      *
      ****************************************/
      
      //
      //  validate number of arguments
      //
      
      if (arguments.size() != 3) throw new ExpressionTypeCheckException("type exception");

      //
      //  arguments
      //
      
      Expression arg1 = (arguments.size() > 0) ? arguments.get(0) : null;
      Expression arg2 = (arguments.size() > 1) ? arguments.get(1) : null;
      Expression arg3 = (arguments.size() > 2) ? arguments.get(2) : null;

      //
      //  validate arg1
      //
      
      switch (arg1.getType())
        {
          case DateExpression:
            break;

          default:
            throw new ExpressionTypeCheckException("type exception");
        }

      //
      //  validate arg2
      //
      
      switch (arg2.getType())
        {
          case IntegerExpression:
            break;

          default:
            throw new ExpressionTypeCheckException("type exception");
        }

      //
      //  validate arg3
      //
      
      switch (arg3.getType())
        {
          case StringExpression:
            break;

          default:
            throw new ExpressionTypeCheckException("type exception");
        }

      //
      //  validate baseTimeUnit
      //

      switch (baseTimeUnit)
        {
          case Unknown:
            throw new ExpressionTypeCheckException("type exception");
        }
      
      /****************************************
      *
      *  constant evaluation
      *
      ****************************************/
      
      if (arg3.isConstant())
        {
          String arg3Value = (String) arg3.evaluate(null, TimeUnit.Unknown);
          switch (TimeUnit.fromExternalRepresentation(arg3Value))
            {
              case Instant:
              case Unknown:
                throw new ExpressionTypeCheckException("type exception");
            }
        }

      /****************************************
      *
      *  type
      *
      ****************************************/
      
      setType(ExpressionDataType.DateExpression);

      /*****************************************
      *
      *  tagFormat/tagMaxLength
      *
      *****************************************/

      setTagFormat(arg1.getTagFormat());
      setTagMaxLength(arg1.getTagMaxLength());
    }

    /*****************************************
    *
    *  assignNodeID
    *
    *****************************************/

    @Override public int assignNodeID(int preorderNumber)
    {
      setNodeID(preorderNumber);
      for (Expression argument : arguments)
        {
          preorderNumber = argument.assignNodeID(preorderNumber+1);
        }
      return preorderNumber;
    }

    /*****************************************
    *
    *  evaluate
    *
    *****************************************/

    @Override protected Object evaluate(SubscriberEvaluationRequest subscriberEvaluationRequest, TimeUnit baseTimeUnit)
    {
      /*****************************************
      *
      *  evaluate arguments
      *
      *****************************************/

      Object arg1Value = (arguments.size() > 0) ? arguments.get(0).evaluate(subscriberEvaluationRequest, baseTimeUnit) : null;
      Object arg2Value = (arguments.size() > 1) ? arguments.get(1).evaluate(subscriberEvaluationRequest, baseTimeUnit) : null;
      Object arg3Value = (arguments.size() > 2) ? arguments.get(2).evaluate(subscriberEvaluationRequest, baseTimeUnit) : null;

      /*****************************************
      *
      *  evaluate operator
      *
      *****************************************/

      Object result;
      switch (function)
        {
          case DateConstantFunction:
            result = preevaluatedResult;
            break;
            
          case DateAddFunction:
            result = evaluateDateAddFunction((Date) arg1Value, (Number) arg2Value, TimeUnit.fromExternalRepresentation((String) arg3Value), baseTimeUnit);
            break;
            
          default:
            throw new ExpressionEvaluationException();
        }
      
      /*****************************************
      *
      *  return
      *
      *****************************************/

      return result;
    }

    /*****************************************
    *
    *  evaluateDateConstantFunction
    *
    *****************************************/

    private Date evaluateDateConstantFunction(String arg, TimeUnit baseTimeUnit)
    {
      /*****************************************
      *
      *  parse argument
      *
      *****************************************/

      DateFormat standardDayFormat = new SimpleDateFormat("yyyy-MM-dd");
      DateFormat standardDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
      standardDayFormat.setTimeZone(TimeZone.getTimeZone(Deployment.getBaseTimeZone()));
      standardDateFormat.setTimeZone(TimeZone.getTimeZone(Deployment.getBaseTimeZone()));
      Date date = null;
      if (date == null) try { date = standardDateFormat.parse(arg.trim()); } catch (ParseException e) { }
      if (date == null) try { date = standardDayFormat.parse(arg.trim()); } catch (ParseException e) { }
      if (date == null) throw new ExpressionEvaluationException();

      /*****************************************
      *
      *  truncate (to baseTimeUnit)
      *
      *****************************************/

      switch (baseTimeUnit)
        {
          case Instant:
            break;
          case Minute:
            date = RLMDateUtils.truncate(date, Calendar.MINUTE, Calendar.SUNDAY, Deployment.getBaseTimeZone());
            break;
          case Hour:
            date = RLMDateUtils.truncate(date, Calendar.HOUR, Calendar.SUNDAY, Deployment.getBaseTimeZone());
            break;
          case Day:
            date = RLMDateUtils.truncate(date, Calendar.DATE, Calendar.SUNDAY, Deployment.getBaseTimeZone());
            break;
          case Week:
            date = RLMDateUtils.truncate(date, Calendar.DAY_OF_WEEK, Calendar.SUNDAY, Deployment.getBaseTimeZone());
            break;
          case Month:
            date = RLMDateUtils.truncate(date, Calendar.MONTH, Calendar.SUNDAY, Deployment.getBaseTimeZone());
            break;
          case Year:
            date = RLMDateUtils.truncate(date, Calendar.YEAR, Calendar.SUNDAY, Deployment.getBaseTimeZone());
            break;
        }
      
      /*****************************************
      *
      *  return
      *
      *****************************************/

      return date;
    }

    /*****************************************
    *
    *  evaluateDateAddFunction
    *
    *****************************************/

    private Date evaluateDateAddFunction(Date date, Number number, TimeUnit timeUnit, TimeUnit baseTimeUnit)
    {
      //
      //  truncate
      //

      switch (baseTimeUnit)
        {
          case Instant:
            break;
          case Minute:
            date = RLMDateUtils.truncate(date, Calendar.MINUTE, Calendar.SUNDAY, Deployment.getBaseTimeZone());
            break;
          case Hour:
            date = RLMDateUtils.truncate(date, Calendar.HOUR, Calendar.SUNDAY, Deployment.getBaseTimeZone());
            break;
          case Day:
            date = RLMDateUtils.truncate(date, Calendar.DATE, Calendar.SUNDAY, Deployment.getBaseTimeZone());
            break;
          case Week:
            date = RLMDateUtils.truncate(date, Calendar.DAY_OF_WEEK, Calendar.SUNDAY, Deployment.getBaseTimeZone());
            break;
          case Month:
            date = RLMDateUtils.truncate(date, Calendar.MONTH, Calendar.SUNDAY, Deployment.getBaseTimeZone());
            break;
          case Year:
            date = RLMDateUtils.truncate(date, Calendar.YEAR, Calendar.SUNDAY, Deployment.getBaseTimeZone());
            break;
        }
      
      //
      //  add time interval
      //

      switch (timeUnit)
        {
          case Minute:
            date = RLMDateUtils.addMinutes(date, number.intValue());
            break;
          case Hour:
            date = RLMDateUtils.addHours(date, number.intValue());
            break;
          case Day:
            date = RLMDateUtils.addDays(date, number.intValue(), Deployment.getBaseTimeZone());
            break;
          case Week:
            date = RLMDateUtils.addWeeks(date, number.intValue(), Deployment.getBaseTimeZone());
            break;
          case Month:
            date = RLMDateUtils.addMonths(date, number.intValue(), Deployment.getBaseTimeZone());
            break;
          case Year:
            date = RLMDateUtils.addYears(date, number.intValue(), Deployment.getBaseTimeZone());
            break;
        }
      
      //
      //  truncate (after adding)
      //

      switch (baseTimeUnit)
        {
          case Instant:
            break;
          case Minute:
            date = RLMDateUtils.truncate(date, Calendar.MINUTE, Calendar.SUNDAY, Deployment.getBaseTimeZone());
            break;
          case Hour:
            date = RLMDateUtils.truncate(date, Calendar.HOUR, Calendar.SUNDAY, Deployment.getBaseTimeZone());
            break;
          case Day:
            date = RLMDateUtils.truncate(date, Calendar.DATE, Calendar.SUNDAY, Deployment.getBaseTimeZone());
            break;
          case Week:
            date = RLMDateUtils.truncate(date, Calendar.DAY_OF_WEEK, Calendar.SUNDAY, Deployment.getBaseTimeZone());
            break;
          case Month:
            date = RLMDateUtils.truncate(date, Calendar.MONTH, Calendar.SUNDAY, Deployment.getBaseTimeZone());
            break;
          case Year:
            date = RLMDateUtils.truncate(date, Calendar.YEAR, Calendar.SUNDAY, Deployment.getBaseTimeZone());
            break;
        }

      //
      //  return
      //
      
      return date;
    }

    /*****************************************
    *
    *  esQuery
    *
    *****************************************/

    @Override public void esQuery(StringBuilder script, TimeUnit baseTimeUnit) throws CriterionException
    {
      /*****************************************
      *
      *  script
      *
      *****************************************/

      switch (function)
        {
          case DateConstantFunction:
            esQueryDateConstantFunction(script, baseTimeUnit);
            break;
            
          case DateAddFunction:
            esQueryDateAddFunction(script, baseTimeUnit);
            break;
            
          default:
            throw new ExpressionEvaluationException();
        }
    }

    /*****************************************
    *
    *  esQueryDateConstantFunction
    *
    *****************************************/

    private void esQueryDateConstantFunction(StringBuilder script, TimeUnit baseTimeUnit) throws CriterionException
    {
      /****************************************
      *
      *  arguments
      *
      ****************************************/

      arguments.get(0).esQuery(script, baseTimeUnit);
      
      /****************************************
      *
      *  function
      *
      ****************************************/
      
      script.append("def rightSF_" + getNodeID() + " = new SimpleDateFormat(\"yyyy-MM-dd'T'HH:mm:ss\"); ");
      script.append("rightSF_" + getNodeID() + ".setTimeZone(TimeZone.getTimeZone(\"" + Deployment.getBaseTimeZone() + "\")); ");
      script.append("def rightDT_" + getNodeID() + " = rightSF_" + getNodeID() + ".parse(right_" + arguments.get(0).getNodeID() + "); ");
      script.append("def rightCalendar_" + getNodeID() + " = rightSF_" + getNodeID() + ".getCalendar(); ");
      script.append("rightCalendar_" + getNodeID() + ".setTime(rightDT_" + getNodeID() + "); ");
      script.append("def rightInstant_" + getNodeID() + " = rightCalendar_" + getNodeID() + ".toInstant(); ");
      script.append("def rightBeforeTruncate_" + getNodeID() + " = LocalDateTime.ofInstant(rightInstant_" + getNodeID() + ", ZoneOffset.UTC); ");
      script.append(EvaluationCriterion.constructDateTruncateESScript(getNodeID(), "rightBeforeTruncate", "right", baseTimeUnit));
    }
    
    /*****************************************
    *
    *  esQueryDateAddFunction
    *
    *****************************************/

    private void esQueryDateAddFunction(StringBuilder script, TimeUnit baseTimeUnit) throws CriterionException
    {
      /*****************************************
      *
      *  validate
      *
      *****************************************/

      if (! arguments.get(2).isConstant())
        {
          throw new CriterionException("invalid criterionField " + arguments.get(2));
        }

      /****************************************
      *
      *  arguments
      *
      ****************************************/
      
      arguments.get(0).esQuery(script, baseTimeUnit);
      arguments.get(1).esQuery(script, baseTimeUnit);
      
      /****************************************
      *
      *  function
      *
      ****************************************/
      
      //
      //  truncate
      //

      script.append("def rightRawInstantBeforeAdd_" + getNodeID() + " = right_" + arguments.get(0).getNodeID() + "; ");
      script.append(EvaluationCriterion.constructDateTruncateESScript(getNodeID(), "rightRawInstantBeforeAdd", "rightInstantBeforeAdd", baseTimeUnit));

      //
      //  add time interval
      //
      
      TimeUnit timeUnit = TimeUnit.fromExternalRepresentation((String) arguments.get(2).evaluate(null, TimeUnit.Unknown));
      script.append("def rightRawInstant_" + getNodeID() + " = rightInstantBeforeAdd_" + getNodeID() + ".plus(right_" + arguments.get(1).getNodeID() + ", ChronoUnit." + timeUnit.getChronoUnit() + "); ");
      
      //
      //  truncate (after adding)
      //

      script.append(EvaluationCriterion.constructDateTruncateESScript(getNodeID(), "rightRawInstant", "rightInstant", baseTimeUnit));

      //
      //  result
      //
      
      script.append("def right_" + getNodeID() + " = rightInstant_" + getNodeID() + "; ");
    }
    
    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public FunctionCallExpression(ExpressionFunction function, List<Expression> arguments)
    {
      super();
      this.function = function;
      this.arguments = arguments;
    }
  }

  /*****************************************
  *
  *  class ExpressionReader
  *
  *****************************************/

  public static class ExpressionReader
  {
    /*****************************************
    *
    *  data
    *
    *****************************************/

    private CriterionContext criterionContext;
    private String expressionString;
    private TimeUnit expressionBaseTimeUnit;

    //
    //  derived
    //

    private Expression expression;

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public ExpressionReader(CriterionContext criterionContext, String expressionString, TimeUnit expressionBaseTimeUnit)
    {
      this.criterionContext = criterionContext;
      this.expressionString = expressionString;
      this.expressionBaseTimeUnit = expressionBaseTimeUnit;
      this.expression = null;
    }

    /*****************************************
    *
    *  parseExpression
    *
    *****************************************/

    public Expression parse(ExpressionContext expressionContext) throws ExpressionParseException, ExpressionTypeCheckException
    {
      /*****************************************
      *
      *  parse
      *
      *****************************************/

      if (expressionString != null)
        {
          /*****************************************
          *
          *  pass 1 -- parse expressionString
          *
          *****************************************/

          try
            {
              //
              //  intialize reader
              //

              reader = new StringReader(expressionString);

              //
              //  parse
              //

              expression = parseExpression();

              //
              //  input consumed?
              //

              switch (nextToken())
                {
                  case END_OF_INPUT:
                    break;

                  default:
                    parseError(tokenPosition,"expected end of input");
                    break;
                }

              //
              //  parse errors
              //

              if (parseErrors != null)
                {
                  throw new ExpressionParseException(parseErrors);
                }
            }
          finally
            {
              reader.close(); 
            }

          /*****************************************
          *
          *  pass 2 -- typecheck expression
          *
          *****************************************/

          expression.typeCheck(expressionContext, expressionBaseTimeUnit);

          /*****************************************
          *
          *  pass 3 -- assignNodeID to expression
          *
          *****************************************/

          expression.assignNodeID(0);
        }

      /*****************************************
      *
      *  return
      *
      *****************************************/

      return expression;
    }

    /*****************************************************************************
    *
    *  character reader
    *
    *****************************************************************************/

    //
    //  whitespace characters (symbolic constants)
    //

    private final char EOF = (char) -1;
    private final char TAB = '\t';
    private final char LF = '\n';
    private final char CR = '\r';
    private final char FF = '\f';

    //
    //  single quote 
    //

    private final char SINGLE_QUOTE = '\'';

    //
    //  character reader state
    //

    private StringReader reader = null;                       // StringReader for lexer input
    private boolean haveCH = false;                           // lexer already has lookahead character
    private char ch = LF;                                     // current character available to lexer
    private LexerPosition chPosition = new LexerPosition();   // file position of current character

    /*****************************************
    *
    *  character classes
    *
    *****************************************/

    private boolean in_range(char low, char c, char high) { return (low <= c && c <= high); }
    private boolean printable(char c) { return (c != CR && c != LF && c != FF && c != EOF); }
    private boolean letter(char c) { return in_range('A',c,'Z') || in_range('a',c,'z'); }
    private boolean digit(char c) { return in_range('0',c,'9'); }
    private boolean id_char(char c) { return (letter(c) || digit(c) || c == '.'); }
    private boolean unary_char(char c) { return (c == '+' || c == '-'); }

    /*****************************************
    *
    *  getNextCharacter
    *
    *****************************************/

    private void getNextCharacter()
    {
      try
        {
          if (ch == LF) chPosition.nextLine();
          boolean done;
          do
            {
              done = true;
              ch = (char) reader.read();
              chPosition.nextChr();
              if (in_range('a',ch,'z') && caseFold)
                {
                  ch = Character.toUpperCase(ch);
                }
              else if (! printable(ch))
                {
                  if (ch == CR)
                    {
                      reader.mark(1);
                      ch = (char) reader.read();
                      if (ch != LF)
                        {
                          parseError(chPosition,"CR not followed by LF");
                          reader.reset();
                          ch = ' ';
                        }
                    }
                  if (ch == LF || ch == FF) { }
                  else if (ch == EOF) { }
                  else
                    {
                      throw new ExpressionParseException("getNextCharacter() - unknown character.");
                    }
                }
            }
          while (! done);
        }
      catch (IOException e)
        {
          throw new ExpressionParseException("IOException (invariant violation)");
        }
    }

    /*****************************************************************************
    *
    *  lexer
    *
    *****************************************************************************/

    //
    //  lexer state (for parser)
    //

    private LexerPosition tokenPosition = new LexerPosition();
    private Object tokenValue;

    //
    //  lexer state (internal)
    //

    private boolean caseFold = false;
    private boolean haveLookaheadToken = false;
    private Token lookaheadToken;
    private LexerPosition lookaheadTokenPosition = new LexerPosition();
    private Object lookaheadTokenValue = null;
    private Token previousToken = Token.END_OF_INPUT;

    /*****************************************
    *
    *  getLookaheadToken
    *
    *****************************************/

    private Token getLookaheadToken()
    {
      boolean done;
      Token result = Token.END_OF_INPUT;
      lookaheadTokenValue = null;
      do
        {
          //
          //  next character
          //

          done = true;
          if (! haveCH) getNextCharacter();
          lookaheadTokenPosition = new LexerPosition(chPosition);
          haveCH = false;

          //
          //  tokenize
          //

          if (ch == ' ' || ch == TAB || ch == FF)
            {
              done = false;               // whitespace
            }
          else if (letter(ch))
            {
              result = getIdentifier();
            }
          else if (digit(ch))
            {
              result = getNumber();
            }
          else if (ch == LF) result = Token.END_OF_INPUT;
          else if (ch == '-') result = Token.MINUS;
          else if (ch == '+') result = Token.PLUS;
          else if (ch == '*') result = Token.MULTIPLY;
          else if (ch == '/') result = Token.DIVIDE;
          else if (ch == '(') result = Token.LEFT_PAREN;
          else if (ch == ')') result = Token.RIGHT_PAREN;
          else if (ch == '[') result = Token.LEFT_BRACKET;
          else if (ch == ']') result = Token.RIGHT_BRACKET;
          else if (ch == ',') result = Token.COMMA;
          else if (ch == EOF) result = Token.END_OF_INPUT;
          else if (ch == SINGLE_QUOTE)
            {
              result = getStringLiteral();
            }
          else
            {
              switch (ch)
                {
                  case '_':
                  case '@':
                  case '\\':
                  case '{':
                  case '}':
                  case '?':
                  case '%':
                  case '.':
                    result = Token.INVALID_CHAR;
                    break;
                  default:
                    parseError(chPosition,"this character not permitted here");
                    done = false;
                    break;
                }
            }
        }
      while (! done);
      previousToken = result;
      return result;
    }

    /*****************************************
    *
    *  getIdentifier
    *
    *****************************************/

    private Token getIdentifier()
    {
      //
      //  get token
      //

      StringBuilder buffer = new StringBuilder();
      do
        {
          buffer.append(ch);
          getNextCharacter();
        }
      while (id_char(ch));
      haveCH = true;
      String identifier = buffer.toString();
      lookaheadTokenValue = identifier;

      //
      //  return
      //

      Token result;
      ExpressionFunction functionCall = ExpressionFunction.fromFunctionName(identifier);
      switch (functionCall)
        {
          case UnknownFunction:
            CriterionField criterionField = criterionContext.getCriterionFields().get(identifier);
            if (criterionField != null)
              {
                lookaheadTokenValue = criterionField;
                result = Token.IDENTIFIER;
              }
            else if (identifier.equalsIgnoreCase("true"))
              {
                lookaheadTokenValue = Boolean.TRUE;
                result = Token.BOOLEAN;
              }
            else if (identifier.equalsIgnoreCase("false"))
              {
                lookaheadTokenValue = Boolean.FALSE;
                result = Token.BOOLEAN;
              }
            else
              {
                result = Token.INVALID_IDENTIFIER;
              }
            break;
          default:
            lookaheadTokenValue = functionCall;
            result = Token.FUNCTION_CALL;
            break;
        }
      return result;
    }

    /*****************************************
    *
    *  getNumber
    *
    *****************************************/

    private Token getNumber()
    {
      //
      //  get token
      //

      StringBuilder buffer = new StringBuilder();
      boolean fixedPoint = false;
      boolean percent = false;
      while (true)
        {
          if (digit(ch))
            {
              buffer.append(ch);
            }
          else if (ch == '.')
            {
              if (fixedPoint) parseError(chPosition,"bad number");
              buffer.append(ch);
              fixedPoint = true;
            }
          else
            {
              break;
            }
          getNextCharacter();
        }

      //
      //  handle percent
      //

      if (ch == '%')
        {
          percent = true;
          haveCH = false;
        }
      else
        {
          haveCH = true;
        }

      //
      //  calculate and return result
      //

      Token result;
      if (fixedPoint || percent)
        {
          double value = (new Double(buffer.toString())).doubleValue();
          if (percent) value *= 0.01;
          lookaheadTokenValue = new Double(value);
          result = Token.DOUBLE;
        }
      else
        {
          long value = (new Long(buffer.toString())).longValue();
          lookaheadTokenValue = new Long(value);
          result = Token.INTEGER;
        }
      return result;
    }

    /*****************************************
    *
    *  getStringLiteral
    *
    *****************************************/

    private Token getStringLiteral()
    {
      StringBuilder buffer = new StringBuilder();
      boolean saveCaseFold = caseFold;
      caseFold = false;
      getNextCharacter();
      while (true)
        {
          if (ch == SINGLE_QUOTE)
            {
              getNextCharacter();
              if (ch != SINGLE_QUOTE)
                {
                  haveCH = true;
                  break;
                }
            }
          else if (ch == LF || ch == EOF)
            {
              parseError(chPosition,"unterminated string (strings must be on one line)");
              break;
            }
          buffer.append(ch);
          getNextCharacter();
        }
      caseFold = saveCaseFold;
      lookaheadTokenValue = buffer.toString();
      return Token.STRING;
    }

    /*****************************************
    *
    *  nextToken
    *
    *****************************************/

    private Token nextToken()
    {
      //
      //  if we don't have a lookahead token, get one
      //

      if (! haveLookaheadToken)
        {
          lookaheadToken = getLookaheadToken();
          haveLookaheadToken = true;
        }

      //
      //  return the lookahead token
      //

      Token result = lookaheadToken;
      tokenPosition = new LexerPosition(lookaheadTokenPosition);
      tokenValue = lookaheadTokenValue;
      haveLookaheadToken = false;

      //
      //  return
      //

      return result;
    }

    /*****************************************
    *
    *  peekToken
    *
    *****************************************/

    private Token peekToken()
    {
      if (! haveLookaheadToken)
        {
          lookaheadToken = getLookaheadToken();
          haveLookaheadToken = true;
        }

      return lookaheadToken;
    }

    /*****************************************
    *
    *  class lexerPosition
    *
    *****************************************/

    private class LexerPosition
    {
      private short line = 0;             // current line
      private short chr = 0;              // current position on line
      LexerPosition() { }
      LexerPosition(LexerPosition position) { line = position.line; chr = position.chr; }
      void nextLine() { line += 1; chr = 0; }
      void nextChr() { chr += 1; }
      short getLine() { return line; }
      short getChr() { return chr; }
      public String toString() { return "Line " + line + ", Chr " + chr; }
    }

    /*****************************************************************************
    *
    *  parseExpression
    *
    *  <expression> ::= <term> { <adding_op> <term> }*
    *
    *  <term> ::= <primary> {<multiplying_op> <primary>}*
    *
    *  <primary> ::=
    *       <constant>
    *     | <identifier>  
    *     | <unary_op> <primary>
    *     | <functionCall>
    *     | '(' <expression> ')'
    *
    *  <constant> ::= <integer> | <double> | <string> | <boolean> | <integerset> | <stringset>
    *
    *  <integerset> ::= '[' { <integer> ; ',' }* ']'
    *
    *  <stringset> :: '[' { <string> ; ',' }* ']'
    *
    *  <unary_op> ::= '-' | '+'
    *
    *  <multiplying_op> ::= '*' | '/'
    *
    *  <adding_op> ::= '+' | '-'
    *
    *  <function> ::= <functionName> '(' { <expression> ; ',' }* ')'
    *  
    *  <functionName> ::=
    *       'dateConstant'
    *     | 'dateAdd'
    *
    *****************************************************************************/

    /*****************************************
    *
    *  parseExpression
    *
    *****************************************/

    private Expression parseExpression()
    {
      //
      //  <expression> ::= <term> { <adding_op> <term> }*
      //

      Expression result = parseTerm();
      boolean parsingExpression = true;
      while (parsingExpression)
        {
          Token token = peekToken();
          switch (token)
            {
              case PLUS:
              case MINUS:
                token = nextToken();
                ExpressionOperator operator = ExpressionOperator.fromOperatorName(token);
                Expression right = parseTerm();
                result = new OperatorExpression(operator, result, right);
                break;

              default:
                parsingExpression = false;
                break;
            }
        }
      return result;
    }

    /*****************************************
    *
    *  parseTerm
    *
    *****************************************/

    private Expression parseTerm()
    {
      //
      //  <term> ::= <primary> {<multiplying_op> <primary>}*
      //

      Expression result = parsePrimary();
      boolean parsingTerm = true;
      while (parsingTerm)
        {
          Token token = peekToken();
          switch (token)
            {
              case MULTIPLY:
              case DIVIDE:
                token = nextToken();
                ExpressionOperator operator = ExpressionOperator.fromOperatorName(token);
                Expression right = parsePrimary();
                result = new OperatorExpression(operator, result, right);
                break;

              default:
                parsingTerm = false;
                break;
            }
        }
      return result;
    }

    /*****************************************
    *
    *  parsePrimary
    *
    *****************************************/

    private Expression parsePrimary()
    {
      //  <primary> ::=
      //       <constant>
      //     | <identifier>  
      //     | <unary_op> <primary>
      //     | <functionCall>
      //     | '(' <expression> ')'
      //
      //  <constant> ::= <integer> | <double> | <string> | <boolean> | <integerset> | <stringset>
      //
      //  <integerset> ::= '[' { <integer> ; ',' }* ']'
      //
      //  <stringset> :: '[' { <string> ; ',' }* ']'
      //
      //  <unary_op> ::= '-' | '+'
      //
      //  <function> ::= <functionName> '(' { <expression> ; ',' }* ')'
      //

      Expression result;
      Token token = nextToken();
      switch (token)
        {
          case INTEGER:
            result = new ConstantExpression(ExpressionDataType.IntegerExpression, tokenValue);
            break;

          case DOUBLE:
            result = new ConstantExpression(ExpressionDataType.DoubleExpression, tokenValue);
            break;

          case STRING:
            result = new ConstantExpression(ExpressionDataType.StringExpression, tokenValue);
            break;

          case BOOLEAN:
            result = new ConstantExpression(ExpressionDataType.BooleanExpression, tokenValue);
            break;

          case LEFT_BRACKET:
            Set<Object> setConstant = new HashSet<Object>();
            ExpressionDataType setDataType = ExpressionDataType.EmptySetExpression;
            token = peekToken();
            while (token != Token.RIGHT_BRACKET)
              {
                token = nextToken();
                switch (token)
                  {
                    case INTEGER:
                      switch (setDataType)
                        {
                          case IntegerSetExpression:
                          case EmptySetExpression:
                            setConstant.add(tokenValue);
                            setDataType = ExpressionDataType.IntegerSetExpression;
                            break;

                          default:
                            parseError(tokenPosition,"expected literal");
                            throw new ExpressionParseException(parseErrors);
                        }
                      break;

                    case STRING:
                      switch (setDataType)
                        {
                          case StringSetExpression:
                          case EmptySetExpression:
                            setConstant.add(tokenValue);
                            setDataType = ExpressionDataType.StringSetExpression;
                            break;

                          default:
                            parseError(tokenPosition,"expected literal");
                            throw new ExpressionParseException(parseErrors);
                        }
                      break;

                    default:
                      parseError(tokenPosition,"expected literal");
                      throw new ExpressionParseException(parseErrors);
                  }
                token = peekToken();
                switch (token)
                  {
                    case COMMA:
                      token = nextToken();
                      break;
                    case RIGHT_BRACKET:
                      break;
                    default:
                      parseError(tokenPosition,"expected ']'");
                      throw new ExpressionParseException(parseErrors);
                  }

              }
            token = nextToken();
            result = new ConstantExpression(setDataType, setConstant);
            break;

          case IDENTIFIER:
            result = new ReferenceExpression((CriterionField) tokenValue);
            break;

          case MINUS:
          case PLUS:
            ExpressionOperator operator = ExpressionOperator.fromOperatorName(token);
            Expression primary = parsePrimary();
            result = new UnaryExpression(operator,primary);
            break;

          case FUNCTION_CALL:
            ExpressionFunction function = (ExpressionFunction) tokenValue;
            List<Expression> arguments = new ArrayList<Expression>();
            token = nextToken();
            if (token != Token.LEFT_PAREN)
              {
                parseError(tokenPosition,"expected '('");
                throw new ExpressionParseException(parseErrors);
              }
            token = peekToken();
            while (token != Token.RIGHT_PAREN)
              {
                Expression argument = parseExpression();
                arguments.add(argument);
                token = peekToken();
                switch (token)
                  {
                    case COMMA:
                      token = nextToken();
                      break;
                    case RIGHT_PAREN:
                      break;
                    default:
                      parseError(tokenPosition,"expected ')'");
                      throw new ExpressionParseException(parseErrors);
                  }
              }
            token = nextToken();
            result = new FunctionCallExpression(function, arguments);
            break;

          case LEFT_PAREN:
            result = parseExpression();
            token = nextToken();
            if (token != Token.RIGHT_PAREN)
              {
                parseError(tokenPosition,"expected ')'");
                throw new ExpressionParseException(parseErrors);
              }
            break;

          default:
            parseError(tokenPosition,"expected <primary> in " + expressionString);
            throw new ExpressionParseException(parseErrors);
        }
      return result;
    }

    /*****************************************
    *
    *  parseError
    *
    *****************************************/

    private String parseErrors = null;
    private void parseError(LexerPosition position, String message)
    {
      if (parseErrors == null)
        {
          parseErrors = position.toString() + ": " + message;
        }
    }
  }

  /*****************************************
  *
  *  ParseException
  *
  *****************************************/

  public static class ExpressionParseException extends RuntimeException
  {
    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public ExpressionParseException(String message)
    {
      super(message);
    }
  }

  /*****************************************
  *
  *  TypeCheckException
  *
  *****************************************/

  public static class ExpressionTypeCheckException extends RuntimeException
  {
    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public ExpressionTypeCheckException(String message)
    {
      super(message);
    }
  }
  
  /*****************************************
  *
  *  ExpressionEvaluationException
  *
  *****************************************/

  public static class ExpressionEvaluationException extends RuntimeException
  {
    /*****************************************
    *
    *  data
    *
    *****************************************/

    private CriterionField criterionField;

    /*****************************************
    *
    *  accessors
    *
    *****************************************/
    
    public CriterionField getCriterionField() { return criterionField; }

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    //
    //  constructor (criterionField)
    //
    
    public ExpressionEvaluationException(CriterionField criterionField)
    {
      this.criterionField = criterionField;
    }
    
    //
    //  constructor (empty)
    //
    
    public ExpressionEvaluationException()
    {
      this.criterionField = null;
    }
  }

  /*****************************************
  *
  *  ExpressionNullException
  *
  *****************************************/

  private static class ExpressionNullException extends ExpressionEvaluationException
  {
    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    //
    //  constructor (criterionField)
    //
    
    private ExpressionNullException(CriterionField criterionField)
    {
      super(criterionField);
    }
    
    //
    //  constructor (empty)
    //
    
    private ExpressionNullException()
    {
      super();
    }
  }
}
