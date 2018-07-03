/*****************************************************************************
*
*  EvaluationCriterion.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

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

import com.rii.utilities.JSONUtilities;
import com.rii.utilities.JSONUtilities.JSONUtilitiesException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import com.rii.utilities.SystemTime;

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

public class EvaluationCriterion
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(EvaluationCriterion.class);

  /*****************************************
  *
  *  enum
  *
  *****************************************/

  //
  //  CriterionContext
  //

  public enum CriterionContext
  {
    Profile("profile"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private CriterionContext(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static CriterionContext fromExternalRepresentation(String externalRepresentation) { for (CriterionContext enumeratedValue : CriterionContext.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }

  //
  //  CriterionDataType
  //

  public enum CriterionDataType
  {
    //
    //  criterionFields AND criterionArguments
    //

    IntegerCriterion("integer"),
    DoubleCriterion("double"),
    StringCriterion("string"),
    BooleanCriterion("boolean"),
    DateCriterion("date"),
    StringSetCriterion("stringSet"),
    
    //
    //  only for criterionArguments
    //

    NumberCriterion("number"),
    NoArgumentCriterion("noArgument"),
    IntegerSetCriterion("integerSet"),

    //
    //  structure
    //

    Unknown("(unknown)");
    private String externalRepresentation;
    private CriterionDataType(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static CriterionDataType fromExternalRepresentation(String externalRepresentation) { for (CriterionDataType enumeratedValue : CriterionDataType.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }

    //
    //  getBaseType
    //
    
    public CriterionDataType getBaseType()
    {
      CriterionDataType result;
      switch (this)
        {
          case IntegerCriterion:
          case DoubleCriterion:
            result = NumberCriterion;
            break;

          case IntegerSetCriterion:
            result = NumberCriterion;
            break;

          case StringSetCriterion:
            result = StringCriterion;
            break;

          default:
            result = this;
        }
      return result;
    }

    //
    //  getSingletonType
    //
    
    public boolean getSingletonType()
    {
      boolean result;
      switch (this)
        {
          case IntegerSetCriterion:
          case StringSetCriterion:
            result = false;
            break;

          default:
            result = true;
            break;
        }
      return result;
    }
  }

  //
  //  CriterionOperator
  //

  public enum CriterionOperator
  {
    EqualOperator("=="),
    NotEqualOperator("<>"),
    GreaterThanOperator(">"),
    GreaterThanOrEqualOperator(">="),
    LessThanOperator("<"),
    LessThanOrEqualOperator("<="),
    IsNullOperator("is null"),
    IsNotNullOperator("is not null"),
    IsInSetOperator("is in set"),
    NotInSetOperator("not in set"),
    ContainsOperator("contains"),
    DoesNotContainOperator("does not contain"),
    NonEmptyIntersectionOperator("non empty intersection"),
    EmptyIntersectionOperator("empty intersection"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private CriterionOperator(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static CriterionOperator fromExternalRepresentation(String externalRepresentation) { for (CriterionOperator enumeratedValue : CriterionOperator.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }

  //
  //  TimeUnit
  //

  public enum TimeUnit
  {
    Instant("instant", "MILLIS"),
    Minute("minute", "MINUTES"),
    Hour("hour", "HOURS"),
    Day("day", "DAYS"),
    Week("week", "WEEKS"),
    Month("month", "MONTHS"),
    Year("year", "YEARS"),
    Unknown("(unknown)", "(unknown)");
    private String externalRepresentation;
    private String chronoUnit;
    private TimeUnit(String externalRepresentation, String chronoUnit) { this.externalRepresentation = externalRepresentation; this.chronoUnit = chronoUnit; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public String getChronoUnit() { return chronoUnit; }
    public static TimeUnit fromExternalRepresentation(String externalRepresentation) { for (TimeUnit enumeratedValue : TimeUnit.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
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
    //
    //  schema
    //
    
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("criterion");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("criterionContext", Schema.STRING_SCHEMA);
    schemaBuilder.field("criterionField", Schema.STRING_SCHEMA);
    schemaBuilder.field("criterionOperator", Schema.STRING_SCHEMA);
    schemaBuilder.field("argumentExpression", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("argumentBaseTimeUnit", Schema.STRING_SCHEMA);
    schemaBuilder.field("storyReference", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("criterionDefault", Schema.BOOLEAN_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  accessor
  //

  public static Schema schema() { return schema; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  //
  //  criterion
  //

  private CriterionContext criterionContext;
  private CriterionField criterionField;
  private CriterionOperator criterionOperator;
  private String argumentExpression;
  private TimeUnit argumentBaseTimeUnit;
  private String storyReference;
  private boolean criterionDefault;

  //
  //  derived
  //

  private Expression argument;

  /*****************************************
  *
  *  constructor -- simple
  *
  *****************************************/

  private EvaluationCriterion(CriterionContext criterionContext, CriterionField criterionField, CriterionOperator criterionOperator, String argumentExpression, TimeUnit argumentBaseTimeUnit, String storyReference, boolean criterionDefault)
  {
    this.criterionContext = criterionContext;
    this.criterionField = criterionField;
    this.criterionOperator = criterionOperator;
    this.argumentExpression = argumentExpression;
    this.argumentBaseTimeUnit = argumentBaseTimeUnit;
    this.storyReference = storyReference;
    this.criterionDefault = criterionDefault;
    this.argument = null;
  }

  /*****************************************
  *
  *  constructor -- external JSON
  *
  *****************************************/

  EvaluationCriterion(JSONObject jsonRoot, CriterionContext criterionContext) throws GUIManagerException
  {
    //
    //  basic fields (all but argument)
    //

    this.criterionContext = criterionContext;
    this.criterionField = Deployment.getCriterionFields(criterionContext).get(JSONUtilities.decodeString(jsonRoot, "criterionField", true));
    this.criterionOperator = CriterionOperator.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "criterionOperator", true));
    this.storyReference = JSONUtilities.decodeString(jsonRoot, "storyReference", false);
    this.criterionDefault = JSONUtilities.decodeBoolean(jsonRoot, "criterionDefault", Boolean.FALSE);

    //
    //  validate (all but argument)
    //
    
    if (this.criterionField == null) throw new GUIManagerException("unsupported " + criterionContext.getExternalRepresentation() + " criterion field", JSONUtilities.decodeString(jsonRoot, "criterionField", true));
    if (this.criterionOperator == CriterionOperator.Unknown) throw new GUIManagerException("unknown operator", JSONUtilities.decodeString(jsonRoot, "criterionOperator", true));

    //
    // argument
    //

    try
      {
        JSONObject argumentJSON = JSONUtilities.decodeJSONObject(jsonRoot, "argument", false);
        this.argumentExpression = (argumentJSON != null) ? JSONUtilities.decodeString(argumentJSON, "expression", true) : null;
        this.argumentBaseTimeUnit = (argumentJSON != null) ? TimeUnit.fromExternalRepresentation(JSONUtilities.decodeString(argumentJSON, "timeUnit", "(unknown)")) : TimeUnit.Unknown;
        parseArgument();
      }
    catch (ExpressionParseException|ExpressionTypeCheckException e)
      {
        throw new GUIManagerException(e);
      }

    //
    //  validate
    //

    try
      {
        validate();
      }
    catch (CriterionException e)
      {
        log.info("invalid criterion for field {}", criterionField.getCriterionFieldName());
        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.info(stackTraceWriter.toString());
        throw new GUIManagerException(e);
      }
  }

  /*****************************************
  *
  *  validate
  *
  *****************************************/

  private void validate() throws CriterionException
  {
    //
    //  validate operator against data type
    //

    boolean validCombination = false;
    ExpressionDataType argumentType = (argument != null) ? argument.getType() : ExpressionDataType.NoArgument;
    switch (this.criterionOperator)
      {
        case EqualOperator:
        case NotEqualOperator:
          switch (criterionField.getFieldDataType())
            {
              case IntegerCriterion:
                switch (argumentType)
                  {
                    case IntegerExpression:
                      validCombination = true;
                      break;
                    default:
                      validCombination = false;
                      break;
                  }
                break;
                
              case DoubleCriterion:
                switch (argumentType)
                  {
                    case DoubleExpression:
                      validCombination = true;
                      break;
                    default:
                      validCombination = false;
                      break;
                  }
                break;
                
              case StringCriterion:
                switch (argumentType)
                  {
                    case StringExpression:
                      validCombination = true;
                      break;
                    default:
                      validCombination = false;
                      break;
                  }
                break;
                
              case BooleanCriterion:
                switch (argumentType)
                  {
                    case BooleanExpression:
                      validCombination = true;
                      break;
                    default:
                      validCombination = false;
                      break;
                  }
                break;
                
              case DateCriterion:
                switch (argumentType)
                  {
                    case DateExpression:
                      validCombination = true;
                      break;
                    default:
                      validCombination = false;
                      break;
                  }
                break;
                
              default:
                validCombination = false;
                break;
            }
          break;
          
        case GreaterThanOperator:
        case GreaterThanOrEqualOperator:
        case LessThanOperator:
        case LessThanOrEqualOperator:
          switch (criterionField.getFieldDataType())
            {
              case IntegerCriterion:
              case DoubleCriterion:
                switch (argumentType)
                  {
                    case IntegerExpression:
                    case DoubleExpression:
                      validCombination = true;
                      break;
                    default:
                      validCombination = false;
                      break;
                  }
                break;
                
              case DateCriterion:
                switch (argumentType)
                  {
                    case DateExpression:
                      validCombination = true;
                      break;
                    default:
                      validCombination = false;
                      break;
                  }
                break;
                
              default:
                validCombination = false;
                break;
            }
          break;
          
        case IsNullOperator:
        case IsNotNullOperator:
          switch (argumentType)
            {
              case NoArgument:
                validCombination = true;
                break;
              default:
                validCombination = false;
                break;
            }
          break;
          
        case IsInSetOperator:
        case NotInSetOperator:
          switch (criterionField.getFieldDataType())
            {
              case StringCriterion:
                switch (argumentType)
                  {
                    case StringSetExpression:
                    case EmptySetExpression:
                      validCombination = true;
                      break;
                    default:
                      validCombination = false;
                      break;
                  }
                break;
                
              case IntegerCriterion:
                switch (argumentType)
                  {
                    case IntegerSetExpression:
                    case EmptySetExpression:
                      validCombination = true;
                      break;
                    default:
                      validCombination = false;
                      break;
                  }
                break;
                
              default:
                validCombination = false;
                break;
            }
          break;

        case ContainsOperator:
        case DoesNotContainOperator:
          switch (criterionField.getFieldDataType())
            {
              case StringSetCriterion:
                switch (argumentType)
                  {
                    case StringExpression:
                      validCombination = true;
                      break;
                    default:
                      validCombination = false;
                      break;
                  }
                break;
                
              default:
                validCombination = false;
                break;
            }
          break;
          
        case NonEmptyIntersectionOperator:
        case EmptyIntersectionOperator:
          switch (criterionField.getFieldDataType())
            {
              case StringSetCriterion:
                switch (argumentType)
                  {
                    case StringSetExpression:
                    case EmptySetExpression:
                      validCombination = true;
                      break;
                    default:
                      validCombination = false;
                      break;
                  }
                break;
                
              default:
                validCombination = false;
                break;
            }
          break;
      }
    if (!validCombination) throw new CriterionException("bad operator/dataType/argument combination " + this.criterionOperator + "/" + criterionField.getFieldDataType() + "/" + argumentExpression);
  }

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public CriterionContext getCriterionContext() { return criterionContext; }
  public CriterionField getCriterionField() { return criterionField; }
  public CriterionOperator getCriterionOperator() { return criterionOperator; }
  public String getArgumentExpression() { return argumentExpression; }
  public TimeUnit getArgumentBaseTimeUnit() { return argumentBaseTimeUnit; }
  public Expression getArgument() { return argument; }
  public String getStoryReference() { return storyReference; }
  public boolean getCriterionDefault() { return criterionDefault; }

  /*****************************************
  *
  *  serde
  *
  *****************************************/

  public static ConnectSerde<EvaluationCriterion> serde()
  {
    return new ConnectSerde<EvaluationCriterion>(schema, false, EvaluationCriterion.class, EvaluationCriterion::pack, EvaluationCriterion::unpack);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    EvaluationCriterion criterion = (EvaluationCriterion) value;
    Struct struct = new Struct(schema);
    struct.put("criterionContext", criterion.getCriterionContext().getExternalRepresentation());
    struct.put("criterionField", criterion.getCriterionField().getCriterionFieldName());
    struct.put("criterionOperator", criterion.getCriterionOperator().getExternalRepresentation());
    struct.put("argumentExpression", criterion.getArgumentExpression());
    struct.put("argumentBaseTimeUnit", criterion.getArgumentBaseTimeUnit().getExternalRepresentation());
    struct.put("storyReference", criterion.getStoryReference());
    struct.put("criterionDefault", criterion.getCriterionDefault());
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static EvaluationCriterion unpack(SchemaAndValue schemaAndValue)
  {
    //
    //  data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? schema.version() : null;

    //
    //  unpack all but argument
    //

    Struct valueStruct = (Struct) value;
    CriterionContext criterionContext = CriterionContext.fromExternalRepresentation(valueStruct.getString("criterionContext"));
    CriterionField criterionField = Deployment.getCriterionFields(criterionContext).get(valueStruct.getString("criterionField"));
    CriterionOperator criterionOperator = CriterionOperator.fromExternalRepresentation(valueStruct.getString("criterionOperator"));
    String argumentExpression = valueStruct.getString("argumentExpression");
    TimeUnit argumentBaseTimeUnit = TimeUnit.fromExternalRepresentation(valueStruct.getString("argumentBaseTimeUnit"));
    String storyReference = valueStruct.getString("storyReference");
    boolean criterionDefault = valueStruct.getBoolean("criterionDefault");

    //
    //  validate
    //

    if (criterionField == null) throw new SerializationException("unknown " + criterionContext.getExternalRepresentation() + " criterion field: " + valueStruct.getString("criterionField"));

    //
    //  construct
    //

    EvaluationCriterion result = new EvaluationCriterion(criterionContext, criterionField, criterionOperator, argumentExpression, argumentBaseTimeUnit, storyReference, criterionDefault);

    //
    //  parse argument
    //

     try
      {
        result.parseArgument();
      }
    catch (ExpressionParseException|ExpressionTypeCheckException e)
      {
        throw new SerializationException("invalid argument expression " + argumentExpression, e);
      }
    
    //
    //  validate
    //

    try
      {
        result.validate();
      }
    catch (CriterionException e)
      {
        throw new SerializationException("invalid criterion", e);
      }

    //
    //  return
    //

    return result;
  }
  
  /*****************************************
  *
  *  evaluate
  *
  *****************************************/

  public boolean evaluate(SubscriberEvaluationRequest evaluationRequest)
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

        criterionFieldValue = criterionField.retrieve(evaluationRequest);
        
        /*****************************************
        *
        *  validate dataType
        *
        *****************************************/

        if (criterionFieldValue != null)
          {
            switch (criterionField.getFieldDataType())
              {
                case IntegerCriterion:
                  if (criterionFieldValue instanceof Integer) criterionFieldValue = new Long(((Integer) criterionFieldValue).longValue());
                  if (! (criterionFieldValue instanceof Long)) throw new CriterionException("criterionField " + criterionField + " expected integer retrieved " + criterionFieldValue.getClass());
                  break;

                case DoubleCriterion:
                  if (criterionFieldValue instanceof Float) criterionFieldValue = new Double(((Float) criterionFieldValue).doubleValue());
                  if (! (criterionFieldValue instanceof Double)) throw new CriterionException("criterionField " + criterionField + " expected double retrieved " + criterionFieldValue.getClass());
                  break;

                case StringCriterion:
                  if (! (criterionFieldValue instanceof String)) throw new CriterionException("criterionField " + criterionField + " expected string retrieved " + criterionFieldValue.getClass());
                  break;

                case BooleanCriterion:
                  if (! (criterionFieldValue instanceof Boolean)) throw new CriterionException("criterionField " + criterionField + " expected boolean retrieved " + criterionFieldValue.getClass());
                  break;

                case DateCriterion:
                  if (! (criterionFieldValue instanceof Date)) throw new CriterionException("criterionField " + criterionField + " expected date retrieved " + criterionFieldValue.getClass());
                  break;

                case StringSetCriterion:
                  if (! (criterionFieldValue instanceof Set)) throw new CriterionException("criterionField " + criterionField + " expected set retrieved " + criterionFieldValue.getClass());
                  for (Object object : (Set<Object>) criterionFieldValue)
                    {
                      if (! (object instanceof String)) throw new CriterionException("criterionField " + criterionField + " expected set of string retrieved " + object.getClass());
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
            switch (criterionField.getFieldDataType())
              {
                case StringCriterion:
                  String stringFieldValue = (String) criterionFieldValue;
                  criterionFieldValue = stringFieldValue.toLowerCase();
                  break;
                  
                case StringSetCriterion:
                  Set<String> normalizedStringSetFieldValue = new HashSet<String>();
                  for (String stringValue : (Set<String>) criterionFieldValue)
                    {
                      normalizedStringSetFieldValue.add(stringValue.toLowerCase());
                    }
                  criterionFieldValue = normalizedStringSetFieldValue;
                  break;
              }
          }
      }
    catch (CriterionException e)
      {
        log.info("invalid criterion field {}", criterionField.getCriterionFieldName());
        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.info(stackTraceWriter.toString());
        evaluationRequest.subscriberTrace("TrueCondition : criterionField {0} not supported", criterionField.getCriterionFieldName());
        return true;
      }

    /****************************************
    *
    *  handle null values
    *
    ****************************************/

    switch (criterionOperator)
      {
        case IsNullOperator:
        case IsNotNullOperator:
          break;

        default:
          if (criterionFieldValue == null)
            {
              evaluationRequest.subscriberTrace((criterionDefault ? "TrueCondition : " : "FalseCondition: ") + "DefaultCriterion {0} {1} value {2} argument {3}", criterionField.getCriterionFieldName(), criterionOperator, criterionFieldValue, argument);
              return criterionDefault;
            }
          break;
      }
    
    /****************************************
    *
    *  evaluate argument
    *
    ****************************************/

    Object evaluatedArgument = null;
    try
      {
        evaluatedArgument = (argument != null) ? argument.evaluate(evaluationRequest, argumentBaseTimeUnit) : null;
      }
    catch (ArithmeticException e)
      {
        log.info("invalid argument {}", argumentExpression);
        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.info(stackTraceWriter.toString());
        evaluationRequest.subscriberTrace("FalseCondition : invalid argument {0}", argumentExpression);
        return false;
      }
    catch (ExpressionEvaluationException e)
      {
        log.info("invalid argument {}", argumentExpression);
        StringWriter stackTraceWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stackTraceWriter, true));
        log.info(stackTraceWriter.toString());
        evaluationRequest.subscriberTrace("FalseCondition : invalid argument {0}", argumentExpression);
        return false;
      }

    /****************************************
    *
    *  handle null argument
    *
    ****************************************/

    switch (criterionOperator)
      {
        case IsNullOperator:
        case IsNotNullOperator:
          break;

        default:
          if (evaluatedArgument == null)
            {
              evaluationRequest.subscriberTrace("FalseCondition : invalid null argument {0}", argumentExpression);
              return false;
            }
          break;
      }

    /*****************************************
    *
    *  normalize
    *
    *****************************************/

    CriterionDataType evaluationDataType = criterionField.getFieldDataType();
    if (criterionFieldValue != null && evaluatedArgument != null)
      {
        switch (criterionField.getFieldDataType())
          {
            case IntegerCriterion:
              switch (argument.getType())
                {
                  case DoubleExpression:
                    criterionFieldValue = new Double(((Long) criterionFieldValue).doubleValue());
                    evaluationDataType = CriterionDataType.DoubleCriterion;
                    break;
                }
              break;

            case DoubleCriterion:
              switch (argument.getType())
                {
                  case IntegerExpression:
                    evaluatedArgument = new Double(((Long) evaluatedArgument).doubleValue());
                    evaluationDataType = CriterionDataType.DoubleCriterion;
                    break;
                }
              break;

            case StringCriterion:
            case StringSetCriterion:
              switch (argument.getType())
                {
                  case StringExpression:
                    String stringArgument = (String) evaluatedArgument;
                    evaluatedArgument = stringArgument.toLowerCase();
                    break;
                    
                  case StringSetExpression:
                    Set<String> normalizedStringSetArgument = new HashSet<String>();
                    for (String stringValue : (Set<String>) evaluatedArgument)
                      {
                        normalizedStringSetArgument.add(stringValue.toLowerCase());
                      }
                    evaluatedArgument = normalizedStringSetArgument;
                    break;
                }
              break;

            case DateCriterion:
              {
                switch (argumentBaseTimeUnit)
                  {
                    case Instant:
                      break;
                    case Minute:
                      criterionFieldValue = RLMDateUtils.truncate((Date) criterionFieldValue, Calendar.MINUTE, Calendar.SUNDAY, Deployment.getBaseTimeZone());
                      break;
                    case Hour:
                      criterionFieldValue = RLMDateUtils.truncate((Date) criterionFieldValue, Calendar.HOUR, Calendar.SUNDAY, Deployment.getBaseTimeZone());
                      break;
                    case Day:
                      criterionFieldValue = RLMDateUtils.truncate((Date) criterionFieldValue, Calendar.DATE, Calendar.SUNDAY, Deployment.getBaseTimeZone());
                      break;
                    case Week:
                      criterionFieldValue = RLMDateUtils.truncate((Date) criterionFieldValue, Calendar.DAY_OF_WEEK, Calendar.SUNDAY, Deployment.getBaseTimeZone());
                      break;
                    case Month:
                      criterionFieldValue = RLMDateUtils.truncate((Date) criterionFieldValue, Calendar.MONTH, Calendar.SUNDAY, Deployment.getBaseTimeZone());
                      break;
                    case Year:
                      criterionFieldValue = RLMDateUtils.truncate((Date) criterionFieldValue, Calendar.YEAR, Calendar.SUNDAY, Deployment.getBaseTimeZone());
                      break;
                  }
              }
          }
      }
        
    /****************************************
    *
    *  evaluate
    *
    ****************************************/

    boolean result = false;
    switch (criterionOperator)
      {
        /*****************************************
        *
        *  equality operators
        *
        *****************************************/

        case EqualOperator:
          result = traceCondition(evaluationRequest, criterionFieldValue.equals(evaluatedArgument), criterionFieldValue, evaluatedArgument);
          break;
          
        case NotEqualOperator:
          result = traceCondition(evaluationRequest, !criterionFieldValue.equals(evaluatedArgument), criterionFieldValue, evaluatedArgument);
          break;
          
        /*****************************************
        *
        *  relational operators
        *
        *****************************************/

        case GreaterThanOperator:
          switch (evaluationDataType)
            {
              case IntegerCriterion:
                result = traceCondition(evaluationRequest, ((Long) criterionFieldValue).compareTo((Long) evaluatedArgument) > 0, criterionFieldValue, evaluatedArgument);
                break;
              case DoubleCriterion:
                result = traceCondition(evaluationRequest, ((Double) criterionFieldValue).compareTo((Double) evaluatedArgument) > 0, criterionFieldValue, evaluatedArgument);
                break;
              case DateCriterion:
                result = traceCondition(evaluationRequest, ((Date) criterionFieldValue).compareTo((Date) evaluatedArgument) > 0, criterionFieldValue, evaluatedArgument);
                break;
            }
          break;
          
        case GreaterThanOrEqualOperator:
          switch (evaluationDataType)
            {
              case IntegerCriterion:
                result = traceCondition(evaluationRequest, ((Long) criterionFieldValue).compareTo((Long) evaluatedArgument) >= 0, criterionFieldValue, evaluatedArgument);
                break;
              case DoubleCriterion:
                result = traceCondition(evaluationRequest, ((Double) criterionFieldValue).compareTo((Double) evaluatedArgument) >= 0, criterionFieldValue, evaluatedArgument);
                break;
              case DateCriterion:
                result = traceCondition(evaluationRequest, ((Date) criterionFieldValue).compareTo((Date) evaluatedArgument) >= 0, criterionFieldValue, evaluatedArgument);
                break;
            }
          break;

        case LessThanOperator:
          switch (evaluationDataType)
            {
              case IntegerCriterion:
                result = traceCondition(evaluationRequest, ((Long) criterionFieldValue).compareTo((Long) evaluatedArgument) < 0, criterionFieldValue, evaluatedArgument);
                break;
              case DoubleCriterion:
                result = traceCondition(evaluationRequest, ((Double) criterionFieldValue).compareTo((Double) evaluatedArgument) < 0, criterionFieldValue, evaluatedArgument);
                break;
              case DateCriterion:
                result = traceCondition(evaluationRequest, ((Date) criterionFieldValue).compareTo((Date) evaluatedArgument) < 0, criterionFieldValue, evaluatedArgument);
                break;
            }
          break;

        case LessThanOrEqualOperator:
          switch (evaluationDataType)
            {
              case IntegerCriterion:
                result = traceCondition(evaluationRequest, ((Long) criterionFieldValue).compareTo((Long) evaluatedArgument) <= 0, criterionFieldValue, evaluatedArgument);
                break;
              case DoubleCriterion:
                result = traceCondition(evaluationRequest, ((Double) criterionFieldValue).compareTo((Double) evaluatedArgument) <= 0, criterionFieldValue, evaluatedArgument);
                break;
              case DateCriterion:
                result = traceCondition(evaluationRequest, ((Date) criterionFieldValue).compareTo((Date) evaluatedArgument) <= 0, criterionFieldValue, evaluatedArgument);
                break;
            }
          break;

        /*****************************************
        *
        *  isNull operators
        *
        *****************************************/

        case IsNullOperator:
          result = traceCondition(evaluationRequest, criterionFieldValue == null, criterionFieldValue, evaluatedArgument);
          break;
          
        case IsNotNullOperator:
          result = traceCondition(evaluationRequest, criterionFieldValue != null, criterionFieldValue, evaluatedArgument);
          break;

        /*****************************************
        *
        *  set operators
        *
        *****************************************/

        case IsInSetOperator:
          switch (evaluationDataType)
            {
              case StringCriterion:
                result = traceCondition(evaluationRequest, ((Set<String>) evaluatedArgument).contains((String) criterionFieldValue), criterionFieldValue, evaluatedArgument);
                break;
              case IntegerCriterion:
                result = traceCondition(evaluationRequest, ((Set<Integer>) evaluatedArgument).contains((Integer) criterionFieldValue), criterionFieldValue, evaluatedArgument);
                break;
            }
          break;
          
        case NotInSetOperator:
          switch (evaluationDataType)
            {
              case StringCriterion:
                result = traceCondition(evaluationRequest, !((Set<String>) evaluatedArgument).contains((String) criterionFieldValue), criterionFieldValue, evaluatedArgument);
                break;
              case IntegerCriterion:
                result = traceCondition(evaluationRequest, !((Set<Integer>) evaluatedArgument).contains((Integer) criterionFieldValue), criterionFieldValue, evaluatedArgument);
                break;
            }
          break;
          
        case ContainsOperator:
          switch (evaluationDataType)
            {
              case StringSetCriterion:
                result = traceCondition(evaluationRequest, ((Set<String>) criterionFieldValue).contains((String) evaluatedArgument), criterionFieldValue, evaluatedArgument);
                break;
            }
          break;
          
        case DoesNotContainOperator:
          switch (evaluationDataType)
            {
              case StringSetCriterion:
                result = traceCondition(evaluationRequest, !((Set<String>) criterionFieldValue).contains((String) evaluatedArgument), criterionFieldValue, evaluatedArgument);
                break;
            }
          break;
          
        case NonEmptyIntersectionOperator:
          switch (evaluationDataType)
            {
              case StringSetCriterion:
                result = traceCondition(evaluationRequest, (new HashSet<String>((Set<String>) criterionFieldValue)).removeAll((Set<String>) evaluatedArgument), criterionFieldValue, evaluatedArgument);
                break;
            }
          break;
          
        case EmptyIntersectionOperator:
          switch (evaluationDataType)
            {
              case StringSetCriterion:
                result = traceCondition(evaluationRequest, !(new HashSet<String>((Set<String>) criterionFieldValue)).removeAll((Set<String>) evaluatedArgument), criterionFieldValue, evaluatedArgument);
                break;
            }
          break;
      }
    
    /****************************************
    *
    *  return
    *
    ****************************************/

    return result;
  }
  
  /*****************************************
  *
  *  evaluateCriteria
  *
  *****************************************/

  public static boolean evaluateCriteria(SubscriberEvaluationRequest evaluationRequest, List<EvaluationCriterion> criteria)
  {
    boolean result = true;
    for (EvaluationCriterion criterion : criteria)
      {
        result = result && criterion.evaluate(evaluationRequest);
      }
    return result;
  }

  /*****************************************
  *
  *  traceCondition
  *
  *****************************************/

  private boolean traceCondition(SubscriberEvaluationRequest evaluationRequest, boolean condition, Object value, Object evaluatedArgument)
  {
    evaluationRequest.subscriberTrace((condition ? "TrueCondition : " : "FalseCondition: ") + "Criterion {0} {1} value {2} argument {3}", criterionField.getCriterionFieldName(), criterionOperator, value, evaluatedArgument);
    return condition;
  }

  /*****************************************
  *
  *  equals
  *
  *****************************************/

  public boolean equals(Object obj)
  {
    boolean result = false;
    if (obj instanceof EvaluationCriterion)
      {
        EvaluationCriterion evaluationCriterion = (EvaluationCriterion) obj;
        result = true;
        result = result && Objects.equals(criterionContext, evaluationCriterion.getCriterionContext());
        result = result && Objects.equals(criterionField, evaluationCriterion.getCriterionField());
        result = result && Objects.equals(criterionOperator, evaluationCriterion.getCriterionOperator());
        result = result && Objects.equals(argumentExpression, evaluationCriterion.getArgumentExpression());
        result = result && Objects.equals(argumentBaseTimeUnit, evaluationCriterion.getArgumentBaseTimeUnit());
        result = result && Objects.equals(storyReference, evaluationCriterion.getStoryReference());
        result = result && Objects.equals(criterionDefault, evaluationCriterion.getCriterionDefault());
        result = result && Objects.equals(argument, evaluationCriterion.getArgument());
      }
    return result;
  }
  
  /*****************************************
  *
  *  class CriterionException
  *
  *****************************************/

  public static class CriterionException extends GUIManagerException
  {
    public CriterionException(String message) { super(message, null); }
    public CriterionException(Throwable e) { super(e); }
  }

  /*****************************************************************************
  *
  *  expressions
  *
  *****************************************************************************/

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
  *  class Expression
  *
  *****************************************/

  public static abstract class Expression
  {
    /*****************************************
    *
    *  data
    *
    *****************************************/

    protected ExpressionDataType type;
    protected String nodeID;
    
    /*****************************************
    *
    *  abstract
    *
    *****************************************/

    public abstract void typeCheck(TimeUnit baseTimeUnit);
    public abstract int assignNodeID(int preorderNumber);
    public boolean isConstant() { return false; }
    public abstract Object evaluate(SubscriberEvaluationRequest subscriberEvaluationRequest, TimeUnit baseTimeUnit);
    public abstract void esQuery(StringBuilder script, TimeUnit baseTimeUnit) throws CriterionException;

    /*****************************************
    *
    *  accessors
    *
    *****************************************/

    public ExpressionDataType getType() { return type; }
    public String getNodeID() { return nodeID; }

    /*****************************************
    *
    *  setType
    *
    *****************************************/

    public void setType(ExpressionDataType type)
    {
      this.type = type;
    }

    /*****************************************
    *
    *  setNodeID
    *
    *****************************************/

    public void setNodeID(int preorderNumber)
    {
      this.nodeID = Integer.toString(preorderNumber);
    }

    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    protected Expression()
    {
      this.type = null;
      this.nodeID = null;
    }
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

    @Override public void typeCheck(TimeUnit baseTimeUnit) { }

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

    @Override public Object evaluate(SubscriberEvaluationRequest subscriberEvaluationRequest, TimeUnit baseTimeUnit)
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
            script.append("def right_" + getNodeID() + " = " + ((Long) constant).toString() + "; ");
            break;

          case DoubleExpression:
            script.append("def right_" + getNodeID() + " = " + ((Double) constant).toString() + "; ");
            break;

          case StringExpression:
            script.append("def right_" + getNodeID() + " = \"" + ((String) constant) + "\"; ");
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

    @Override public void typeCheck(TimeUnit baseTimeUnit)
    {
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
          default:
            throw new ExpressionTypeCheckException("invariant violated");
        }
    }

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

    @Override public Object evaluate(SubscriberEvaluationRequest subscriberEvaluationRequest, TimeUnit baseTimeUnit)
    {
      //
      //  retrieve
      //

      Object referenceValue = reference.retrieve(subscriberEvaluationRequest);
      
      //
      //  null check
      //

      if (referenceValue == null) throw new ExpressionEvaluationException(reference);

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
  	    script.append("def right_" + getNodeID() + " = doc." + esField + ".value; ");
            break;
            
          case StringSetExpression:
          case IntegerSetExpression:
  	    script.append("def right_" + getNodeID() + " = new ArrayList(); right_" + getNodeID() + ".addAll(doc." + esField + "); ");
            break;
            
          case DateExpression:
            script.append("def rightSF_" + getNodeID() + " = new SimpleDateFormat(\"yyyy-MM-dd'T'HH:mm:ss.SSSX\"); ");
            script.append("def rightMillis_" + getNodeID() + " = doc." + esField + ".value.getMillis(); ");
            script.append("def rightCalendar_" + getNodeID() +" = rightSF_" + getNodeID() + ".getCalendar(); ");
            script.append("rightCalendar_" + getNodeID() + ".setTimeInMillis(rightMillis_" + getNodeID() + "); ");
            script.append("def rightInstant_" + getNodeID() + " = rightCalendar_" + getNodeID() + ".toInstant(); ");
            script.append("def rightRaw_" + getNodeID() + " = LocalDateTime.ofInstant(rightInstant_" + getNodeID() + ", ZoneOffset.UTC); ");
            script.append(constructDateTruncateESScript(getNodeID(), "rightRaw", "right", baseTimeUnit));
            break;
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

    @Override public void typeCheck(TimeUnit baseTimeUnit)
    {
      /*****************************************
      *
      *  typeCheck arguments
      *
      *****************************************/

      leftArgument.typeCheck(baseTimeUnit);
      rightArgument.typeCheck(baseTimeUnit);

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

    @Override public Object evaluate(SubscriberEvaluationRequest subscriberEvaluationRequest, TimeUnit baseTimeUnit)
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

    @Override public void typeCheck(TimeUnit baseTimeUnit)
    {
      /*****************************************
      *
      *  typeCheck arguments
      *
      *****************************************/

      unaryArgument.typeCheck(baseTimeUnit);

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

    @Override public Object evaluate(SubscriberEvaluationRequest subscriberEvaluationRequest, TimeUnit baseTimeUnit)
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
                  result = new Long(-1L * ((Long) argumentValue).longValue());
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

    @Override public void typeCheck(TimeUnit baseTimeUnit)
    {
      /*****************************************
      *
      *  typeCheck arguments
      *
      *****************************************/

      for (Expression argument : arguments)
        {
          argument.typeCheck(baseTimeUnit);
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
            if (! arg3.isConstant()) throw new ExpressionTypeCheckException("type exception");
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
      
      String arg3Value = (String) arg3.evaluate(null, TimeUnit.Unknown);
      switch (TimeUnit.fromExternalRepresentation(arg3Value))
        {
          case Instant:
          case Unknown:
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

    @Override public Object evaluate(SubscriberEvaluationRequest subscriberEvaluationRequest, TimeUnit baseTimeUnit)
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
            result = evaluateDateAddFunction((Date) arg1Value, (Long) arg2Value, TimeUnit.fromExternalRepresentation((String) arg3Value), baseTimeUnit);
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

    private Date evaluateDateAddFunction(Date date, Long number, TimeUnit timeUnit, TimeUnit baseTimeUnit)
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
      script.append(constructDateTruncateESScript(getNodeID(), "rightBeforeTruncate", "right", baseTimeUnit));
    }
    
    /*****************************************
    *
    *  esQueryDateAddFunction
    *
    *****************************************/

    private void esQueryDateAddFunction(StringBuilder script, TimeUnit baseTimeUnit) throws CriterionException
    {
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
      script.append(constructDateTruncateESScript(getNodeID(), "rightRawInstantBeforeAdd", "rightInstantBeforeAdd", baseTimeUnit));

      //
      //  add time interval
      //
      
      TimeUnit timeUnit = TimeUnit.fromExternalRepresentation((String) arguments.get(2).evaluate(null, TimeUnit.Unknown));
      script.append("def rightRawInstant_" + getNodeID() + " = rightInstantBeforeAdd_" + getNodeID() + ".plus(right_" + arguments.get(1).getNodeID() + ", ChronoUnit." + timeUnit.getChronoUnit() + "); ");
      
      //
      //  truncate (after adding)
      //

      script.append(constructDateTruncateESScript(getNodeID(), "rightRawInstant", "rightInstant", baseTimeUnit));

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
  *  esQuery
  *
  *****************************************/

  QueryBuilder esQuery() throws CriterionException
  {
    /*****************************************
    *
    *  esField
    *
    *****************************************/

    String esField = criterionField.getESField();
    if (esField == null)
      {
        throw new CriterionException("invalid criterionField " + criterionField);
      }

    /*****************************************
    *
    *  script
    *
    ****************************************/

    StringBuilder script = new StringBuilder();

    /*****************************************
    *
    *  left -- generate code to evaluate left 
    *
    *****************************************/

    CriterionDataType evaluationDataType = criterionField.getFieldDataType();
    switch (evaluationDataType)
      {
        case DateCriterion:
          script.append("def leftSF = new SimpleDateFormat(\"yyyy-MM-dd'T'HH:mm:ss.SSSX\"); ");
          script.append("def leftMillis = doc." + esField + ".value.getMillis(); ");
          script.append("def leftCalendar = leftSF.getCalendar(); ");
          script.append("leftCalendar.setTimeInMillis(leftMillis); ");
          script.append("def leftInstant = leftCalendar.toInstant(); ");
          script.append("def leftBeforeTruncate = LocalDateTime.ofInstant(leftInstant, ZoneOffset.UTC); ");
          script.append(constructDateTruncateESScript(null, "leftBeforeTruncate", "left", argumentBaseTimeUnit));
          break;

        case StringSetCriterion:
        case IntegerSetCriterion:
          script.append("def left = new ArrayList(); left.addAll(doc." + esField + "); ");
          break;
          
        default:
          script.append("def left = doc." + esField + ".value; ");
          break;
      }

    /*****************************************
    *
    *  right -- generate code to evaluate right
    *
    *****************************************/

    if (argument != null)
      {
        argument.esQuery(script, argumentBaseTimeUnit);
        script.append("def right = right_0; ");
      }

    /*****************************************
    *
    *  operator -- generate code to evaluate the operator (using left and right)
    *
    *****************************************/

    switch (criterionOperator)
      {
        /*****************************************
        *
        *  equality operators
        *
        *****************************************/

        case EqualOperator:
          script.append("return left == right; ");
          break;

        case NotEqualOperator:
          script.append("return left != right; ");
          break;

        /*****************************************
        *
        *  relational operators
        *
        *****************************************/

        case GreaterThanOperator:
          switch (evaluationDataType)
            {
              case DateCriterion:
                script.append("return left.isAfter(right); ");
                break;

              default:
                script.append("return left > right; ");
                break;
            }
          break;

        case GreaterThanOrEqualOperator:
          switch (evaluationDataType)
            {
              case DateCriterion:
                script.append("return !left.isBefore(right); ");
                break;

              default:
                script.append("return left >= right; ");
                break;
            }
          break;

        case LessThanOperator:
          switch (evaluationDataType)
            {
              case DateCriterion:
                script.append("return left.isBefore(right); ");
                break;

              default:
                script.append("return left < right; ");
                break;
            }
          break;

        case LessThanOrEqualOperator:
          switch (evaluationDataType)
            {
              case DateCriterion:
                script.append("return !left.isAfter(right); ");
                break;

              default:
                script.append("return left <= right; ");
                break;
            }
          break;

        /*****************************************
        *
        *  isNull operators
        *
        *****************************************/

        case IsNullOperator:
          script.append("return left == null; ");
          break;

        case IsNotNullOperator:
          script.append("return left != null; ");
          break;

        /*****************************************
        *
        *  set operators
        *
        *****************************************/

        case IsInSetOperator:
          switch (argument.getType())
            {
              case StringSetExpression:
              case EmptySetExpression:
                script.append("return right.contains(left); ");
                break;

              case IntegerSetExpression:
                script.append("def found = false; for (int i=0;i<right.size();i++) found = (found || right.get(i) == left); return found; ");
                break;
            }
          break;

        case NotInSetOperator:
          switch (argument.getType())
            {
              case StringSetExpression:
              case EmptySetExpression:
                script.append("return !right.contains(left); ");
                break;

              case IntegerSetExpression:
                script.append("def found = false; for (int i=0;i<right.size();i++) found = (found || right.get(i) == left); return !found; ");
                break;
            }
          break;

        case ContainsOperator:
          switch (evaluationDataType)
            {
              case StringSetCriterion:
                script.append("return left.contains(right); ");
                break;

              case IntegerSetCriterion:
                script.append("def found = false; for (int i=0;i<left.size();i++) found = (found || left.get(i) == right); return found; ");
                break;
            }
          break;

        case DoesNotContainOperator:
          switch (evaluationDataType)
            {
              case StringSetCriterion:
                script.append("return !left.contains(right); ");
                break;

              case IntegerSetCriterion:
                script.append("def found = false; for (int i=0;i<left.size();i++) found = (found || left.get(i) == right); return !found; ");
                break;
            }
          break;

	case NonEmptyIntersectionOperator:
	  script.append("left.retainAll(right); return !left.isEmpty(); ");
	  break;
          
	case EmptyIntersectionOperator:
	  script.append("left.retainAll(right); return left.isEmpty(); ");
	  break;
          
        /*****************************************
        *
        *  default
        *
        *****************************************/

        default:
          throw new UnsupportedOperationException(criterionOperator.getExternalRepresentation());
      }

    /*****************************************
    *
    *  log painless script
    *
    *****************************************/
    
    log.info("painless script: {}", script.toString());
    
    /*****************************************
    *
    *  script query
    *
    *****************************************/
    
    Map<String, Object> parameters = Collections.<String, Object>emptyMap();
    QueryBuilder baseQuery = QueryBuilders.scriptQuery(new Script(ScriptType.INLINE, "painless", script.toString(), parameters));

    /*****************************************
    *
    *  criterionDefault
    *
    *****************************************/
    
    QueryBuilder query;
    switch (criterionOperator)
      {
        case IsNullOperator:
        case IsNotNullOperator:
          query = baseQuery;
          break;

        default:
          if (criterionDefault)
            query = QueryBuilders.boolQuery().should(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(esField))).should(baseQuery);
          else
            query = baseQuery;
          break;
      }
    
    /*****************************************
    *
    *  return
    *
    *****************************************/
    
    return query;
  }

  /****************************************
  *
  *  constructDateTruncateESScript
  *
  ****************************************/

  private static String constructDateTruncateESScript(String nodeIDArg, String rawPrefix, String finalPrefix, TimeUnit timeUnit)
  {
    String nodeID = (nodeIDArg != null) ? new String("_" + nodeIDArg) : "";
    String result = null;
    switch (timeUnit)
      {
        case Instant:
          result = "def " + finalPrefix + nodeID + " = " + rawPrefix + nodeID + "; ";
          break;
          
        case Minute:
        case Hour:
        case Day:
        case Week:
        case Month:
        case Year:
          result = "def " + finalPrefix + nodeID + " = " + rawPrefix + nodeID + ".truncatedTo(ChronoUnit." + timeUnit.getChronoUnit() + "); ";
          break;
      }
    return result;
  }

  /*****************************************************************************
  *
  *  parseArgument
  *
  *****************************************************************************/

  private void parseArgument() throws ExpressionParseException, ExpressionTypeCheckException
  {
    if (argumentExpression != null)
      {
        /*****************************************
        *
        *  pass 1 -- parse argumentExpression
        *
        *****************************************/

        try
          {
            reader = new StringReader(argumentExpression);
            argument = parseExpression();
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
        *  pass 2 -- typecheck argument
        *
        *****************************************/

        argument.typeCheck(argumentBaseTimeUnit);

        /*****************************************
        *
        *  pass 3 --assignNodeID to argument
        *
        *****************************************/

        argument.assignNodeID(0);
      }
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
          CriterionField criterionField = Deployment.getCriterionFields(criterionContext).get(identifier);
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
          break;
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
          break;
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
          parseError(tokenPosition,"expected <primary>");
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
}
