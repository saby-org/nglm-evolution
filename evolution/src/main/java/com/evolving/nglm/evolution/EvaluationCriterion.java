/*****************************************************************************
*
*  EvaluationCriterion.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Type;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.index.query.*;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.EvolutionUtilities.TimeUnit;
import com.evolving.nglm.evolution.Expression.ExpressionContext;
import com.evolving.nglm.evolution.Expression.ExpressionDataType;
import com.evolving.nglm.evolution.Expression.ExpressionEvaluationException;
import com.evolving.nglm.evolution.Expression.ExpressionParseException;
import com.evolving.nglm.evolution.Expression.ExpressionReader;
import com.evolving.nglm.evolution.Expression.ExpressionTypeCheckException;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.elasticsearch.ElasticsearchClientAPI;

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
    TimeCriterion("time"),
    AniversaryCriterion("aniversary"),
    StringSetCriterion("stringSet"),
    
    //
    //  only for parameters
    //

    EvaluationCriteriaParameter("evaluationCriteria"),
    WorkflowParameter("workflow"),
    
    //
    // only notification parameters (hardcoded sms, email and push, and generic types)
    // 
    
    SMSMessageParameter("smsMessage"),
    EmailMessageParameter("emailMessage"),
    PushMessageParameter("pushMessage"),
    
    Dialog("dialog"),

    NotificationStringParameter("template_string"),
    NotificationHTMLStringParameter("template_html_string"),

    //
    //  only for criterionArguments
    //

    NumberCriterion("number"),
    NoArgumentCriterion("noArgument"),
    IntegerSetCriterion("integerSet"),
    DoubleSetCriterion("doubleSet"),

    //
    //  structure
    //

    Unknown("(unknown)");
    private String externalRepresentation;
    private CriterionDataType(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static CriterionDataType fromExternalRepresentation(String externalRepresentation) { for (CriterionDataType enumeratedValue : CriterionDataType.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
    public boolean compatibleWith(CriterionDataType dataType) {
      if(this.equals(DoubleCriterion) && dataType.equals(IntegerCriterion)) {
        return true;
      }
      else {
        return this.equals(dataType);
      }
    }

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
          case DoubleSetCriterion:
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
    ContainsKeywordOperator("contains keyword"),
    DoesNotContainsKeywordOperator("doesn't contains keyword"),
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
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(2));
    schemaBuilder.field("criterionContext", CriterionContext.schema());
    schemaBuilder.field("criterionField", Schema.STRING_SCHEMA);
    schemaBuilder.field("criterionOperator", Schema.STRING_SCHEMA);
    schemaBuilder.field("argumentExpression", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("argumentBaseTimeUnit", Schema.STRING_SCHEMA);
    schemaBuilder.field("storyReference", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("criterionDefault", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("useESQueryNoPainless",Schema.OPTIONAL_BOOLEAN_SCHEMA);
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
  private boolean referencesEvaluationDate;

  private Boolean useESQueryNoPainless;

  /*****************************************
  *
  *  constructor -- simple
  *
  *****************************************/

  private EvaluationCriterion(CriterionContext criterionContext, CriterionField criterionField, CriterionOperator criterionOperator, String argumentExpression, TimeUnit argumentBaseTimeUnit, String storyReference, boolean criterionDefault,Boolean useESQueryNoPainless)
  {
    this.criterionContext = criterionContext;
    this.criterionField = criterionField;
    this.criterionOperator = criterionOperator;
    this.argumentExpression = argumentExpression;
    this.argumentBaseTimeUnit = argumentBaseTimeUnit;
    this.storyReference = storyReference;
    this.criterionDefault = criterionDefault;
    this.argument = null;
    this.referencesEvaluationDate = criterionField.getID().equals(CriterionField.EvaluationDateField);
    this.useESQueryNoPainless = useESQueryNoPainless;
  }

  /*****************************************
  *
  *  constructor -- external JSON
  *
  *****************************************/

  public EvaluationCriterion(JSONObject jsonRoot, CriterionContext criterionContext) throws GUIManagerException
  {
    //
    //  basic fields (all but argument)
    //

    this.criterionContext = criterionContext;
    this.criterionField = criterionContext.getCriterionFields().get(JSONUtilities.decodeString(jsonRoot, "criterionField", true));
    this.criterionOperator = CriterionOperator.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "criterionOperator", true));
    this.storyReference = JSONUtilities.decodeString(jsonRoot, "storyReference", false);
    this.criterionDefault = JSONUtilities.decodeBoolean(jsonRoot, "criterionDefault", Boolean.FALSE);
    this.referencesEvaluationDate = (this.criterionField != null) && this.criterionField.getID().equals(CriterionField.EvaluationDateField);

    //
    //  validate (all but argument)
    //
    
    if (this.criterionField == null) throw new GUIManagerException("unsupported " + criterionContext.getCriterionContextType().getExternalRepresentation() + " criterion field", JSONUtilities.decodeString(jsonRoot, "criterionField", true));
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

    this.useESQueryNoPainless = JSONUtilities.decodeBoolean(jsonRoot, "useESQueryNoPainless");

    //
    //  validate
    //

    try
      {
        validate();
      }
    catch (CriterionException e)
      {
        log.info("invalid criterion for field {}", criterionField.getID());
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
                
              case AniversaryCriterion:  
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
                
              case TimeCriterion:
                switch (argumentType)
                  {
                    case TimeExpression:
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
                
              case AniversaryCriterion:
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
                
              case TimeCriterion:
                switch (argumentType)
                  {
                    case TimeExpression:
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
          
        case ContainsKeywordOperator:
          switch (criterionField.getFieldDataType())
            {
              case StringCriterion:
                switch (argumentType)
                  {
                    case StringExpression:
                      validCombination = true;
                      break;
                  }
                break;

              default:
                validCombination = false;
                break;
            }
          break;
          
        case DoesNotContainsKeywordOperator:
          switch (criterionField.getFieldDataType())
            {
              case StringCriterion:
                switch (argumentType)
                  {
                    case StringExpression:
                      validCombination = true;
                      break;
                  }
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
  public Boolean getUseESQueryNoPainless(){return  useESQueryNoPainless;}

  /*****************************************
   *
   *  setters
   *
   *****************************************/
  //this method was defined especially for extracts to be sure that extracts
  public void setUseESQueryNoPainless(Boolean useESQueryNoPainless)
  {
    this.useESQueryNoPainless = useESQueryNoPainless;
  }

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
    struct.put("criterionContext", CriterionContext.pack(criterion.getCriterionContext()));
    struct.put("criterionField", criterion.getCriterionField().getID());
    struct.put("criterionOperator", criterion.getCriterionOperator().getExternalRepresentation());
    struct.put("argumentExpression", criterion.getArgumentExpression());
    struct.put("argumentBaseTimeUnit", criterion.getArgumentBaseTimeUnit().getExternalRepresentation());
    struct.put("storyReference", criterion.getStoryReference());
    struct.put("criterionDefault", criterion.getCriterionDefault());
    struct.put("useESQueryNoPainless",criterion.getUseESQueryNoPainless());
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
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;

    //
    //  unpack all but argument
    //

    Struct valueStruct = (Struct) value;
    CriterionContext criterionContext = CriterionContext.unpack(new SchemaAndValue(schema.field("criterionContext").schema(), valueStruct.get("criterionContext")));
    CriterionField criterionField = criterionContext.getCriterionFields().get(valueStruct.getString("criterionField"));
    CriterionOperator criterionOperator = CriterionOperator.fromExternalRepresentation(valueStruct.getString("criterionOperator"));
    String argumentExpression = valueStruct.getString("argumentExpression");
    TimeUnit argumentBaseTimeUnit = TimeUnit.fromExternalRepresentation(valueStruct.getString("argumentBaseTimeUnit"));
    String storyReference = valueStruct.getString("storyReference");
    boolean criterionDefault = valueStruct.getBoolean("criterionDefault");
    Boolean useESQueryNoPainless = schemaVersion >=2 ? valueStruct.getBoolean("useESQueryNoPainless"):null;

    //
    //  validate
    //

    if (criterionField == null) throw new SerializationException("unknown " + criterionContext.getCriterionContextType().getExternalRepresentation() + " criterion field: " + valueStruct.getString("criterionField"));

    //
    //  construct
    //

    EvaluationCriterion result = new EvaluationCriterion(criterionContext, criterionField, criterionOperator, argumentExpression, argumentBaseTimeUnit, storyReference, criterionDefault,useESQueryNoPainless);

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
  *  parseArgument
  *
  *****************************************/

  public void parseArgument() throws ExpressionParseException, ExpressionTypeCheckException
  {
    ExpressionReader expressionReader = new ExpressionReader(criterionContext, argumentExpression, argumentBaseTimeUnit);
    argument = expressionReader.parse(ExpressionContext.Criterion);
  }

  /*****************************************
  *
  *  evaluate
  *
  *****************************************/

  public boolean evaluate(SubscriberEvaluationRequest evaluationRequest)
  {
    /*****************************************
    *
    *  result
    *
    *****************************************/

    boolean result = false;

    /****************************************
    *
    *  retrieve fieldValue
    *
    ****************************************/
    Object criterionFieldValue = null;
    Object evaluatedArgument = null;
    ExpressionDataType argumentType = null;
    try
      {        
        criterionFieldValue = criterionField.retrieveNormalized(evaluationRequest);
    
        /****************************************
        *
        *  evaluate argument
        *
        ****************************************/    
        argumentType = (argument != null) ? argument.getType() : ExpressionDataType.NoArgument;        
        evaluatedArgument = (argument != null) ? argument.evaluateExpression(evaluationRequest, argumentBaseTimeUnit) : null;
      }
    catch (Exception e)
      {
        if (log.isDebugEnabled())
          {
            log.debug("EvaluationCriterion.evaluate Exception " + e.getClass().getName() + " while evaluating criterionField {} and argumentExpression {}", criterionField, argumentExpression);
          }
        evaluationRequest.subscriberTrace("FalseCondition : invalid argument {0}", argumentExpression);
        return false;
      }

    /*****************************************
    *
    *  handle evaluation variables
    *
    *****************************************/

    if (criterionField.getEvaluationVariable())
      {
        evaluationRequest.getEvaluationVariables().put((String) criterionFieldValue, evaluatedArgument);
        result = traceCondition(evaluationRequest, true, criterionFieldValue, evaluatedArgument);
        return result;
      }

    /****************************************
    *
    *  handle null field
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
              evaluationRequest.subscriberTrace((criterionDefault ? "TrueCondition : " : "FalseCondition: ") + "DefaultCriterion {0} {1} value {2} argument {3}", criterionField.getID(), criterionOperator, criterionFieldValue, evaluatedArgument);
              return criterionDefault;
            }
          break;
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
    *  normalize integer/longs
    *
    *****************************************/

    switch (argumentType)
      {
        case IntegerExpression:
          if (evaluatedArgument instanceof Integer) evaluatedArgument = new Long(((Integer) evaluatedArgument).longValue());
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
              switch (argumentType)
                {
                  case DoubleExpression:
                    criterionFieldValue = new Double(((Number) criterionFieldValue).doubleValue());
                    evaluationDataType = CriterionDataType.DoubleCriterion;
                    break;
                }
              break;

            case DoubleCriterion:
              switch (argumentType)
                {
                  case IntegerExpression:
                    evaluatedArgument = new Double(((Number) evaluatedArgument).doubleValue());
                    evaluationDataType = CriterionDataType.DoubleCriterion;
                    break;
                }
              break;

            case StringCriterion:
            case StringSetCriterion:
              switch (argumentType)
                {
                  case StringExpression:
                    String stringArgument = (String) evaluatedArgument;
                    evaluatedArgument = (stringArgument != null) ? stringArgument.toLowerCase() : stringArgument;
                    break;
                    
                  case StringSetExpression:
                    Set<String> normalizedStringSetArgument = new HashSet<String>();
                    for (String stringValue : (Set<String>) evaluatedArgument)
                      {
                        normalizedStringSetArgument.add((stringValue != null) ? stringValue.toLowerCase() : stringValue);
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
            break;
              
          case AniversaryCriterion:
            {
              evaluatedArgument = RLMDateUtils.truncate((Date) evaluatedArgument, Calendar.DATE, Deployment.getBaseTimeZone());
              criterionFieldValue = RLMDateUtils.truncate((Date) criterionFieldValue, Calendar.DATE, Deployment.getBaseTimeZone());
              int yearOfEvaluatedArgument = RLMDateUtils.getField((Date) evaluatedArgument, Calendar.YEAR, Deployment.getBaseTimeZone());
              criterionFieldValue = RLMDateUtils.setField((Date) criterionFieldValue, Calendar.YEAR, yearOfEvaluatedArgument, Deployment.getBaseTimeZone());
              break;
            }
              
          case TimeCriterion:
            {
              Date now = SystemTime.getCurrentTime();
              criterionFieldValue = getCurrentDateFromTime(now, (String) criterionFieldValue);
              evaluatedArgument = getCurrentDateFromTime(now, (String) evaluatedArgument);
            }
          }
      }
    
    /****************************************
    *
    *  evaluate
    *
    ****************************************/

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
                
              case TimeCriterion:
              case AniversaryCriterion:
              case DateCriterion:
                result = traceCondition(evaluationRequest, ((Date) criterionFieldValue).compareTo((Date) evaluatedArgument) > 0, criterionFieldValue, evaluatedArgument);
                if (referencesEvaluationDate) evaluationRequest.getNextEvaluationDates().add((Date) evaluatedArgument);
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
                
              case TimeCriterion:
              case AniversaryCriterion:
              case DateCriterion:
                result = traceCondition(evaluationRequest, ((Date) criterionFieldValue).compareTo((Date) evaluatedArgument) >= 0, criterionFieldValue, evaluatedArgument);
                if (referencesEvaluationDate) evaluationRequest.getNextEvaluationDates().add((Date) evaluatedArgument);
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
              
              case TimeCriterion:
              case AniversaryCriterion:
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
                
              case TimeCriterion:
              case AniversaryCriterion:
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
        *  containsKeyword operator
        *
        *****************************************/

        case ContainsKeywordOperator:
          result = traceCondition(evaluationRequest, evaluateContainsKeyword((String) criterionFieldValue, (String) evaluatedArgument), criterionFieldValue, evaluatedArgument);
          break;
          
        /*****************************************
        *
        *  doesNotContainsKeywordOperator operator
        *
        *****************************************/
          
        case DoesNotContainsKeywordOperator:
          result = traceCondition(evaluationRequest, evaluateDoesNotContainsKeyword((String) criterionFieldValue, (String) evaluatedArgument), criterionFieldValue, evaluatedArgument);
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
  
  //
  // getCurrentDateFromTime
  //
  
  private Date getCurrentDateFromTime(final Date now, String arg)
  {
    String[] args = ((String) arg).trim().split(":");
    if (args.length != 3) throw new ExpressionEvaluationException();
    int hh = Integer.parseInt(args[0]);
    int mm = Integer.parseInt(args[1]);
    int ss = Integer.parseInt(args[2]);
    Calendar c = SystemTime.getCalendar();
    c.setTime(now);
    c.set(Calendar.HOUR_OF_DAY, hh);
    c.set(Calendar.MINUTE, mm);
    c.set(Calendar.SECOND, ss);
    return c.getTime();
  }

  /*****************************************
  *
  *  evaluateCriteria
  *
  *****************************************/

  public static boolean evaluateCriteria(SubscriberEvaluationRequest evaluationRequest, List<EvaluationCriterion> criteria)
  {
    //
    //  log
    //

    if (evaluationRequest.getSubscriberTraceEnabled())
      {
        boolean firstCriterion = true;
        StringBuilder b = new StringBuilder();
        b.append("evaluateCriteria [ ");
        b.append(criteria.toString());
        b.append(" ]");
        evaluationRequest.subscriberTrace("{0}", b.toString());
      }

    //
    //  clear evaluationVariables
    //

    evaluationRequest.getEvaluationVariables().clear();

    //
    //  evaluate
    //

    boolean result = true;
    for (EvaluationCriterion criterion : criteria)
    result = result && criterion.evaluate(evaluationRequest);
      
    return result;
  }
  
  /*****************************************
  *
  *  esCountMatchCriteria
  *
  *****************************************/
  //
  // construct query
  //
  public static BoolQueryBuilder esCountMatchCriteriaGetQuery(List<EvaluationCriterion> criteriaList) throws CriterionException {
    BoolQueryBuilder query = QueryBuilders.boolQuery();
    for (EvaluationCriterion evaluationCriterion : criteriaList)
      {
        query = query.filter(evaluationCriterion.esQuery());
      }
    
    return query;
  }
  
  //
  // execute query
  //
  public static long esCountMatchCriteriaExecuteQuery(BoolQueryBuilder query, ElasticsearchClientAPI elasticsearch) throws IOException, ElasticsearchStatusException {
    CountRequest countRequest = new CountRequest("subscriberprofile").query(query);
    CountResponse countResponse = elasticsearch.count(countRequest, RequestOptions.DEFAULT);
    return countResponse.getCount();
  }

  /*****************************************
  *
  *  traceCondition
  *
  *****************************************/

  private boolean traceCondition(SubscriberEvaluationRequest evaluationRequest, boolean condition, Object value, Object evaluatedArgument)
  {
    evaluationRequest.subscriberTrace((condition ? "TrueCondition : " : "FalseCondition: ") + "Criterion {0} {1} value {2} argument {3}", criterionField.getID(), criterionOperator, value, evaluatedArgument);
    return condition;
  }

  /*****************************************
  *
  *  evaluateContainsKeyword
  *
  *****************************************/

  //
  //  generateContainsKeywordRegex
  //

  private String generateContainsKeywordRegex(String words)
  {
    Pattern topLevelPattern = Pattern.compile("(\"([^\"]+)\")|(\\S+)");
    Matcher topLevelMatcher = topLevelPattern.matcher(words);
    StringBuilder result = new StringBuilder();
    while (topLevelMatcher.find())
      {
        //
        //  pattern for one "word"
        //

        String wordPattern;
        if (topLevelMatcher.group(1) != null)
          {
            Pattern singleWordPattern = Pattern.compile("\\S+");
            Matcher singleWordMatcher = singleWordPattern.matcher(topLevelMatcher.group(2));
            StringBuilder wordPatternBuilder = new StringBuilder();
            while (singleWordMatcher.find())
              {
                if (wordPatternBuilder.length() > 0) wordPatternBuilder.append("\\s+");
                wordPatternBuilder.append(Pattern.quote(singleWordMatcher.group(0)));
              }
            wordPattern = wordPatternBuilder.toString();
          }
        else
          {
            wordPattern = Pattern.quote(topLevelMatcher.group(3));
          }

        //
        //  add pattern for "word"
        //

        if (result.length() > 0) result.append("|");
        result.append("((^|\\s)" + wordPattern + "(\\s|$))");
      }
    return result.toString();
  }

  //
  //  evaluateContainsKeyword
  //

  private boolean evaluateContainsKeyword(String data, String words)
  {
    //
    //  regex
    //

    String regex = generateContainsKeywordRegex(words);

    //
    //  match
    //

    Pattern p = Pattern.compile(regex);
    Matcher m = p.matcher(data);

    //
    //  result
    //

    return m.find();
  }
  
  //
  //  evaluateDoesNotContainsKeyword
  //

  private boolean evaluateDoesNotContainsKeyword(String data, String words) { return !evaluateContainsKeyword(data, words); }

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

    //
    // Handle criterion "loyaltyprogram.name"
    //

    if ("loyaltyprogram.name".equals(esField))
    {
      QueryBuilder query = null;
      // ES special case for isNull : (must_not -> exists) does not work when inside a nested query : must_not must be on the toplevel query !
      switch (criterionOperator)
      {
      case IsNullOperator:
        query = QueryBuilders.boolQuery().mustNot(
            QueryBuilders.nestedQuery("loyaltyPrograms",
                QueryBuilders.existsQuery("loyaltyPrograms.loyaltyProgramName") , ScoreMode.Total));
        break;

      case IsNotNullOperator:
      default:
        query = QueryBuilders.boolQuery().filter(
            QueryBuilders.nestedQuery("loyaltyPrograms",
                buildCompareQuery("loyaltyPrograms.loyaltyProgramName", ExpressionDataType.StringExpression) , ScoreMode.Total));
        break;
      }
      return query;
    }

    //
    // Handle dynamic criterion "loyaltyprogram.LP1.xxxxx"
    //

    if (esField.startsWith("loyaltyprogram."))
    {
      QueryBuilder query = handleLoyaltyProgramDynamicCriterion(esField);
      return query;
    }

    //
    // Handle dynamic criterion "point.POINT001.balance"
    //

    if (esField.startsWith("point."))
    {
      QueryBuilder query = handlePointDynamicCriterion(esField);
      return query;
    }

    //
    // Handle dynamic criterion "campaign.name, journey.customer.name..."
    //

    if (esField.startsWith("specialCriterion"))
    {
      QueryBuilder query = handleSpecialCriterion(esField);
      return query;
    }

    //
    // Handle targets
    //

    if ("internal.targets".equals(esField))
    {
      QueryBuilder query = handleTargetsCriterion(esField);
      return query;
    }

    //base on criterion settings decide which es query will be used (with painless or not)
    //the way how criterion will be evaluated can come from json request or can be defined into CriterionField
    //if defined in request CriterionField value will be ignored
    boolean evaluateNoQuery;
    //if criterion is not comming from request it will be completed with valued defined in CriterionField (CriterionField follows the usual pattern false if not defined)
    if(useESQueryNoPainless == null)
    {
      evaluateNoQuery = criterionField.getUseESQueryNoPainless();
    }
    else
    {
      evaluateNoQuery = useESQueryNoPainless.booleanValue();
    }
    if(evaluateNoQuery)
    {
      return noPainlessEsQuery(esField);
    }
    else
    {
      return painlessEsQuery(esField);
    }
  }

  /*****************************************
   *
   *  painlessEsQuery
   *
   *****************************************/

  private QueryBuilder painlessEsQuery(String esField) throws CriterionException
  {

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
        case StringCriterion:
          script.append("def left = (doc." + esField + ".size() != 0) ? doc." + esField + ".value?.toLowerCase() : null; ");
          break;
          
        case DateCriterion:
          script.append("def left; ");
          script.append("if (doc." + esField + ".size() != 0) { ");
          script.append("def leftSF = new SimpleDateFormat(\"yyyy-MM-dd'T'HH:mm:ss.SSSX\"); ");
          script.append("def leftMillis = doc." + esField + ".value.getMillis(); ");
          script.append("def leftCalendar = leftSF.getCalendar(); ");
          script.append("leftCalendar.setTimeInMillis(leftMillis); ");
          script.append("def leftInstant = leftCalendar.toInstant(); ");
          script.append("def leftBeforeTruncate = LocalDateTime.ofInstant(leftInstant, ZoneOffset.UTC); ");
          script.append(constructDateTruncateESScript(null, "leftBeforeTruncate", "tempLeft", argumentBaseTimeUnit));
          script.append("left = tempLeft; } ");
          break;

        case StringSetCriterion:
          script.append("def left = new ArrayList(); for (int i=0;i<doc." + esField + ".size();i++) left.add(doc." + esField + ".get(i)?.toLowerCase()); ");
          break;

        case IntegerSetCriterion:
          script.append("def left = new ArrayList(); left.addAll(doc." + esField + "); ");
          break;
          
        case AniversaryCriterion:
          throw new UnsupportedOperationException("AniversaryCriterion is not supported");
          
        case TimeCriterion:
          throw new UnsupportedOperationException("timeCriterion is not supported");
          
        default:
          script.append("def left = (doc." + esField + ".size() != 0) ? doc." + esField + "?.value : null; ");
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
        switch (argument.getType())
          {
            case StringExpression:
              script.append("def right = right_0?.toLowerCase(); ");
              break;
              
            case StringSetExpression:
              script.append("def right = new ArrayList(); for (int i=0;i<right_0.size();i++) right.add(right_0.get(i)?.toLowerCase()); ");
              break;

            default:
              script.append("def right = right_0; ");
              break;
          }
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
          script.append("return (left != null) ? left == right : false; ");
          break;

        case NotEqualOperator:
          script.append("return (left != null) ? left != right : false; ");
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
                script.append("return (left != null) ? left.isAfter(right) : false; ");
                break;
              case AniversaryCriterion:
                throw new UnsupportedOperationException("AniversaryCriterion is not supported");
              case TimeCriterion:
                throw new UnsupportedOperationException("timeCriterion is not supported");
                
              default:
                script.append("return (left != null) ? left > right : false; ");
                break;
            }
          break;

        case GreaterThanOrEqualOperator:
          switch (evaluationDataType)
            {
              case DateCriterion:
                script.append("return (left != null) ? !left.isBefore(right) : true; ");
                break;
                
              case AniversaryCriterion:
                throw new UnsupportedOperationException("AniversaryCriterion is not supported");

              case TimeCriterion:
                throw new UnsupportedOperationException("timeCriterion is not supported");
                
              default:
                script.append("return (left != null) ? left >= right : false; ");
                break;
            }
          break;

        case LessThanOperator:
          switch (evaluationDataType)
            {
              case DateCriterion:
                script.append("return (left != null) ? left.isBefore(right) : false; ");
                break;
                
              case AniversaryCriterion:
                throw new UnsupportedOperationException("AniversaryCriterion is not supported");

              case TimeCriterion:
                throw new UnsupportedOperationException("timeCriterion is not supported");
                
              default:
                script.append("return (left != null) ? left < right : false; ");
                break;
            }
          break;

        case LessThanOrEqualOperator:
          switch (evaluationDataType)
            {
              case DateCriterion:
                script.append("return (left != null) ? !left.isAfter(right) : true; ");
                break;
                
              case AniversaryCriterion:
                throw new UnsupportedOperationException("AniversaryCriterion is not supported");
                
              case TimeCriterion:
                throw new UnsupportedOperationException("timeCriterion is not supported");
                
              default:
                script.append("return (left != null) ? left <= right : false; ");
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
        *  containsKeyword operator
        *
        *****************************************/

        case ContainsKeywordOperator:
        case DoesNotContainsKeywordOperator:

          //
          //  argument must be constant to evaluate esQuery
          //

          if (! argument.isConstant())
            {
              throw new CriterionException("containsKeyword invalid (non-constant) argument");
            }

          //
          //  evaluate constant right hand-side
          //

          String argumentValue = (String) argument.evaluateExpression(null, TimeUnit.Unknown);

          //
          //  script
          //

          script.append("return left =~ /" + generateContainsKeywordRegex(argumentValue) + "/; ");

          //
          //  break
          //

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
    
    if (log.isDebugEnabled()) log.debug("painless script: {}", script.toString());
    
    /*****************************************
    *
    *  script query
    *
    *****************************************/
    
    Map<String, Object> parameters = Collections.<String, Object>emptyMap();
    QueryBuilder baseQuery = QueryBuilders.scriptQuery(new Script(ScriptType.INLINE, "painless", script.toString(), parameters));
    //QueryBuilders.

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
          
        case DoesNotContainsKeywordOperator:
            query = QueryBuilders.boolQuery().must(QueryBuilders.existsQuery(esField)).mustNot(baseQuery);
          break;
          

        default:
          if (criterionDefault)
            query = QueryBuilders.boolQuery().should(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(esField))).should(baseQuery);
          else
            query = QueryBuilders.boolQuery().must(QueryBuilders.existsQuery(esField)).must(baseQuery);
          break;
      }
    
    /*****************************************
    *
    *  return
    *
    *****************************************/
    
    return query;
  }

  /*****************************************
   *
   *  noPainlessEsQuery
   *
   *****************************************/

  private QueryBuilder noPainlessEsQuery(String esField) throws CriterionException
  {

    /*****************************************
     *
     *  query
     *
     ****************************************/
    QueryBuilder queryBuilder = null;

    /*****************************************
     *
     *  left -- generate code to evaluate left
     *
     *****************************************/

    //verify is null or not
    switch (criterionOperator)
    {
    case IsNullOperator:
      return QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(esField));
    case IsNotNullOperator:
      return QueryBuilders.existsQuery(esField);
    }

    CriterionDataType evaluationDataType = criterionField.getFieldDataType();
    switch (evaluationDataType)
    {
    case BooleanCriterion:
    {
      switch (criterionOperator)
      {
      case EqualOperator:
        queryBuilder = QueryBuilders.termQuery(esField,argument.esQueryNoPainless());
        break;
      case NotEqualOperator:
        TermQueryBuilder termQuery = QueryBuilders.termQuery(esField,argument.esQueryNoPainless());
        queryBuilder = QueryBuilders.boolQuery().mustNot(termQuery);
        break;
      default:
        throw new UnsupportedOperationException("Operation "+criterionOperator.getExternalRepresentation()+" not supported for "+evaluationDataType.getExternalRepresentation());
      }
    }
    break;

    case StringCriterion:
    {
      switch (criterionOperator)
      {
      case EqualOperator:
        queryBuilder = QueryBuilders.termQuery(esField,argument.esQueryNoPainless());
        break;
      case NotEqualOperator:
        TermQueryBuilder termQuery = QueryBuilders.termQuery(esField,argument.esQueryNoPainless());
        queryBuilder = QueryBuilders.boolQuery().mustNot(termQuery);
        break;
      case ContainsKeywordOperator:
      case DoesNotContainsKeywordOperator:
        if (! argument.isConstant())
        {
          throw new CriterionException("containsKeyword invalid (non-constant) argument");
        }
        if(argumentExpression.isEmpty())
        {
          throw new CriterionException("Operation "+criterionOperator.getExternalRepresentation()+" not allowed for empty argument");
        }
        queryBuilder = QueryBuilders.regexpQuery(esField,"@"+argument.evaluateExpression(null,null)+"@");
        if(criterionOperator == CriterionOperator.DoesNotContainsKeywordOperator)
        {
          queryBuilder =  QueryBuilders.boolQuery().mustNot(queryBuilder);
        }
        break;
      case IsInSetOperator:
      {
        Object argumentObject = argument.esQueryNoPainless();
        queryBuilder = new TermsQueryBuilder(esField, (Set<String>) argumentObject);
        break;
      }
      case NotInSetOperator:
      {
        Object argumentObject = argument.esQueryNoPainless();
        queryBuilder = QueryBuilders.boolQuery().mustNot(new TermsQueryBuilder(esField, (Set<String>) argumentObject));
        break;
      }
      default:
        throw new UnsupportedOperationException("Operation "+criterionOperator.getExternalRepresentation()+" not supported for "+evaluationDataType.getExternalRepresentation());
      }
      break;
    }

    case IntegerCriterion:
    case DoubleCriterion:
    {
      //operations can be performed in ES only by using painless script for the moment
      if(argument instanceof Expression.OperatorExpression || argument instanceof Expression.UnaryExpression || argument instanceof Expression.ReferenceExpression)
      {
        return painlessEsQuery(esField);
      }
      else
      {
        RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder(esField);
        switch (criterionOperator)
        {
        case EqualOperator:
          queryBuilder = QueryBuilders.termQuery(esField, argument.esQueryNoPainless());
          break;
        case NotEqualOperator:
          TermQueryBuilder termQuery = QueryBuilders.termQuery(esField, argument.esQueryNoPainless());
          queryBuilder = QueryBuilders.boolQuery().mustNot(termQuery);
          break;
        case GreaterThanOrEqualOperator:
          rangeQueryBuilder.gte(argument.esQueryNoPainless());
          queryBuilder = rangeQueryBuilder;
          break;
        case GreaterThanOperator:
          rangeQueryBuilder.gt(argument.esQueryNoPainless());
          queryBuilder = rangeQueryBuilder;
          break;
        case LessThanOperator:
          rangeQueryBuilder.lt(argument.esQueryNoPainless());
          queryBuilder = rangeQueryBuilder;
          break;
        case LessThanOrEqualOperator:
          rangeQueryBuilder.lte(argument.esQueryNoPainless());
          queryBuilder = rangeQueryBuilder;
          break;
        case IsInSetOperator:
        {
          //double will not get here because is rejected by validate
          Object argumentObject = argument.esQueryNoPainless();
          queryBuilder = new TermsQueryBuilder(esField, (Set<Integer>) argumentObject);
          break;
        }
        case NotInSetOperator:
        {
          //double will not get here because is rejected by validate
          Object argumentObject = argument.esQueryNoPainless();
          queryBuilder = QueryBuilders.boolQuery().mustNot(new TermsQueryBuilder(esField, (Set<Integer>) argumentObject));
          break;
        }
        default:
          throw new UnsupportedOperationException(
              "Operation " + criterionOperator.getExternalRepresentation() + " not supported for " + evaluationDataType.getExternalRepresentation());
        }
      }
      break;
    }


    case DateCriterion:
    {
      RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder(esField);
      rangeQueryBuilder.timeZone(Deployment.getBaseTimeZone());
      if(argument instanceof Expression.OperatorExpression || argument instanceof Expression.UnaryExpression || argument instanceof Expression.ReferenceExpression)
      {
        return painlessEsQuery(esField);
      }
      else
      {
        switch (criterionOperator)
        {
        case EqualOperator:
          rangeQueryBuilder.from(argument.esQueryNoPainless());
          rangeQueryBuilder.to(argument.esQueryNoPainless());
          queryBuilder = rangeQueryBuilder;
          break;
        case NotEqualOperator:
          rangeQueryBuilder.from(argument.esQueryNoPainless());
          rangeQueryBuilder.to(argument.esQueryNoPainless());
          queryBuilder = QueryBuilders.boolQuery().mustNot(rangeQueryBuilder);
          break;
        case GreaterThanOrEqualOperator:
          rangeQueryBuilder.from(argument.esQueryNoPainless(), true);
          queryBuilder = rangeQueryBuilder;
          break;
        case GreaterThanOperator:
          rangeQueryBuilder.from(argument.esQueryNoPainless(), false);
          queryBuilder = rangeQueryBuilder;
          break;
        case LessThanOperator:
          rangeQueryBuilder.to(argument.esQueryNoPainless(), false);
          queryBuilder = rangeQueryBuilder;
          break;
        case LessThanOrEqualOperator:
          rangeQueryBuilder.to(argument.esQueryNoPainless(), true);
          queryBuilder = rangeQueryBuilder;
          break;
        }
      }
    }
    break;

    case StringSetCriterion:
    {
      Object argumentObject = argument.esQueryNoPainless();
      QueryBuilder stringSetQueryBuilder;
      if (argumentObject instanceof String)
      {
        stringSetQueryBuilder = new TermQueryBuilder(esField, (String) argumentObject);
      }
      else if (argumentObject instanceof Set)
      {
        stringSetQueryBuilder = new TermsQueryBuilder(esField, (Set<String>) argumentObject);
      }
      else
      {
        throw new UnsupportedOperationException(
            "Argument type" + argumentObject.getClass().toString() + " not supported for " + evaluationDataType.getExternalRepresentation());
      }
      switch (criterionOperator)
      {
      case NonEmptyIntersectionOperator:
      case ContainsOperator:
        queryBuilder = stringSetQueryBuilder;
        break;
      case EmptyIntersectionOperator:
      case DoesNotContainOperator:
        queryBuilder = QueryBuilders.boolQuery().mustNot(stringSetQueryBuilder);
        break;
      }
      break;
    }

    case IntegerSetCriterion:
    {
      Object argumentObject = argument.esQueryNoPainless();
      QueryBuilder stringSetQueryBuilder;
      if (argumentObject instanceof String)
      {
        stringSetQueryBuilder = new TermQueryBuilder(esField, (Integer) argumentObject);
      }
      else if (argumentObject instanceof Set)
      {
        stringSetQueryBuilder = new TermsQueryBuilder(esField, (Set<Integer>) argumentObject);
      }
      else
      {
        throw new UnsupportedOperationException(
            "Argument type" + argumentObject.getClass().toString() + " not supported for " + evaluationDataType.getExternalRepresentation());
      }
      switch (criterionOperator)
      {
      case NonEmptyIntersectionOperator:
      case ContainsOperator:
        queryBuilder = stringSetQueryBuilder;
        break;
      case EmptyIntersectionOperator:
      case DoesNotContainOperator:
        queryBuilder = QueryBuilders.boolQuery().mustNot(stringSetQueryBuilder);
        break;
      }
      break;
    }

    case AniversaryCriterion:
      throw new UnsupportedOperationException("AniversaryCriterion is not supported");

    case TimeCriterion:
      throw new UnsupportedOperationException("timeCriterion is not supported");

    default:
      //script.append("def left = (doc." + esField + ".size() != 0) ? doc." + esField + "?.value : null; ");
      break;
    }

    /*****************************************
     *
     *  return
     *
     *****************************************/

    return queryBuilder;
  }

  static String journeyName = "";
  static String campaignName = "";
  static String bulkcampaignName = "";
  
  /*****************************************
  *
  *  handleSpecialOtherCriterion
  *
  *****************************************/
  
  public QueryBuilder handleSpecialCriterion(String esField) throws CriterionException
  {
    Pattern fieldNamePattern = Pattern.compile("^specialCriterion([^.]+)$");
    Matcher fieldNameMatcher = fieldNamePattern.matcher(esField);
    if (! fieldNameMatcher.find()) throw new CriterionException("invalid special criterion field " + esField);
    String criterion = fieldNameMatcher.group(1);
    // TODO : necessary ? To be checked
    if (!(argument instanceof Expression.ConstantExpression)) throw new CriterionException("dynamic criterion can only be compared to constants " + esField + ", " + argument);
    String value = "";
    switch (criterion)
    {
      case "Journey":
        journeyName = (String) (argument.evaluate(null, null));
        return QueryBuilders.matchAllQuery();
        
      case "Campaign":
        campaignName = (String) (argument.evaluate(null, null));
        return QueryBuilders.matchAllQuery();
        
      case "Bulkcampaign":
        bulkcampaignName = (String) (argument.evaluate(null, null));
        return QueryBuilders.matchAllQuery();
        
      case "JourneyStatus":
        value = journeyName;
        break;
        
      case "CampaignStatus":
        value = campaignName;
        break;
        
      case "BulkcampaignStatus":
        value = bulkcampaignName;
        break;
        
      default:
        throw new CriterionException("unknown criteria : " + esField);
    }
    
    QueryBuilder queryID = QueryBuilders.termQuery("subscriberJourneys.journeyID", value);
    QueryBuilder queryStatus = null;
    QueryBuilder query = null;
    QueryBuilder insideQuery = null;
    boolean isNot = false;
    
    switch (criterionOperator)
    {
      case NotInSetOperator:
        isNot = true;
        // fallthrough
      case IsInSetOperator:
        /*
        {
          "query": {
            "nested" :
            {
              "path" : "subscriberJourneys",
              "query" :{
                "bool": {
                  "must": [
                    {
                      "term": {
                        "subscriberJourneys.journeyID": "1104"
                      }
                    }
                  ],
                  "should": [
                    {
                      "term": {
                        "subscriberJourneys.status": "other"
                      }
                    },
                    {
                      "term": {
                        "subscriberJourneys.status": "entered"
                      }
                    }
                  ],
                  "minimum_should_match": 1
                }
              }
            }
          }
        }
        */
        queryStatus = buildCompareQuery("subscriberJourneys.status", ExpressionDataType.StringSetExpression);
        if (!(queryStatus instanceof BoolQueryBuilder))
          {
            throw new CriterionException("BoolQueryBuilder expected, got " + queryStatus.getClass().getName());
          }
        BoolQueryBuilder boolQuery = (BoolQueryBuilder) queryStatus;
        BoolQueryBuilder insideQueryBool = QueryBuilders.boolQuery().must(queryID).minimumShouldMatch(1);
        for (QueryBuilder should : boolQuery.should())
          {
            insideQueryBool = insideQueryBool.should(should);
          }
        insideQuery = insideQueryBool;
        break;
        
      default:
        queryStatus = buildCompareQuery("subscriberJourneys.status", ExpressionDataType.StringExpression);
        insideQuery = QueryBuilders.boolQuery().must(queryID).must(queryStatus);
        break;
      }
    query = QueryBuilders.nestedQuery("subscriberJourneys", insideQuery, ScoreMode.Total);
    if (isNot)
      {
        query = QueryBuilders.boolQuery().mustNot(query);
      }
    return query;
  }

  /*****************************************
  *
  *  handleTargetsCriterion
  *
  * generates POST subscriberprofile/_search
      {
        "query": {
          "constant_score": {
            "filter": {
              "bool": {
                "should": [
                  { "term": { "targets": "Target_107"  }},
                  { "term": { "targets": "target_108" }}
                ]
              }
            }
          }
        }
      }
  *****************************************/
  
  public QueryBuilder handleTargetsCriterion(String esField) throws CriterionException
  {
    if (!(argument instanceof Expression.ConstantExpression)) throw new CriterionException("target criterion can only be compared to constants " + esField + ", " + argument);
    Object value =  argument.evaluate(null, null);
    BoolQueryBuilder innerQuery = QueryBuilders.boolQuery();
    String fieldName = "targets";
    if (argument.getType() == ExpressionDataType.StringExpression)
      {
        String val = (String) value;
        innerQuery = innerQuery.should(QueryBuilders.termQuery(fieldName, val));
      }
    else if (argument.getType() == ExpressionDataType.StringSetExpression)
      {
        for (Object obj : (Set<Object>) value)
          {
            innerQuery = innerQuery.should(QueryBuilders.termQuery(fieldName, (String) obj));
          }
      }
    else
      {
        throw new CriterionException(esField+" can only be compared to " + ExpressionDataType.StringExpression + " or " + ExpressionDataType.StringSetExpression + " " + esField + ", "+argument.getType());
      }
    QueryBuilder query = QueryBuilders.constantScoreQuery(innerQuery);
    return query;
  }
  
  /*****************************************
  *
  *  handlePointDynamicCriterion
  *
  *****************************************/
  
  public QueryBuilder handlePointDynamicCriterion(String esField) throws CriterionException
  {
    Pattern fieldNamePattern = Pattern.compile("^point\\.([^.]+)\\.(.+)$");
    Matcher fieldNameMatcher = fieldNamePattern.matcher(esField);
    if (! fieldNameMatcher.find()) throw new CriterionException("invalid point field " + esField);
    String pointID = fieldNameMatcher.group(1);
    String criterionFieldBaseName = fieldNameMatcher.group(2);
    QueryBuilder queryPointID = QueryBuilders.termQuery("pointBalances.pointID", pointID);
    QueryBuilder queryPointFluctuations = null;
    QueryBuilder queryInternal = null;
    switch (criterionFieldBaseName)
    {
      case "balance": // point.POINT_ID.balance
        queryInternal = buildCompareQuery("pointBalances." + SubscriberProfile.CURRENT_BALANCE, ExpressionDataType.IntegerExpression);
        break;

      case "earliestexpirydate": // point.POINT_ID.earliestexpirydate
        queryInternal = buildCompareQuery("pointBalances." + SubscriberProfile.EARLIEST_EXPIRATION_DATE, ExpressionDataType.DateExpression);
        break;

      case "earliestexpiryquantity": // point.POINT_ID.earliestexpiryquantity
        queryInternal = buildCompareQuery("pointBalances." + SubscriberProfile.EARLIEST_EXPIRATION_QUANTITY, ExpressionDataType.IntegerExpression);
        break;
        
      default:  // point.POINT_ID.expired.last7days
        String searchStringForPointFluctuations = "pointFluctuations." + pointID + ".";
        fieldNamePattern = Pattern.compile("^([^.]+)\\.([^.]+)$");
        fieldNameMatcher = fieldNamePattern.matcher(criterionFieldBaseName);
        if (! fieldNameMatcher.find()) throw new CriterionException("invalid criterionFieldBaseName field " + criterionFieldBaseName);
        String nature = fieldNameMatcher.group(1); // earned, consumed, expired
        String interval = fieldNameMatcher.group(2); // yesterday, last7days, last30days
        switch (interval)
        {
          case "yesterday":
          case "last7days":
          case "last30days":
            searchStringForPointFluctuations += interval + "."; // pointFluctuations.POINT_ID.yesterday.earned
            break;
          default: throw new CriterionException("invalid criterionField interval " + interval + " (should be yesterday, last7days, last30days)");
        }
        switch (nature)
        {
          case "earned"   :
          case "expired"  :
            searchStringForPointFluctuations += nature;
            break;

          case "consumed" :
            searchStringForPointFluctuations += "redeemed";  // different name in criteria and in ElasticSearch, don't know why (??)
            break;

          default: throw new CriterionException("invalid criterionField nature " + nature + " (should be earned, consumed, expired)");
        }
        return buildCompareQuery(searchStringForPointFluctuations, ExpressionDataType.IntegerExpression);
    }
    QueryBuilder query = QueryBuilders.nestedQuery("pointBalances",
            QueryBuilders.boolQuery()
            .filter(queryPointID)
            .filter(queryInternal), ScoreMode.Total);
    return query;
  }

  /*****************************************
  *
  *  handleLoyaltyProgramDynamicCriterion
  *
  *****************************************/

  public QueryBuilder handleLoyaltyProgramDynamicCriterion(String esField) throws CriterionException
  {
    Pattern fieldNamePattern = Pattern.compile("^loyaltyprogram\\.([^.]+)\\.(.+)$");
    Matcher fieldNameMatcher = fieldNamePattern.matcher(esField);
    if (! fieldNameMatcher.find()) throw new CriterionException("invalid loyaltyprogram field " + esField);
    String loyaltyProgramID = fieldNameMatcher.group(1);
    String criterionSuffix = fieldNameMatcher.group(2);
    QueryBuilder queryLPID = QueryBuilders.termQuery("loyaltyPrograms.programID", loyaltyProgramID);
    QueryBuilder query = null;
    switch (criterionSuffix)
    {
      case "tier":
        query = handleLoyaltyProgramField("loyaltyPrograms.tierName", esField, queryLPID, ExpressionDataType.StringExpression);
        break;

      case "statuspoint.balance":
        query = handleLoyaltyProgramField("loyaltyPrograms.statusPointBalance", esField, queryLPID, ExpressionDataType.IntegerExpression);
        break;

      case "rewardpoint.balance":
        query = handleLoyaltyProgramField("loyaltyPrograms.rewardPointBalance", esField, queryLPID, ExpressionDataType.IntegerExpression);
        break;
        
      case "tierupdatedate":
        query = handleLoyaltyProgramField("loyaltyPrograms.tierUpdateDate", esField, queryLPID, ExpressionDataType.DateExpression);
        break;

      case "optindate":
        query = handleLoyaltyProgramField("loyaltyPrograms.loyaltyProgramEnrollmentDate", esField, queryLPID, ExpressionDataType.DateExpression);
        break;

      case "optoutdate":
        query = handleLoyaltyProgramField("loyaltyPrograms.loyaltyProgramExitDate", esField, queryLPID, ExpressionDataType.DateExpression);
        break;

      case "tierupdatetype":
        query = handleLoyaltyProgramField("loyaltyPrograms.tierChangeType", esField, queryLPID, ExpressionDataType.StringExpression);
        break;

      default:
        Pattern pointsPattern = Pattern.compile("^([^.]+)\\.([^.]+)\\.(.+)$"); // "statuspoint.POINT001.earliestexpirydate"
        Matcher pointsMatcher = pointsPattern.matcher(criterionSuffix);
        if (! pointsMatcher.find()) throw new CriterionException("invalid criterionFieldBaseName field " + criterionSuffix);
        String pointKind = pointsMatcher.group(1); // statuspoint , rewardpoint
        String pointID = pointsMatcher.group(2); // POINT001
        String whatWeNeed = pointsMatcher.group(3); // earliestexpirydate , earliestexpiryquantity
        QueryBuilder queryPoint = QueryBuilders.termQuery("loyaltyPrograms."+(pointKind.equals("statuspoint")?"statusPointID":"rewardPointID"), pointID);           
        QueryBuilder queryExpiry = null;
        switch (whatWeNeed)
        {
          case "earliestexpirydate" :
            queryExpiry = handleEarliestExpiry("pointBalances."+SubscriberProfile.EARLIEST_EXPIRATION_DATE, esField, ExpressionDataType.DateExpression);
            break;

          case "earliestexpiryquantity" :
            queryExpiry = handleEarliestExpiry("pointBalances."+SubscriberProfile.EARLIEST_EXPIRATION_QUANTITY, esField, ExpressionDataType.IntegerExpression);
            break;

          default:
            throw new CriterionException("Internal error, unknown criterion field : " + esField);
        }
        query = QueryBuilders.boolQuery()
            .filter(QueryBuilders.nestedQuery("loyaltyPrograms",
                QueryBuilders.boolQuery()
                .filter(queryLPID)
                .filter(queryPoint), ScoreMode.Total))
            .filter(queryExpiry);
    }
    return query;
  }
  
  /*****************************************
  *
  *  handleEarliestExpiry
  *
  *****************************************/
  
  private QueryBuilder handleEarliestExpiry(String field, String esField, ExpressionDataType expectedType) throws CriterionException
  {
    if (argument.getType() != expectedType) throw new CriterionException(esField+" can only be compared to " + expectedType + " " + esField + ", "+argument.getType());
    QueryBuilder queryBalance = buildCompareQuery(field, expectedType);
    QueryBuilder query = QueryBuilders.nestedQuery("pointBalances",
        QueryBuilders.boolQuery().filter(queryBalance), ScoreMode.Total);
    return query;
  }

  /*****************************************
  *
  *  buildCompareQuery
  *
  *****************************************/
  
  private QueryBuilder buildCompareQuery(String field, ExpressionDataType expectedType) throws CriterionException
  {
    Object value = evaluateArgumentIfNecessary(expectedType);
    return buildCompareQueryWithValue(field, expectedType, value);
  }

  /*****************************************
  *
  *  buildCompareQueryWithValue
  *
  *****************************************/
  
  private QueryBuilder buildCompareQueryWithValue(String field, ExpressionDataType expectedType, Object value) throws CriterionException
  {
    QueryBuilder queryCompare = null;
    switch (criterionOperator)
    {
      case EqualOperator:
      case ContainsOperator:
        queryCompare = QueryBuilders.termQuery(field, value);
        break;

      case NotEqualOperator:
      case DoesNotContainOperator:
        queryCompare = QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery(field, value));
        break;

      case GreaterThanOperator:
        queryCompare = QueryBuilders.rangeQuery(field).gt(value);
        break;

      case GreaterThanOrEqualOperator:
        queryCompare = QueryBuilders.rangeQuery(field).gte(value);
        break;

      case LessThanOperator:
        queryCompare = QueryBuilders.rangeQuery(field).lt(value);
        break;

      case LessThanOrEqualOperator:
        queryCompare = QueryBuilders.rangeQuery(field).lte(value);
        break;

      case IsNotNullOperator:
        queryCompare = QueryBuilders.existsQuery(field);
        break;

      case IsNullOperator:
        queryCompare = QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(field));
        break;

      case IsInSetOperator:
      case NotInSetOperator:
        /*
{
  "query": {
          "should": [
            {
              "term": {
                "subscriberJourneys.status": "converted"
              }
            },
            {
              "term": {
                "subscriberJourneys.status": "entered"
              }
            }
          ]
          }
}
        */
        
        Pattern fieldNamePattern = Pattern.compile("^([^.]+)\\.([^.]+)$");
        Matcher fieldNameMatcher = fieldNamePattern.matcher(field);
        if (! fieldNameMatcher.find()) throw new CriterionException("malformated field " + field);
        String toplevel = fieldNameMatcher.group(1);
        String subfield = fieldNameMatcher.group(2);
        if (!(value instanceof Set<?>))
          {
            throw new CriterionException("Set expected, got " + value.getClass().getName());
          }
        BoolQueryBuilder queryCompareBool = QueryBuilders.boolQuery();
        for (String possibleValue : (Set<String>) value)
          {
            queryCompareBool = queryCompareBool.should(QueryBuilders.termQuery(field, possibleValue));
          }
        queryCompare = queryCompareBool;
        break;

      default:
        throw new CriterionException("not yet implemented : " + criterionOperator);
    }
    return queryCompare;
  }

  /*****************************************
  *
  *  handleLoyaltyProgramField
  *
  *****************************************/
  
  private QueryBuilder handleLoyaltyProgramField(String field, String esField, QueryBuilder queryLPID, ExpressionDataType expectedType) throws CriterionException
  {
    if (argument.getType() != expectedType) throw new CriterionException(esField+" can only be compared to " + expectedType + " " + esField + ", "+argument.getType());
    QueryBuilder queryTierName = buildCompareQuery(field, expectedType);
    QueryBuilder query = QueryBuilders.nestedQuery("loyaltyPrograms",
        QueryBuilders.boolQuery()
            .filter(queryLPID)
            .filter(queryTierName), ScoreMode.Total);
    return query;
  }

  /****************************************
  *
  *  evaluateArgumentIfNecessary
  *
  ****************************************/

  private Object evaluateArgumentIfNecessary(ExpressionDataType dataType) throws CriterionException
  {
    Object value = null;
    switch (criterionOperator)
    {
      case IsNullOperator:
      case IsNotNullOperator:
        break;
        
      default:
        value = evaluateArgument(dataType);
        break;
    }
    return value;
  }

  /****************************************
  *
  *  evaluateArgument : generate a value suitable for an ES query, based on the expected datatype
  *
  ****************************************/
  
  private Object evaluateArgument(ExpressionDataType expectedType) throws CriterionException
  {
    Object value = null;
    try
    {
      switch (expectedType)
      {
        case IntegerExpression:
          value = ((Number) (argument.evaluate(null, null))).toString();
          break;
          
        case StringExpression:
          value = (String) (argument.evaluate(null, null));
          break;
          
        case DateExpression:
          value = ((Date) (argument.evaluate(null, null))).getTime();
          break;
          
        case BooleanExpression:
          value = ((Boolean) (argument.evaluate(null, null))).toString();
          break;
          
        case StringSetExpression:
          value = (Set<String>) (argument.evaluate(null, null));
          break;
          
        case TimeExpression:
          
          //
          //  to do (not now)
          //
          
        default:
          throw new CriterionException("datatype not yet implemented : " + expectedType);
      }
    }
    catch (ExpressionParseException|ExpressionTypeCheckException e)
    {
      throw new CriterionException("argument " + argument + " must be a constant " + expectedType);
    }
    return value;
  }
  
  /****************************************
  *
  *  constructDateTruncateESScript
  *
  ****************************************/

  public static String constructDateTruncateESScript(String nodeIDArg, String rawPrefix, String finalPrefix, TimeUnit timeUnit)
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
          result = "def " + finalPrefix + nodeID + " = " + rawPrefix + nodeID + ".truncatedTo(ChronoUnit." + timeUnit.getChronoUnit() + "); ";
          break;
          
        case Week:
          result = "def " + finalPrefix + nodeID + " = " + rawPrefix + nodeID + ".truncatedTo(ChronoUnit.DAYS).minusDays(" + rawPrefix + nodeID + ".getDayOfWeek().getValue() - DayOfWeek.SUNDAY.getValue()); ";
          break;
          
        case Month:
          result = "def " + finalPrefix + nodeID + " = " + rawPrefix + nodeID + ".truncatedTo(ChronoUnit.DAYS).withDayOfMonth(1); ";
          break;

        case Year:
          result = "def " + finalPrefix + nodeID + " = " + rawPrefix + nodeID + ".truncatedTo(ChronoUnit.DAYS).withDayOfYear(1); ";
          break;
      }
    return result;
  }

  /*****************************************
  *
  *  toString
  *
  *****************************************/

  public String toString()
  {
    StringBuilder b = new StringBuilder();
    b.append("EvaluationCriterion:{");
    b.append(criterionField.getID());
    b.append(" ");
    b.append(criterionOperator);
    b.append(" ");
    b.append(argumentExpression);
    b.append("}");
    return b.toString();
  }
}