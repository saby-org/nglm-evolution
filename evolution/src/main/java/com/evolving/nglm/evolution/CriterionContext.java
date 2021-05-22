/*****************************************************************************
*
*  CriterionContext.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionDataType;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class CriterionContext
{
  /*****************************************
  *
  *  enum
  *
  *****************************************/

  public enum CriterionContextType
  {
    Profile("profile"),
    FullProfile("fullProfile"),
    Presentation("presentation"),
    Journey("journey"),
    JourneyNode("journeyNode"),
    DynamicProfile("dynamicProfile"),
    FullDynamicProfile("fullDynamicProfile"),
    Unknown("(unknown)");
    private String externalRepresentation;
    private CriterionContextType(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public static CriterionContextType fromExternalRepresentation(String externalRepresentation) { for (CriterionContextType enumeratedValue : CriterionContextType.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }

  /*****************************************
  *
  *  constants
  *
  *****************************************/
 
  public static final String EVALUATION_WK_DAY_ID = "evaluation.weekday";
  public static final String EVALUATION_TIME_ID = "evaluation.time";
  public static final String EVALUATION_MONTH_ID = "evaluation.month";
  public static final String EVALUATION_DAY_OF_MONTH_ID = "evaluation.dayofmonth";
  public static final String EVALUATION_ANIVERSARY_DAY_ID = "evaluation.aniversary.day";
  
  private static HashMap<Integer, CriterionContext> Profile;
  private static HashMap<Integer, CriterionContext> FullProfile;
  private static HashMap<Integer, CriterionContext> DynamicProfile;
  private static HashMap<Integer, CriterionContext> FullDynamicProfile;
  private static HashMap<Integer, CriterionContext> Presentation;
  public static final String JOURNEY_DISPLAY_PARAMETER_ID = "this.journey.display";

  /*****************************************
  *
  *  standard CriterionFields
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(CriterionContext.class);
  
  //
  //  internal
  //

  private static CriterionField evaluationDate;
  private static CriterionField evaluationAniversary;
  private static CriterionField evaluationWeekday;
  private static CriterionField evaluationTime;
  private static CriterionField evaluationMonth;
  private static CriterionField evaluationDayOfMonth;
  private static CriterionField unknownRelationship;
  private static CriterionField evaluationEventName;
  private static CriterionField internalRandom100;
  private static CriterionField subscriberTmpSuccessVouchers;
  private static CriterionField internalFalse;
  private static CriterionField internalTargets;
  private static CriterionField internalExclusionInclusionList;
  
  static
  {   
    Profile = new HashMap<Integer, CriterionContext>();
    FullProfile = new HashMap<Integer, CriterionContext>();
    DynamicProfile = new HashMap<Integer, CriterionContext>();
    FullDynamicProfile = new HashMap<Integer, CriterionContext>();
    Presentation = new HashMap<Integer, CriterionContext>();
    
    //
    //  evaluationDate
    //

    try
      {
        Map<String,Object> evaluationDateJSON = new LinkedHashMap<String,Object>();
        evaluationDateJSON.put("id", "evaluation.date");
        evaluationDateJSON.put("display", "Evaluation Date");
        evaluationDateJSON.put("dataType", "date");
        evaluationDateJSON.put("retriever", "getEvaluationDate");
        evaluationDateJSON.put("esField", "evaluationDate");
        evaluationDateJSON.put("internalOnly", false);
        evaluationDate  = new CriterionField(JSONUtilities.encodeObject(evaluationDateJSON));
      }
    catch (GUIManagerException e)
      {
        throw new ServerRuntimeException(e);
      }
    
    //
    //  evaluationAniversaryDay
    //

    try
      {
        Map<String,Object> evaluationAniversaryJSON = new LinkedHashMap<String,Object>();
        evaluationAniversaryJSON.put("id", EVALUATION_ANIVERSARY_DAY_ID);
        evaluationAniversaryJSON.put("display", "Evaluation Aniversary");
        evaluationAniversaryJSON.put("dataType", "aniversary");
        evaluationAniversaryJSON.put("retriever", "getEvaluationAniversary");
        evaluationAniversaryJSON.put("internalOnly", false);
        evaluationAniversary  = new CriterionField(JSONUtilities.encodeObject(evaluationAniversaryJSON));
      }
    catch (GUIManagerException e)
      {
        throw new ServerRuntimeException(e);
      }
    
    //
    //  evaluationWeekday
    //

    try
      {
        Map<String,Object> evaluationWeekdayJSON = new LinkedHashMap<String,Object>();
        evaluationWeekdayJSON.put("id", EVALUATION_WK_DAY_ID);
        evaluationWeekdayJSON.put("display", "Evaluation Day Of Week");
        evaluationWeekdayJSON.put("dataType", "stringSet");
        evaluationWeekdayJSON.put("retriever", "getEvaluationWeekDay");
        ArrayList<String> availableValues = new ArrayList<>(); availableValues.add("#weekDays#");
        evaluationWeekdayJSON.put("availableValues", JSONUtilities.encodeArray(availableValues));
        evaluationWeekdayJSON.put("internalOnly", false);
        evaluationWeekday  = new CriterionField(JSONUtilities.encodeObject(evaluationWeekdayJSON));
      }
    catch (GUIManagerException e)
      {
        throw new ServerRuntimeException(e);
      }
    
    //
    //  evaluationTime
    //

    try
      {
        Map<String,Object> evaluationTimeJSON = new LinkedHashMap<String,Object>();
        evaluationTimeJSON.put("id", EVALUATION_TIME_ID);
        evaluationTimeJSON.put("display", "Evaluation Time");
        evaluationTimeJSON.put("dataType", "time");
        evaluationTimeJSON.put("retriever", "getEvaluationTime");
        evaluationTimeJSON.put("internalOnly", false);
        evaluationTime  = new CriterionField(JSONUtilities.encodeObject(evaluationTimeJSON));
      }
    catch (GUIManagerException e)
      {
        throw new ServerRuntimeException(e);
      }
    
    //
    //  evaluationMonth
    //

    try
      {
        Map<String,Object> evaluationMonthJSON = new LinkedHashMap<String,Object>();
        evaluationMonthJSON.put("id", EVALUATION_MONTH_ID);
        evaluationMonthJSON.put("display", "Evaluation Month");
        evaluationMonthJSON.put("dataType", "stringSet");
        evaluationMonthJSON.put("retriever", "getEvaluationMonth");
        ArrayList<String> availableValues = new ArrayList<>(); availableValues.add("#months#");
        evaluationMonthJSON.put("availableValues", JSONUtilities.encodeArray(availableValues));
        evaluationMonthJSON.put("internalOnly", false);
        evaluationMonth  = new CriterionField(JSONUtilities.encodeObject(evaluationMonthJSON));
      }
    catch (GUIManagerException e)
      {
        throw new ServerRuntimeException(e);
      }
    
    //
    //  evaluationDayOfMonth
    //

    try
      {
        Map<String,Object> evaluationDayOfMonthJSON = new LinkedHashMap<String,Object>();
        evaluationDayOfMonthJSON.put("id", EVALUATION_DAY_OF_MONTH_ID);
        evaluationDayOfMonthJSON.put("display", "Evaluation Day Of Month");
        evaluationDayOfMonthJSON.put("dataType", "integer");
        evaluationDayOfMonthJSON.put("retriever", "getEvaluationDayOfMonth");
        evaluationDayOfMonthJSON.put("minValue", new Integer(1));
        evaluationDayOfMonthJSON.put("maxValue", new Integer(31));
        evaluationDayOfMonthJSON.put("internalOnly", false);
        evaluationDayOfMonth  = new CriterionField(JSONUtilities.encodeObject(evaluationDayOfMonthJSON));
      }
    catch (GUIManagerException e)
      {
        throw new ServerRuntimeException(e);
      }
    
    //
    //  evaluationEventName
    //

    try
      {
        Map<String,Object> evaluationEventNameJSON = new LinkedHashMap<String,Object>();
        evaluationEventNameJSON.put("id", "evaluation.eventname");
        evaluationEventNameJSON.put("display", "evaluation.eventname");
        evaluationEventNameJSON.put("dataType", "string");
        evaluationEventNameJSON.put("retriever", "getJourneyEvaluationEventName");
        evaluationEventNameJSON.put("internalOnly", true);
        evaluationEventName  = new CriterionField(JSONUtilities.encodeObject(evaluationEventNameJSON));
      }
    catch (GUIManagerException e)
      {
        throw new ServerRuntimeException(e);
      }

    //
    //  unknown relationship
    //

    try
      {
        Map<String,Object> unknownRelationshipJSON =  new LinkedHashMap<String,Object>();
        unknownRelationshipJSON.put("id", "unknown.relationship");
        unknownRelationshipJSON.put("display", "unknown.relationship");
        unknownRelationshipJSON.put("dataType", "boolean");
        unknownRelationshipJSON.put("retriever", "isUnknownRelationship");
        unknownRelationshipJSON.put("internalOnly", true);
        unknownRelationship  = new CriterionField(JSONUtilities.encodeObject(unknownRelationshipJSON));
      }
    catch (GUIManagerException e)
      {
        throw new ServerRuntimeException(e);
      }
    

    //
    //  internalRandom100
    //

    try
      {
        Map<String,Object> internalRandom100JSON = new LinkedHashMap<String,Object>();
        internalRandom100JSON.put("id", "internal.random100");
        internalRandom100JSON.put("display", "internal.random100");
        internalRandom100JSON.put("dataType", "integer");
        internalRandom100JSON.put("retriever", "getRandom100");
        internalRandom100JSON.put("internalOnly", true);
        internalRandom100  = new CriterionField(JSONUtilities.encodeObject(internalRandom100JSON));
      }
    catch (GUIManagerException e)
      {
        throw new ServerRuntimeException(e);
      }

    //
    //  internalFalse
    //

    try
      {
        Map<String,Object> internalFalseJSON = new LinkedHashMap<String,Object>();
        internalFalseJSON.put("id", "internal.false");
        internalFalseJSON.put("display", "internal.false");
        internalFalseJSON.put("dataType", "boolean");
        internalFalseJSON.put("retriever", "getFalse");
        internalFalseJSON.put("internalOnly", true);
        internalFalse  = new CriterionField(JSONUtilities.encodeObject(internalFalseJSON));
      }
    catch (GUIManagerException e)
      {
        throw new ServerRuntimeException(e);
      }

    //
    //  internalTargets
    //

    try
      {
        Map<String,Object> internalTargetsJSON = new LinkedHashMap<String,Object>();
        internalTargetsJSON.put("id", "internal.targets");
        internalTargetsJSON.put("display", "Customer Targets");
        internalTargetsJSON.put("dataType", "stringSet");
        internalTargetsJSON.put("retriever", "getTargets");
        internalTargetsJSON.put("esField", "internal.targets");
        internalTargetsJSON.put("internalOnly", false);
        ArrayList<String> av = new ArrayList<>();
        av.add("#targets#");
        internalTargetsJSON.put("availableValues", JSONUtilities.encodeArray(av));
        
        internalTargets  = new CriterionField(JSONUtilities.encodeObject(internalTargetsJSON));
      }
    catch (GUIManagerException e)
      {
        throw new ServerRuntimeException(e);
      }
    
    //
    //  internalExclusionInclusionList
    //

    try
      {
        Map<String,Object> internalExclusionInclusionListJSON = new LinkedHashMap<String,Object>();
        internalExclusionInclusionListJSON.put("id", "internal.exclusionInclusionList");
        internalExclusionInclusionListJSON.put("display", "Customer Exclusion Inclusion Lists");
        internalExclusionInclusionListJSON.put("dataType", "stringSet");
        internalExclusionInclusionListJSON.put("retriever", "getExclusionInclusionTargets");
        internalExclusionInclusionListJSON.put("esField", "internal.exclusionInclusionList");
        internalExclusionInclusionListJSON.put("internalOnly", false);
        ArrayList<String> av = new ArrayList<>();
        av.add("#exclusionList#");
        internalExclusionInclusionListJSON.put("availableValues", JSONUtilities.encodeArray(av));
        
        internalExclusionInclusionList  = new CriterionField(JSONUtilities.encodeObject(internalExclusionInclusionListJSON));
      }
    catch (GUIManagerException e)
      {
        throw new ServerRuntimeException(e);
      }      
    
    
    //
    //  subscriber.tmp.redeem.vouchers
    //

    try
      {
        Map<String,Object> subscriberTmpSuccessVouchersJSON = new LinkedHashMap<String,Object>();
        subscriberTmpSuccessVouchersJSON.put("id", "subscriber.tmp.success.vouchers");
        subscriberTmpSuccessVouchersJSON.put("display", "subscriber.tmp.success.vouchers");
        subscriberTmpSuccessVouchersJSON.put("dataType", "stringSet");
        subscriberTmpSuccessVouchersJSON.put("retriever", "getSubscriberTmpSuccessVouchers");
        subscriberTmpSuccessVouchersJSON.put("internalOnly", true);
        subscriberTmpSuccessVouchers  = new CriterionField(JSONUtilities.encodeObject(subscriberTmpSuccessVouchersJSON));
      }
    catch (GUIManagerException e)
      {
        throw new ServerRuntimeException(e);
      }
  }

  //
  //  journey
  //

  private static CriterionField journeyEntryDate;
  private static CriterionField nodeEntryDate;
  private static CriterionField journeyActionDeliveryStatus;
  private static CriterionField journeyActionJourneyStatus;
  private static CriterionField journeyEndDate;
  private static CriterionField journeyDisplay;
  static
  {
    
    //
    //  journeyDisplay
    //

    try
      {
        Map<String,Object> journeyDisplayJSON = new LinkedHashMap<String,Object>();
        journeyDisplayJSON.put("id", JOURNEY_DISPLAY_PARAMETER_ID);
        journeyDisplayJSON.put("display", "Campaign");
        journeyDisplayJSON.put("dataType", "string");
        journeyDisplayJSON.put("retriever", "getJourneyParameter");
        journeyDisplayJSON.put("internalOnly", false);
        journeyDisplayJSON.put("tagMaxLength", 100);
        journeyDisplay  = new CriterionField(JSONUtilities.encodeObject(journeyDisplayJSON));
      }
    catch (GUIManagerException e)
      {
        throw new ServerRuntimeException(e);
      }
    
    //
    //  journeyEntryDate
    //

    try
      {
        Map<String,Object> journeyEntryDateJSON = new LinkedHashMap<String,Object>();
        journeyEntryDateJSON.put("id", "journey.entryDate");
        journeyEntryDateJSON.put("display", "journey.entryDate");
        journeyEntryDateJSON.put("dataType", "date");
        journeyEntryDateJSON.put("retriever", "getJourneyEntryDate");
        journeyEntryDateJSON.put("internalOnly", false);
        journeyEntryDate  = new CriterionField(JSONUtilities.encodeObject(journeyEntryDateJSON));
      }
    catch (GUIManagerException e)
      {
        throw new ServerRuntimeException(e);
      }
    
    //
    //  journeyEndDate
    //

    try
      {
        Map<String,Object> journeyEndDateJSON = new LinkedHashMap<String,Object>();
        journeyEndDateJSON.put("id", "journey.endDate");
        journeyEndDateJSON.put("display", "journey.endDate");
        journeyEndDateJSON.put("dataType", "date");
        journeyEndDateJSON.put("retriever", "getJourneyEndDate");
        journeyEndDateJSON.put("internalOnly", false);
        journeyEndDate  = new CriterionField(JSONUtilities.encodeObject(journeyEndDateJSON));
      }
    catch (GUIManagerException e)
      {
        throw new ServerRuntimeException(e);
      }

    //
    //  nodeEntryDate
    //

    try
      {
        Map<String,Object> nodeEntryDateJSON = new LinkedHashMap<String,Object>();
        nodeEntryDateJSON.put("id", "node.entryDate");
        nodeEntryDateJSON.put("display", "node.entryDate");
        nodeEntryDateJSON.put("dataType", "date");
        nodeEntryDateJSON.put("retriever", "getJourneyNodeEntryDate");
        nodeEntryDateJSON.put("internalOnly", false);
        nodeEntryDate  = new CriterionField(JSONUtilities.encodeObject(nodeEntryDateJSON));
      }
    catch (GUIManagerException e)
      {
        throw new ServerRuntimeException(e);
      }

    //
    //  journeyActionDeliveryStatus
    //

    try
      {
        Map<String,Object> journeyActionDeliveryStatusJSON = new LinkedHashMap<String,Object>();
        journeyActionDeliveryStatusJSON.put("id", "node.action.deliverystatus");
        journeyActionDeliveryStatusJSON.put("display", "node.action.deliverystatus");
        journeyActionDeliveryStatusJSON.put("dataType", "string");
        journeyActionDeliveryStatusJSON.put("retriever", "getJourneyActionDeliveryStatus");
        journeyActionDeliveryStatusJSON.put("internalOnly", true);
        journeyActionDeliveryStatus  = new CriterionField(JSONUtilities.encodeObject(journeyActionDeliveryStatusJSON));
      }
    catch (GUIManagerException e)
      {
        throw new ServerRuntimeException(e);
      }

    //
    //  journeyActionJourneyStatus
    //

    try
      {
        Map<String,Object> journeyActionJourneyStatusJSON = new LinkedHashMap<String,Object>();
        journeyActionJourneyStatusJSON.put("id", "node.action.journeystatus");
        journeyActionJourneyStatusJSON.put("display", "node.action.journeystatus");
        journeyActionJourneyStatusJSON.put("dataType", "string");
        journeyActionJourneyStatusJSON.put("retriever", "getJourneyActionJourneyStatus");
        journeyActionJourneyStatusJSON.put("internalOnly", true);
        journeyActionJourneyStatus  = new CriterionField(JSONUtilities.encodeObject(journeyActionJourneyStatusJSON));
      }
    catch (GUIManagerException e)
      {
        throw new ServerRuntimeException(e);
      }
  }
  
  private static Object lock = new Object();
  
  
  public static CriterionContext Profile(int tenantID) 
  {
    CriterionContext criterionContext = Profile.get(tenantID);
    if(criterionContext == null)
      {
        synchronized(lock)
        {
          criterionContext = Profile.get(tenantID);
          if(criterionContext == null)
            {
              criterionContext = new CriterionContext(CriterionContextType.Profile, tenantID);
              Profile.put(tenantID, criterionContext);
            }          
        }        
      }
    return criterionContext;
  }
  
  public static CriterionContext FullProfile(int tenantID) 
  {
    CriterionContext criterionContext = FullProfile.get(tenantID);
    if(criterionContext == null)
      {
        synchronized(lock)
        {
          criterionContext = FullProfile.get(tenantID);
          if(criterionContext == null)
            {
              criterionContext = new CriterionContext(CriterionContextType.FullProfile, tenantID);
              FullProfile.put(tenantID, criterionContext);
            }          
        }        
      }
    return criterionContext;
  }
  
  public static CriterionContext DynamicProfile(int tenantID) 
  {
    CriterionContext criterionContext = DynamicProfile.get(tenantID);
    if(criterionContext == null)
      {
        synchronized(lock)
        {
          criterionContext = DynamicProfile.get(tenantID);
          if(criterionContext == null)
            {
              criterionContext = new CriterionContext(CriterionContextType.DynamicProfile, tenantID);
              DynamicProfile.put(tenantID, criterionContext);
            }          
        }        
      }
    return criterionContext;
  }
  
  public static CriterionContext FullDynamicProfile(int tenantID) 
  {
    CriterionContext criterionContext = FullDynamicProfile.get(tenantID);
    if(criterionContext == null)
      {
        synchronized(lock)
        {
          criterionContext = FullDynamicProfile.get(tenantID);
          if(criterionContext == null)
            {
              criterionContext = new CriterionContext(CriterionContextType.FullDynamicProfile, tenantID);
              FullDynamicProfile.put(tenantID, criterionContext);
            }          
        }        
      }
    return criterionContext;
  }
  
  public static CriterionContext Presentation(int tenantID) 
  {
    CriterionContext criterionContext = Presentation.get(tenantID);
    if(criterionContext == null)
      {
        synchronized(lock)
        {
          criterionContext = Presentation.get(tenantID);
          if(criterionContext == null)
            {
              criterionContext = new CriterionContext(CriterionContextType.Presentation, tenantID);
              Presentation.put(tenantID, criterionContext);
            }          
        }        
      }
    return criterionContext;
  }

  /*****************************************
  *
  *  dynamic criterion fields
  *
  *****************************************/

  private static DynamicCriterionFieldService dynamicCriterionFieldService = null;
  public static void initialize(DynamicCriterionFieldService dynamicCriterionFieldService)
  {
    CriterionContext.dynamicCriterionFieldService = dynamicCriterionFieldService;
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
    schemaBuilder.name("criterion_context");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(3));
    schemaBuilder.field("criterionContextType", Schema.STRING_SCHEMA);
    schemaBuilder.field("additionalCriterionFields", SchemaBuilder.array(CriterionField.schema()).schema());
    schemaBuilder.field("tenantID", Schema.INT16_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<CriterionContext> serde = new ConnectSerde<CriterionContext>(schema, false, CriterionContext.class, CriterionContext::pack, CriterionContext::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<CriterionContext> serde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private CriterionContextType criterionContextType;
  private Map<String,CriterionField> additionalCriterionFields;
  private int tenantID;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public CriterionContextType getCriterionContextType() { return criterionContextType; }
  public Map<String,CriterionField> getAdditionalCriterionFields() { return additionalCriterionFields; }
  public int getTenantID() { return tenantID; }

  /*****************************************
  *
  *  constructor -- simple
  *
  *****************************************/

  public CriterionContext(CriterionContextType criterionContextType, int tenantID)
  {
    this.criterionContextType = criterionContextType;
    this.additionalCriterionFields = Collections.<String,CriterionField>emptyMap();
    this.tenantID = tenantID;
  }
  
  /*****************************************
  *
  *  constructor -- top-level of journey
  *
  *****************************************/
  
  public CriterionContext(Map<String,CriterionField> journeyParameters, Map<String,CriterionField> contextVariables, int tenantID)
  {
    this.criterionContextType = CriterionContextType.Journey;
    this.additionalCriterionFields = new LinkedHashMap<String,CriterionField>();
    this.additionalCriterionFields.put(journeyEntryDate.getID(), journeyEntryDate);
    this.additionalCriterionFields.put(journeyEndDate.getID(), journeyEndDate);
    this.additionalCriterionFields.put(journeyDisplay.getID(), journeyDisplay);
    this.additionalCriterionFields.putAll(journeyParameters);
    for (CriterionField contextVariable : contextVariables.values())
      {
        this.additionalCriterionFields.put(contextVariable.getID(), contextVariable);
      }
    this.tenantID = tenantID;
  }

  /*****************************************
  *
  *  constructor -- journey working context
  *
  *****************************************/

  public CriterionContext(CriterionContext baseCriterionContext, Map<String,CriterionField> additionalCriterionFields, int tenantID)
  {
    this.criterionContextType = CriterionContextType.JourneyNode;
    this.additionalCriterionFields = new LinkedHashMap<String,CriterionField>(baseCriterionContext.getAdditionalCriterionFields());
    this.additionalCriterionFields.putAll(additionalCriterionFields);
    this.tenantID = tenantID;
  }

  /*****************************************
  *
  *  constructor -- journey node
  *
  *****************************************/
  public CriterionContext(Map<String,CriterionField> journeyParameters, Map<String,CriterionField> contextVariables, NodeType journeyNodeType, EvolutionEngineEventDeclaration journeyEvent, Journey selectedJourney, int tenantID) throws GUIManagerException
  {
    this(journeyParameters, contextVariables, journeyNodeType, journeyEvent, selectedJourney, null, tenantID);
  }

  public CriterionContext(Map<String,CriterionField> journeyParameters, Map<String,CriterionField> contextVariables, NodeType journeyNodeType, EvolutionEngineEventDeclaration journeyEvent, Journey selectedJourney, CriterionDataType expectedDataType, int tenantID) throws GUIManagerException
  {
    
    this.tenantID = tenantID;
    
    /*****************************************
    *
    *  contextType
    *
    *****************************************/

    this.criterionContextType = CriterionContextType.JourneyNode;

    /*****************************************
    *
    *  additionalCriterionFields -- standard node
    *
    *****************************************/

    this.additionalCriterionFields = new LinkedHashMap<String,CriterionField>();
    if (journeyNodeType == null /* case for journey targeting by trigger */ || !journeyNodeType.getStartNode())
      {
        //
        //  standard journey top-level fields
        //

        this.additionalCriterionFields.put(journeyEntryDate.getID(), journeyEntryDate);
        this.additionalCriterionFields.put(journeyEndDate.getID(), journeyEndDate);
        this.additionalCriterionFields.put(journeyDisplay.getID(), journeyDisplay);

        //
        //  journey parameters
        //

        this.additionalCriterionFields.putAll(journeyParameters);

        //
        //  context variables
        //

        for (CriterionField contextVariable : contextVariables.values())
          {
            this.additionalCriterionFields.put(contextVariable.getID(), contextVariable);
          }

        //
        //  standard journey node-level fields
        //

        this.additionalCriterionFields.put(nodeEntryDate.getID(), nodeEntryDate);
        this.additionalCriterionFields.put(journeyActionDeliveryStatus.getID(), journeyActionDeliveryStatus);
        this.additionalCriterionFields.put(journeyActionJourneyStatus.getID(), journeyActionJourneyStatus);
        
        if(journeyNodeType != null)
          {

            //
            //  node-level parameters
            //
    
            this.additionalCriterionFields.putAll(journeyNodeType.getParameters());
    
            //
            //  action-manager parameters
            //
    
            this.additionalCriterionFields.putAll((journeyNodeType.getActionManager() != null) ? journeyNodeType.getActionManager().getOutputAttributes() : Collections.<String,CriterionField>emptyMap());
            
            //
            //  scheduleNode
            //
            
            if (journeyNodeType.getScheduleNode())
              {
                this.additionalCriterionFields.put(evaluationWeekday.getID(), evaluationWeekday);
                this.additionalCriterionFields.put(evaluationTime.getID(), evaluationTime);
                this.additionalCriterionFields.put(evaluationAniversary.getID(), evaluationAniversary);
              }
          }

        //
        //  trigger-level fields
        //

        if (journeyEvent != null)
          {
            this.additionalCriterionFields.putAll(journeyEvent.getEventCriterionFields());
          }

        //
        //  called-journey fields
        //

        if (selectedJourney != null)
          {
            for (CriterionField contextVariable : selectedJourney.getContextVariables().values())
              {
                String resultFieldID = Journey.generateJourneyResultID(selectedJourney, contextVariable);
                this.additionalCriterionFields.put(resultFieldID, new CriterionField(resultFieldID, selectedJourney, contextVariable));
              }
          }
        
        if (expectedDataType != null)
          {
            // only keep criterion of this type
            Map<String,CriterionField> newMap = new LinkedHashMap<String,CriterionField>();
            for (Entry<String, CriterionField> entry : this.additionalCriterionFields.entrySet())
              {
                if (expectedDataType.compatibleWith(entry.getValue().getFieldDataType()))
                  {
                    newMap.put(entry.getKey(), entry.getValue());
                  }
              }
            this.additionalCriterionFields = newMap;
          }
      }
  }

  /*****************************************
  *
  *  constructor -- journey link
  *
  *****************************************/

  public CriterionContext(CriterionContext nodeCriterionContext, NodeType journeyNodeType, EvolutionEngineEventDeclaration journeyEvent, int tenantID)
  {
    this.criterionContextType = CriterionContextType.JourneyNode;
    this.additionalCriterionFields = new LinkedHashMap<String,CriterionField>(nodeCriterionContext.getAdditionalCriterionFields());
    this.additionalCriterionFields.putAll(journeyNodeType.getOutputConnectorParameters());
    if (journeyEvent != null)
      {
        this.additionalCriterionFields.putAll(journeyEvent.getEventCriterionFields());
      }
    this.tenantID = tenantID;
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  private CriterionContext(CriterionContextType criterionContextType, Map<String,CriterionField> additionalCriterionFields, int tenantID)
  {
    this.criterionContextType = criterionContextType;
    this.additionalCriterionFields = additionalCriterionFields;
    this.tenantID = tenantID;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    CriterionContext criterionContext = (CriterionContext) value;
    Struct struct = new Struct(schema);
    struct.put("criterionContextType", criterionContext.getCriterionContextType().getExternalRepresentation());
    struct.put("additionalCriterionFields", packAdditionalCriterionFields(criterionContext.getAdditionalCriterionFields()));
    struct.put("tenantID", (short)criterionContext.getTenantID());
    return struct;
  }

  /****************************************
  *
  *  packAdditionalCriterionFields
  *
  ****************************************/

  private static List<Object> packAdditionalCriterionFields(Map<String,CriterionField> additionalCriterionFields)
  {
    List<Object> result = new ArrayList<Object>();
    for (CriterionField criterionField : additionalCriterionFields.values())
      {
        result.add(CriterionField.pack(criterionField));
      }
    return result;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static CriterionContext unpack(SchemaAndValue schemaAndValue)
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
    CriterionContextType criterionContextType = CriterionContextType.fromExternalRepresentation(valueStruct.getString("criterionContextType"));
    Map<String,CriterionField> additionalCriterionFields = (schemaVersion >= 2) ? unpackAdditionalCriterionFields(schema.field("additionalCriterionFields").schema(), valueStruct.get("additionalCriterionFields")) : unpackAdditionalCriterionFields(schema.field("journeyCriterionFields").schema(), valueStruct.get("journeyCriterionFields"));
    int tenantID = schema.field("tenantID") != null ? valueStruct.getInt16("tenantID") : 1;
    //
    //  return
    //

    return new CriterionContext(criterionContextType, additionalCriterionFields, tenantID);
  }

  /*****************************************
  *
  *  unpackAdditionalCriterionFields
  *
  *****************************************/

  private static Map<String,CriterionField> unpackAdditionalCriterionFields(Schema schema, Object value)
  {
    //
    //  get schema for CriterionField
    //

    Schema criterionFieldSchema = schema.valueSchema();
    
    //
    //  unpack
    //

    Map<String,CriterionField> result = new LinkedHashMap<String,CriterionField>();
    List<Object> valueArray = (List<Object>) value;
    for (Object field : valueArray)
      {
        CriterionField criterionField = CriterionField.unpack(new SchemaAndValue(criterionFieldSchema, field));
        result.put(criterionField.getID(), criterionField);
      }

    //
    //  return
    //

    return result;
  }

  /*****************************************
  *
  *  getCriterionFields
  *
  *****************************************/
  
  public Map<String,CriterionField> getCriterionFields(int tenantID)
  {
    Map<String,CriterionField> result;
    switch (criterionContextType)
      {
        case Profile:
          result = new LinkedHashMap<String,CriterionField>();
          result.put(evaluationDate.getID(), evaluationDate);
          result.put(evaluationEventName.getID(), evaluationEventName);
          result.put(unknownRelationship.getID(), unknownRelationship);
          result.put(internalRandom100.getID(), internalRandom100);
          result.put(subscriberTmpSuccessVouchers.getID(), subscriberTmpSuccessVouchers);
          result.put(internalFalse.getID(), internalFalse);
          result.put(internalTargets.getID(), internalTargets);
          result.put(internalExclusionInclusionList.getID(), internalExclusionInclusionList);
          result.put(evaluationDayOfMonth.getID(), evaluationDayOfMonth);
          result.put(evaluationMonth.getID(), evaluationMonth);
          result.putAll(Deployment.getProfileCriterionFields());
          break;

        case FullProfile:
          result = new LinkedHashMap<String,CriterionField>();
          result.putAll(Profile(tenantID).getCriterionFields(tenantID));
          result.putAll(Deployment.getDeployment(tenantID).getExtendedProfileCriterionFields());
          break;

        case DynamicProfile:
          if (dynamicCriterionFieldService == null) throw new ServerRuntimeException("criterion context not initialized");
          result = new LinkedHashMap<String,CriterionField>();
          result.putAll(Profile(tenantID).getCriterionFields(tenantID));
          for (DynamicCriterionField dynamicCriterionField : dynamicCriterionFieldService.getActiveDynamicCriterionFields(SystemTime.getCurrentTime(), tenantID))
            {
              result.put(dynamicCriterionField.getCriterionField().getID(), dynamicCriterionField.getCriterionField());
            }
          break;

        case FullDynamicProfile:
          result = new LinkedHashMap<String,CriterionField>();
          result.putAll(DynamicProfile(tenantID).getCriterionFields(tenantID));
          result.putAll(Deployment.getDeployment(tenantID).getExtendedProfileCriterionFields());
          break;

        case Journey:
        case JourneyNode:
          result = new LinkedHashMap<String,CriterionField>();
          result.putAll(additionalCriterionFields);
          result.putAll(DynamicProfile(tenantID).getCriterionFields(tenantID));
          break;

        case Presentation:
          result = Deployment.getDeployment(tenantID).getPresentationCriterionFields();
          break;

        default:
          throw new ServerRuntimeException("unknown criterionContext: " + criterionContextType);
      }
    return result;
  }
}
