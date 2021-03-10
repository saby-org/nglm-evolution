package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;
import com.evolving.nglm.evolution.GUIManagedObject.GUIManagedObjectType;
import com.evolving.nglm.evolution.Journey.SubscriberJourneyStatus;

public class JourneyState
{

  private static final Logger log = LoggerFactory.getLogger(JourneyState.class);

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
    schemaBuilder.name("journey_state");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(9));
    schemaBuilder.field("journeyEndedState", JourneyEndedState.schema());//those are the info we still need after journey is finished
    schemaBuilder.field("callingJourneyRequest", JourneyRequest.serde().optionalSchema());
    schemaBuilder.field("journeyNodeID", Schema.STRING_SCHEMA);
    schemaBuilder.field("journeyParameters", ParameterMap.schema());
    schemaBuilder.field("journeyActionManagerContext", ParameterMap.serde().optionalSchema());
    schemaBuilder.field("journeyEntryDate", Timestamp.SCHEMA);
    schemaBuilder.field("journeyNodeEntryDate", Timestamp.SCHEMA);
    schemaBuilder.field("journeyOutstandingDeliveryRequestID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("sourceFeatureID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("journeyHistory", JourneyHistory.schema());
    schemaBuilder.field("journeyEndDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("specialExitReason", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("priority", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("sourceOrigin", Schema.OPTIONAL_STRING_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<JourneyState> serde = new ConnectSerde<JourneyState>(schema, false, JourneyState.class, JourneyState::pack, JourneyState::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<JourneyState> serde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private JourneyEndedState journeyEndedState;
  private JourneyRequest callingJourneyRequest;
  private String journeyNodeID;
  private ParameterMap journeyParameters;
  private ParameterMap journeyActionManagerContext;
  private Date journeyEntryDate;
  private Date journeyNodeEntryDate;
  private String journeyOutstandingDeliveryRequestID;
  private String sourceFeatureID; // can be null, present by example for workflows that must not define there own ModuleID / FeatureID <ModuleID:FeatureID>
  private JourneyHistory journeyHistory;
  private Date journeyEndDate;
  private List<VoucherChange> voucherChanges;
  private SubscriberJourneyStatus specialExitReason;
  private int priority;
  private String sourceOrigin;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  // JourneyEndedState is a wrapped object of all the infos we still need once campaign is ended for the subs
  public JourneyEndedState getJourneyEndedState() { return journeyEndedState; }
  // all wrapping getters
  public String getJourneyInstanceID() { return journeyEndedState.getJourneyInstanceID(); }
  public String getJourneyID() { return journeyEndedState.getJourneyID(); }
  public Date getJourneyExitDate() { return journeyEndedState.getJourneyExitDate(); }
  public Map<String,Long> getJourneyMetricsPrior() { return journeyEndedState.getJourneyMetricsPrior(); }
  public Map<String,Long> getJourneyMetricsDuring() { return journeyEndedState.getJourneyMetricsDuring(); }
  public Map<String,Long> getJourneyMetricsPost() { return journeyEndedState.getJourneyMetricsPost(); }

  public JourneyRequest getCallingJourneyRequest() { return callingJourneyRequest; }
  public String getJourneyNodeID() { return journeyNodeID; }
  public ParameterMap getJourneyParameters() { return journeyParameters; }
  public ParameterMap getJourneyActionManagerContext() { return journeyActionManagerContext; }
  public Date getJourneyEntryDate() { return journeyEntryDate; }
  public Date getJourneyNodeEntryDate() { return journeyNodeEntryDate; }
  public String getJourneyOutstandingDeliveryRequestID() { return journeyOutstandingDeliveryRequestID; }
  public String getsourceFeatureID() { return sourceFeatureID; }
  public JourneyHistory getJourneyHistory() { return journeyHistory; }
  public Date getJourneyEndDate() { return journeyEndDate; }
  public List<VoucherChange> getVoucherChanges() { return voucherChanges; }
  public Boolean isSpecialExit() { return specialExitReason != null; }
  public SubscriberJourneyStatus getSpecialExitReason() { return specialExitReason; }
  public int getPriority() { return priority; }
  public String getsourceOrigin() { return sourceOrigin; }
  
  /*****************************************
  *
  *  setters
  *
  *****************************************/

  public void setJourneyNodeID(String journeyNodeID, Date journeyNodeEntryDate) { this.journeyNodeID = journeyNodeID; this.journeyNodeEntryDate = journeyNodeEntryDate; this.journeyOutstandingDeliveryRequestID = null; }
  public void setJourneyOutstandingDeliveryRequestID(String journeyOutstandingDeliveryRequestID) { this.journeyOutstandingDeliveryRequestID = journeyOutstandingDeliveryRequestID; }
  public void setSpecialExitReason(SubscriberJourneyStatus specialExitReason) { this.specialExitReason = specialExitReason;	}
  public void setJourneyNodeID(String journeyNodeID) { this.journeyNodeID = journeyNodeID; }
  public void setPriority(int priority) { this.priority = priority; }

  /*****************************************
  *
  *  constructor -- standard
  *
  *****************************************/

  public JourneyState(EvolutionEventContext context, Journey journey, JourneyRequest callingJourneyRequest, String sourceFeatureID, Map<String, Object> journeyParameters, Date journeyEntryDate, JourneyHistory journeyHistory, String sourceOrigin)
  {
  	this.journeyEndedState = new JourneyEndedState(context.getUniqueKey(),journey.getJourneyID());
    this.callingJourneyRequest = callingJourneyRequest;
    this.sourceFeatureID = sourceFeatureID;
    this.journeyNodeID = journey.getStartNodeID();
    this.journeyParameters = new ParameterMap(journeyParameters);
    this.journeyActionManagerContext = new ParameterMap();
    this.journeyEntryDate = journeyEntryDate;
    this.journeyNodeEntryDate = journeyEntryDate;
    this.journeyOutstandingDeliveryRequestID = null;    
    this.journeyHistory = journeyHistory;
    this.journeyEndDate = journey.getEffectiveEndDate();
    this.voucherChanges = new ArrayList<VoucherChange>();
    this.priority = journey.getPriority();
    this.sourceOrigin = sourceOrigin;
    this.specialExitReason = null;
  }
  
 

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public JourneyState(JourneyEndedState journeyEndedState, JourneyRequest callingJourneyRequest, String journeyNodeID, ParameterMap journeyParameters, ParameterMap journeyActionManagerContext, Date journeyEntryDate, Date journeyNodeEntryDate, String journeyOutstandingDeliveryRequestID, String sourceFeatureID, JourneyHistory journeyHistory, Date journeyEndDate, List<VoucherChange> voucherChanges, SubscriberJourneyStatus specialExitReason, int priority, String sourceOrigin)
  {
  	this.journeyEndedState = journeyEndedState;
    this.callingJourneyRequest = callingJourneyRequest;
    this.journeyNodeID = journeyNodeID;
    this.journeyParameters = journeyParameters;
    this.journeyActionManagerContext = journeyActionManagerContext;
    this.journeyEntryDate = journeyEntryDate;
    this.journeyNodeEntryDate = journeyNodeEntryDate;
    this.journeyOutstandingDeliveryRequestID = journeyOutstandingDeliveryRequestID;
    this.sourceFeatureID = sourceFeatureID;
    this.journeyHistory = journeyHistory;
    this.journeyEndDate = journeyEndDate;
    this.voucherChanges = voucherChanges;
    this.specialExitReason = specialExitReason;
    this.priority = priority;
    this.sourceOrigin = sourceOrigin;
  }


  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    JourneyState journeyState = (JourneyState) value;
    Struct struct = new Struct(schema);
    struct.put("journeyEndedState", JourneyEndedState.serde().pack(journeyState.getJourneyEndedState()));
    struct.put("callingJourneyRequest", JourneyRequest.serde().packOptional(journeyState.getCallingJourneyRequest()));
    struct.put("journeyNodeID", journeyState.getJourneyNodeID());
    struct.put("journeyParameters", ParameterMap.pack(journeyState.getJourneyParameters()));
    struct.put("journeyActionManagerContext", ParameterMap.serde().packOptional(journeyState.getJourneyActionManagerContext()));
    struct.put("journeyEntryDate", journeyState.getJourneyEntryDate());
    struct.put("journeyNodeEntryDate", journeyState.getJourneyNodeEntryDate());
    struct.put("journeyOutstandingDeliveryRequestID", journeyState.getJourneyOutstandingDeliveryRequestID());
    struct.put("sourceFeatureID",  journeyState.getsourceFeatureID());
    struct.put("journeyHistory", JourneyHistory.serde().pack(journeyState.getJourneyHistory()));
    struct.put("journeyEndDate", journeyState.getJourneyEndDate());
    struct.put("specialExitReason", journeyState.getSpecialExitReason() != null ? journeyState.getSpecialExitReason().getExternalRepresentation() : null);
    struct.put("priority", journeyState.getPriority());
    struct.put("sourceOrigin", journeyState.getsourceOrigin());
    return struct;
  }
  
  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static JourneyState unpack(SchemaAndValue schemaAndValue)
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
    JourneyRequest callingJourneyRequest = (schemaVersion >= 4) ? JourneyRequest.serde().unpackOptional(new SchemaAndValue(schema.field("callingJourneyRequest").schema(), valueStruct.get("callingJourneyRequest"))) : null;
    String journeyNodeID = valueStruct.getString("journeyNodeID");
    ParameterMap journeyParameters = ParameterMap.unpack(new SchemaAndValue(schema.field("journeyParameters").schema(), valueStruct.get("journeyParameters")));
    ParameterMap journeyActionManagerContext = (schemaVersion >= 3) ? ParameterMap.serde().unpackOptional(new SchemaAndValue(schema.field("journeyActionManagerContext").schema(), valueStruct.get("journeyActionManagerContext"))) : new ParameterMap();
    Date journeyEntryDate = (Date) valueStruct.get("journeyEntryDate");
    Date journeyNodeEntryDate = (Date) valueStruct.get("journeyNodeEntryDate");
    String journeyOutstandingDeliveryRequestID = valueStruct.getString("journeyOutstandingDeliveryRequestID");
    String sourceFeatureID = schema.field("sourceFeatureID") != null ? valueStruct.getString("sourceFeatureID") : null;
    JourneyHistory journeyHistory = JourneyHistory.serde().unpack(new SchemaAndValue(schema.field("journeyHistory").schema(), valueStruct.get("journeyHistory")));
    Date journeyEndDate = (schemaVersion >= 5) ? (Date) valueStruct.get("journeyEndDate") : new Date();
    List<VoucherChange> voucherChanges = new ArrayList<VoucherChange>();
    SubscriberJourneyStatus specialExitReason = schema.field("specialExitReason") != null ? unpackSpecialExitReason(valueStruct) : null;
    int priority = schema.field("priority") != null ? valueStruct.getInt32("priority") : Integer.MAX_VALUE; // for legacy campaigns, very low priority
    String sourceOrigin= schema.field("sourceOrigin") != null ? valueStruct.getString("sourceOrigin") : null;

    JourneyEndedState journeyEndedState=null;
    if(schema.field("journeyEndedState")!=null){//after EVPRO-885
      journeyEndedState = JourneyEndedState.unpack(new SchemaAndValue(schema.field("journeyEndedState").schema(),valueStruct.get("journeyEndedState")));
	}else{//before
      String journeyInstanceID = valueStruct.getString("journeyInstanceID");
      String journeyID = valueStruct.getString("journeyID");
      Date journeyExitDate = (Date) valueStruct.get("journeyExitDate");
      // in old version, if journeyCloseDate was null, means no more jobs to do on metrics
      Date journeyCloseDate = (schemaVersion >= 2) ? (Date) valueStruct.get("journeyCloseDate") : journeyExitDate;
      Map<String,Long> journeyMetricsPrior = new HashMap<>();
      Map<String,Long> journeyMetricsDuring = new HashMap<>();
      Map<String,Long> journeyMetricsPost = new HashMap<>();
      if(journeyCloseDate==null){
        journeyMetricsPrior = (schemaVersion >= 2) ? (Map<String,Long>) valueStruct.get("journeyMetricsPrior") : new HashMap<String,Long>();
        journeyMetricsDuring = (schemaVersion >= 2) ? (Map<String,Long>) valueStruct.get("journeyMetricsDuring") : new HashMap<String,Long>();
        journeyMetricsPost = (schemaVersion >= 2) ? (Map<String,Long>) valueStruct.get("journeyMetricsPost") : new HashMap<String,Long>();
      }
      journeyEndedState = new JourneyEndedState(journeyInstanceID,journeyID,journeyExitDate,journeyMetricsPrior,journeyMetricsDuring,journeyMetricsPost);
	}
    
    //
    //  return
    //

    return new JourneyState(journeyEndedState, callingJourneyRequest, journeyNodeID, journeyParameters, journeyActionManagerContext, journeyEntryDate, journeyNodeEntryDate, journeyOutstandingDeliveryRequestID, sourceFeatureID, journeyHistory, journeyEndDate, voucherChanges, specialExitReason, priority, sourceOrigin);
  }
  
  private static SubscriberJourneyStatus unpackSpecialExitReason(Struct valueStruct)
  {
    if(valueStruct.getString("specialExitReason") != null)
      {
        return SubscriberJourneyStatus.fromExternalRepresentation(valueStruct.getString("specialExitReason"));
      }
    return null;
  }
  
  @Override
  public String toString()
  {
    return "JourneyState [JourneyEndedState=" + journeyEndedState + " priority=" + priority + ", callingJourneyRequest=" + callingJourneyRequest + ", journeyNodeID=" + journeyNodeID + ", journeyParameters=" + journeyParameters + ", journeyActionManagerContext=" + journeyActionManagerContext + ", journeyEntryDate=" + journeyEntryDate + ", journeyNodeEntryDate=" + journeyNodeEntryDate + ", journeyOutstandingDeliveryRequestID=" + journeyOutstandingDeliveryRequestID + ", sourceFeatureID=" + sourceFeatureID + ", journeyHistory=" + journeyHistory + "]";
  }

  public boolean setJourneyExitDate(Date journeyExitDate, SubscriberState subscriberState, Journey journey, EvolutionEngine.EvolutionEventContext context){
    return this.getJourneyEndedState().setJourneyExitDate(journeyExitDate,subscriberState,journey,context);
  }
  public boolean populateMetricsPrior(SubscriberState subscriberState, int tenantID){
    return this.getJourneyEndedState().populateMetricsPrior(subscriberState,this.getJourneyEntryDate(),tenantID);
  }
  
}
