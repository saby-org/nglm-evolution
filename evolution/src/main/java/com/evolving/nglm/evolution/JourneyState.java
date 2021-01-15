/*****************************************************************************
*
*  JourneyState.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.time.Duration;
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
import com.evolving.nglm.evolution.Journey.SubscriberJourneyStatus;
import com.evolving.nglm.evolution.retention.Cleanable;
import com.evolving.nglm.evolution.retention.RetentionService;

public class JourneyState implements Cleanable
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
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(7));
    schemaBuilder.field("callingJourneyRequest", JourneyRequest.serde().optionalSchema());
    schemaBuilder.field("journeyInstanceID", Schema.STRING_SCHEMA);
    schemaBuilder.field("journeyID", Schema.STRING_SCHEMA);
    schemaBuilder.field("journeyNodeID", Schema.STRING_SCHEMA);
    schemaBuilder.field("journeyParameters", ParameterMap.schema());
    schemaBuilder.field("journeyActionManagerContext", ParameterMap.serde().optionalSchema());
    schemaBuilder.field("journeyEntryDate", Timestamp.SCHEMA);
    schemaBuilder.field("journeyExitDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("journeyCloseDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("journeyMetricsPrior", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT64_SCHEMA).name("journeystate_metrics_prior").schema());
    schemaBuilder.field("journeyMetricsDuring", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT64_SCHEMA).name("journeystate_metrics_during").schema());
    schemaBuilder.field("journeyMetricsPost", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT64_SCHEMA).name("journeystate_metrics_post").schema());
    schemaBuilder.field("journeyNodeEntryDate", Timestamp.SCHEMA);
    schemaBuilder.field("journeyOutstandingDeliveryRequestID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("sourceFeatureID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("journeyHistory", JourneyHistory.schema());
    schemaBuilder.field("journeyEndDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("specialExitReason", Schema.OPTIONAL_STRING_SCHEMA);
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

  private JourneyRequest callingJourneyRequest;
  private String journeyInstanceID;
  private String journeyID;
  private String journeyNodeID;
  

  private ParameterMap journeyParameters;
  private ParameterMap journeyActionManagerContext;
  private Date journeyEntryDate;
  private Date journeyExitDate;
  private Date journeyCloseDate;
  private Map<String,Long> journeyMetricsPrior;
  private Map<String,Long> journeyMetricsDuring;
  private Map<String,Long> journeyMetricsPost;
  private Date journeyNodeEntryDate;
  private String journeyOutstandingDeliveryRequestID;
  private String sourceFeatureID; // can be null, present by example for workflows that must not define there own ModuleID / FeatureID <ModuleID:FeatureID>
  private JourneyHistory journeyHistory;
  private Date journeyEndDate;
  private List<VoucherChange> voucherChanges;
  private SubscriberJourneyStatus specialExitReason = null;  
  public void setJourneyEndDate(Date journeyEndDate) { this.journeyEndDate = journeyEndDate; }
  public void setSpecialExitReason(SubscriberJourneyStatus specialExitReason) { this.specialExitReason = specialExitReason;	}
  public void setJourneyNodeID(String journeyNodeID) { this.journeyNodeID = journeyNodeID; }
 

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public JourneyRequest getCallingJourneyRequest() { return callingJourneyRequest; }
  public String getJourneyInstanceID() { return journeyInstanceID; }
  public String getJourneyID() { return journeyID; }
  public String getJourneyNodeID() { return journeyNodeID; }
  public ParameterMap getJourneyParameters() { return journeyParameters; }
  public ParameterMap getJourneyActionManagerContext() { return journeyActionManagerContext; }
  public Date getJourneyEntryDate() { return journeyEntryDate; }
  public Date getJourneyExitDate() { return journeyExitDate; }
  public Date getJourneyCloseDate() { return journeyCloseDate; }
  public Map<String,Long> getJourneyMetricsPrior() { return journeyMetricsPrior; }
  public Map<String,Long> getJourneyMetricsDuring() { return journeyMetricsDuring; }
  public Map<String,Long> getJourneyMetricsPost() { return journeyMetricsPost; }
  public Date getJourneyNodeEntryDate() { return journeyNodeEntryDate; }
  public String getJourneyOutstandingDeliveryRequestID() { return journeyOutstandingDeliveryRequestID; }
  public String getsourceFeatureID() { return sourceFeatureID; }
  public JourneyHistory getJourneyHistory() { return journeyHistory; }
  public Date getJourneyEndDate() { return journeyEndDate; }
  public List<VoucherChange> getVoucherChanges() { return voucherChanges; }
  public Boolean isSpecialExit() { return specialExitReason != null; }
  public SubscriberJourneyStatus getSpecialExitReason() {return specialExitReason;}
  /*****************************************
  *
  *  setters
  *
  *****************************************/

  public void setJourneyNodeID(String journeyNodeID, Date journeyNodeEntryDate) { this.journeyNodeID = journeyNodeID; this.journeyNodeEntryDate = journeyNodeEntryDate; this.journeyOutstandingDeliveryRequestID = null; }
  public void setJourneyOutstandingDeliveryRequestID(String journeyOutstandingDeliveryRequestID) { this.journeyOutstandingDeliveryRequestID = journeyOutstandingDeliveryRequestID; }
  public void setsourceFeatureID(String sourceFeatureID) { this.sourceFeatureID = sourceFeatureID; };
  public void setJourneyExitDate(Date journeyExitDate) { this.journeyExitDate = journeyExitDate; }
  public void setJourneyCloseDate(Date journeyCloseDate) { this.journeyCloseDate = journeyCloseDate; }

  @Override public Date getExpirationDate(RetentionService retentionService) {
    if(getJourneyExitDate()!=null) return getJourneyExitDate();//case subscriber ended the journey
    return getJourneyEndDate();// case subscriber did not end the journey
  }
  @Override public Duration getRetention(RetentionType type, RetentionService retentionService) {
    return retentionService.getJourneyRetention(type,getJourneyID());
  }

  /*****************************************
  *
  *  constructor -- standard
  *
  *****************************************/

  public JourneyState(EvolutionEventContext context, Journey journey, JourneyRequest callingJourneyRequest, String sourceFeatureID, Map<String, Object> journeyParameters, Date journeyEntryDate, JourneyHistory journeyHistory)
  {
     this.callingJourneyRequest = callingJourneyRequest;
    this.sourceFeatureID = sourceFeatureID;
    this.journeyInstanceID = context.getUniqueKey();
    this.journeyID = journey.getJourneyID();
    this.journeyNodeID = journey.getStartNodeID();
    this.journeyParameters = new ParameterMap(journeyParameters);
    this.journeyActionManagerContext = new ParameterMap();
    this.journeyEntryDate = journeyEntryDate;
    this.journeyExitDate = null;
    this.journeyCloseDate = null;
    this.journeyMetricsPrior = new HashMap<String,Long>();
    this.journeyMetricsDuring = new HashMap<String,Long>();
    this.journeyMetricsPost = new HashMap<String,Long>();
    this.journeyNodeEntryDate = journeyEntryDate;
    this.journeyOutstandingDeliveryRequestID = null;    
    this.journeyHistory = journeyHistory;
    this.journeyEndDate = journey.getEffectiveEndDate();
    this.voucherChanges = new ArrayList<VoucherChange>();
  }
  
 

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public JourneyState(String journeyInstanceID, JourneyRequest callingJourneyRequest, String journeyID, String journeyNodeID, ParameterMap journeyParameters, ParameterMap journeyActionManagerContext, Date journeyEntryDate, Date journeyExitDate, Date journeyCloseDate, Map<String,Long> journeyMetricsPrior, Map<String,Long> journeyMetricsDuring, Map<String,Long> journeyMetricsPost, Date journeyNodeEntryDate, String journeyOutstandingDeliveryRequestID, String sourceFeatureID, JourneyHistory journeyHistory, Date journeyEndDate, List<VoucherChange> voucherChanges, SubscriberJourneyStatus specialExitReason)
  {
    this.callingJourneyRequest = callingJourneyRequest;
    this.journeyInstanceID = journeyInstanceID;
    this.journeyID = journeyID;
    this.journeyNodeID = journeyNodeID;
    this.journeyParameters = journeyParameters;
    this.journeyActionManagerContext = journeyActionManagerContext;
    this.journeyEntryDate = journeyEntryDate;
    this.journeyExitDate = journeyExitDate;
    this.journeyCloseDate = journeyCloseDate;
    this.journeyMetricsPrior = journeyMetricsPrior;
    this.journeyMetricsDuring = journeyMetricsDuring;
    this.journeyMetricsPost = journeyMetricsPost;
    this.journeyNodeEntryDate = journeyNodeEntryDate;
    this.journeyOutstandingDeliveryRequestID = journeyOutstandingDeliveryRequestID;
    this.sourceFeatureID = sourceFeatureID;
    this.journeyHistory = journeyHistory;
    this.journeyEndDate = journeyEndDate;
    this.voucherChanges = voucherChanges;
    this.specialExitReason = specialExitReason;
  }

  /*****************************************
  *
  *  constructor -- copy
  *
  *****************************************/

  public JourneyState(JourneyState journeyState)
  {
    this.callingJourneyRequest = journeyState.getCallingJourneyRequest();
    this.journeyInstanceID = journeyState.getJourneyInstanceID();
    this.journeyID = journeyState.getJourneyID();
    this.journeyNodeID = journeyState.getJourneyNodeID();
    this.journeyParameters = new ParameterMap(journeyState.getJourneyParameters());
    this.journeyActionManagerContext = new ParameterMap(journeyState.getJourneyActionManagerContext());
    this.journeyEntryDate = journeyState.getJourneyEntryDate();
    this.journeyExitDate = journeyState.getJourneyExitDate();
    this.journeyCloseDate = journeyState.getJourneyCloseDate();
    this.journeyMetricsPrior = new HashMap<String,Long>(journeyState.getJourneyMetricsPrior());
    this.journeyMetricsDuring = new HashMap<String,Long>(journeyState.getJourneyMetricsDuring());
    this.journeyMetricsPost = new HashMap<String,Long>(journeyState.getJourneyMetricsPost());
    this.journeyNodeEntryDate = journeyState.getJourneyNodeEntryDate();
    this.journeyOutstandingDeliveryRequestID = journeyState.getJourneyOutstandingDeliveryRequestID();
    this.sourceFeatureID = journeyState.getsourceFeatureID();
    this.journeyHistory = journeyState.getJourneyHistory();
    this.journeyEndDate = journeyState.getJourneyEndDate();
    this.voucherChanges = journeyState.getVoucherChanges();
    this.specialExitReason = journeyState.getSpecialExitReason();
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
    struct.put("callingJourneyRequest", JourneyRequest.serde().packOptional(journeyState.getCallingJourneyRequest()));
    struct.put("journeyInstanceID", journeyState.getJourneyInstanceID());
    struct.put("journeyID", journeyState.getJourneyID());
    struct.put("journeyNodeID", journeyState.getJourneyNodeID());
    struct.put("journeyParameters", ParameterMap.pack(journeyState.getJourneyParameters()));
    struct.put("journeyActionManagerContext", ParameterMap.serde().packOptional(journeyState.getJourneyActionManagerContext()));
    struct.put("journeyEntryDate", journeyState.getJourneyEntryDate());
    struct.put("journeyExitDate", journeyState.getJourneyExitDate());
    struct.put("journeyCloseDate", journeyState.getJourneyCloseDate());
    struct.put("journeyMetricsPrior", journeyState.getJourneyMetricsPrior());
    struct.put("journeyMetricsDuring", journeyState.getJourneyMetricsDuring());
    struct.put("journeyMetricsPost", journeyState.getJourneyMetricsPost());
    struct.put("journeyNodeEntryDate", journeyState.getJourneyNodeEntryDate());
    struct.put("journeyOutstandingDeliveryRequestID", journeyState.getJourneyOutstandingDeliveryRequestID());
    struct.put("sourceFeatureID",  journeyState.getsourceFeatureID());
    struct.put("journeyHistory", JourneyHistory.serde().pack(journeyState.getJourneyHistory()));
    struct.put("journeyEndDate", journeyState.getJourneyEndDate());
    struct.put("specialExitReason", journeyState.getSpecialExitReason() != null ? journeyState.getSpecialExitReason().getExternalRepresentation() : null);
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
    String journeyInstanceID = valueStruct.getString("journeyInstanceID");
    String journeyID = valueStruct.getString("journeyID");
    String journeyNodeID = valueStruct.getString("journeyNodeID");
    ParameterMap journeyParameters = ParameterMap.unpack(new SchemaAndValue(schema.field("journeyParameters").schema(), valueStruct.get("journeyParameters")));
    ParameterMap journeyActionManagerContext = (schemaVersion >= 3) ? ParameterMap.serde().unpackOptional(new SchemaAndValue(schema.field("journeyActionManagerContext").schema(), valueStruct.get("journeyActionManagerContext"))) : new ParameterMap();
    Date journeyEntryDate = (Date) valueStruct.get("journeyEntryDate");
    Date journeyExitDate = (Date) valueStruct.get("journeyExitDate");
    Date journeyCloseDate = (schemaVersion >= 2) ? (Date) valueStruct.get("journeyCloseDate") : journeyExitDate;
    Map<String,Long> journeyMetricsPrior = (schemaVersion >= 2) ? (Map<String,Long>) valueStruct.get("journeyMetricsPrior") : new HashMap<String,Long>();
    Map<String,Long> journeyMetricsDuring = (schemaVersion >= 2) ? (Map<String,Long>) valueStruct.get("journeyMetricsDuring") : new HashMap<String,Long>();
    Map<String,Long> journeyMetricsPost = (schemaVersion >= 2) ? (Map<String,Long>) valueStruct.get("journeyMetricsPost") : new HashMap<String,Long>();
    Date journeyNodeEntryDate = (Date) valueStruct.get("journeyNodeEntryDate");
    String journeyOutstandingDeliveryRequestID = valueStruct.getString("journeyOutstandingDeliveryRequestID");
    String sourceFeatureID = schema.field("sourceFeatureID") != null ? valueStruct.getString("sourceFeatureID") : null;
    JourneyHistory journeyHistory = JourneyHistory.serde().unpack(new SchemaAndValue(schema.field("journeyHistory").schema(), valueStruct.get("journeyHistory")));
    Date journeyEndDate = (schemaVersion >= 5) ? (Date) valueStruct.get("journeyEndDate") : new Date();
    List<VoucherChange> voucherChanges = new ArrayList<VoucherChange>();
    SubscriberJourneyStatus specialExitReason = schema.field("specialExitReason") != null ? unpackSpecialExitReason(valueStruct) : null;
  
    //
    //  return
    //

    return new JourneyState(journeyInstanceID, callingJourneyRequest, journeyID, journeyNodeID, journeyParameters, journeyActionManagerContext, journeyEntryDate, journeyExitDate, journeyCloseDate, journeyMetricsPrior, journeyMetricsDuring, journeyMetricsPost, journeyNodeEntryDate, journeyOutstandingDeliveryRequestID, sourceFeatureID, journeyHistory, journeyEndDate, voucherChanges, specialExitReason);
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
    return "JourneyState [callingJourneyRequest=" + callingJourneyRequest + ", journeyInstanceID=" + journeyInstanceID + ", journeyID=" + journeyID + ", journeyNodeID=" + journeyNodeID + ", journeyParameters=" + journeyParameters + ", journeyActionManagerContext=" + journeyActionManagerContext + ", journeyEntryDate=" + journeyEntryDate + ", journeyExitDate=" + journeyExitDate + ", journeyCloseDate=" + journeyCloseDate + ", journeyMetricsPrior=" + journeyMetricsPrior + ", journeyMetricsDuring=" + journeyMetricsDuring + ", journeyMetricsPost=" + journeyMetricsPost + ", journeyNodeEntryDate=" + journeyNodeEntryDate + ", journeyOutstandingDeliveryRequestID=" + journeyOutstandingDeliveryRequestID + ", sourceFeatureID=" + sourceFeatureID + ", journeyHistory=" + journeyHistory + "]";
  }
  
  /*****************************************
  *
  *  populate journeyMetrics
  *
  *****************************************/  
  /**
   * populate journeyMetrics (prior and "during")
   * @return true if subscriber state has been updated
   */
  public boolean populateMetricsPrior(SubscriberState subscriberState, int tenantID) 
  {
    boolean subscriberStateUpdated = false;
    for (JourneyMetricDeclaration journeyMetricDeclaration : Deployment.getJourneyMetricDeclarations().values()) {
      //
      //  metricHistory
      //
      MetricHistory metricHistory = journeyMetricDeclaration.getMetricHistory(subscriberState.getSubscriberProfile());

      //
      //  prior
      //
      Date journeyEntryDay = RLMDateUtils.truncate(this.getJourneyEntryDate(), Calendar.DATE, Calendar.SUNDAY, Deployment.getDeployment(tenantID).getBaseTimeZone());
      Date metricStartDay = RLMDateUtils.addDays(journeyEntryDay, -1 * journeyMetricDeclaration.getPriorPeriodDays(), Deployment.getDeployment(tenantID).getBaseTimeZone());
      Date metricEndDay = RLMDateUtils.addDays(journeyEntryDay, -1, Deployment.getDeployment(tenantID).getBaseTimeZone());
      long priorMetricValue = metricHistory.getValue(metricStartDay, metricEndDay);
      this.getJourneyMetricsPrior().put(journeyMetricDeclaration.getID(), priorMetricValue);

      //
      //  during (note:  at entry these are set to the "all-time-total" and will be fixed up when the journey ends
      //
      Long startMetricValue = metricHistory.getAllTimeBucket();
      this.getJourneyMetricsDuring().put(journeyMetricDeclaration.getID(), startMetricValue);
      subscriberStateUpdated = true;
    }
    
    return subscriberStateUpdated;
  }

  /**
   * populate journeyMetrics (during)
   * @return true if subscriber state has been updated
   */
  public boolean populateMetricsDuring(SubscriberState subscriberState) 
  {
    boolean subscriberStateUpdated = false;
    for (JourneyMetricDeclaration journeyMetricDeclaration : Deployment.getJourneyMetricDeclarations().values()) {
      MetricHistory metricHistory = journeyMetricDeclaration.getMetricHistory(subscriberState.getSubscriberProfile());
      
      // Check for journey metrics added while the journey was running
      if(! this.getJourneyMetricsDuring().containsKey(journeyMetricDeclaration.getID())) {
        continue;
      }
      long startMetricValue = this.getJourneyMetricsDuring().get(journeyMetricDeclaration.getID());
      long endMetricValue = metricHistory.getAllTimeBucket();
      long duringMetricValue = endMetricValue - startMetricValue;
      this.getJourneyMetricsDuring().put(journeyMetricDeclaration.getID(), duringMetricValue);
      subscriberStateUpdated = true;
    }
    
    return subscriberStateUpdated;
  }

  /**
   * populate journeyMetrics (post)
   * @return true if subscriber state has been updated
   */
  public boolean populateMetricsPost(SubscriberState subscriberState, Date now, int tenantID) 
  {
    boolean subscriberStateUpdated = false;
    
    //
    //  post metrics
    //
    for (JourneyMetricDeclaration journeyMetricDeclaration : Deployment.getJourneyMetricDeclarations().values()) {
      if (! this.getJourneyMetricsPost().containsKey(journeyMetricDeclaration.getID())) {
        Date journeyExitDay = RLMDateUtils.truncate(this.getJourneyExitDate(), Calendar.DATE, Calendar.SUNDAY, Deployment.getDeployment(tenantID).getBaseTimeZone());
        Date metricStartDay = RLMDateUtils.addDays(journeyExitDay, 1, Deployment.getDeployment(tenantID).getBaseTimeZone());
        Date metricEndDay = RLMDateUtils.addDays(journeyExitDay, journeyMetricDeclaration.getPostPeriodDays(), Deployment.getDeployment(tenantID).getBaseTimeZone());
        if (now.after(RLMDateUtils.addDays(metricEndDay, 1, Deployment.getDeployment(tenantID).getBaseTimeZone()))) {
          MetricHistory metricHistory = journeyMetricDeclaration.getMetricHistory(subscriberState.getSubscriberProfile());
          long postMetricValue = metricHistory.getValue(metricStartDay, metricEndDay);
          this.getJourneyMetricsPost().put(journeyMetricDeclaration.getID(), postMetricValue);
          subscriberStateUpdated = true;
        }
      }
    }
    
    return subscriberStateUpdated;
  }
  
}
