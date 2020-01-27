/*****************************************************************************
*
*  JourneyState.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;
import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JourneyState
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
    schemaBuilder.name("journey_state");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(4));
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
    schemaBuilder.field("journeyHistory", JourneyHistory.schema());
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
  private JourneyHistory journeyHistory;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

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
  public JourneyHistory getJourneyHistory() { return journeyHistory; }

  /*****************************************
  *
  *  setters
  *
  *****************************************/

  public void setJourneyNodeID(String journeyNodeID, Date journeyNodeEntryDate) { this.journeyNodeID = journeyNodeID; this.journeyNodeEntryDate = journeyNodeEntryDate; this.journeyOutstandingDeliveryRequestID = null; }
  public void setJourneyOutstandingDeliveryRequestID(String journeyOutstandingDeliveryRequestID) { this.journeyOutstandingDeliveryRequestID = journeyOutstandingDeliveryRequestID; }
  public void setJourneyExitDate(Date journeyExitDate) { this.journeyExitDate = journeyExitDate; }
  public void setJourneyCloseDate(Date journeyCloseDate) { this.journeyCloseDate = journeyCloseDate; }

  /*****************************************
  *
  *  constructor -- standard
  *
  *****************************************/

  public JourneyState(EvolutionEventContext context, Journey journey, Map<String, Object> journeyParameters, Date journeyEntryDate, JourneyHistory journeyHistory)
  {
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
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public JourneyState(String journeyInstanceID, String journeyID, String journeyNodeID, ParameterMap journeyParameters, ParameterMap journeyActionManagerContext, Date journeyEntryDate, Date journeyExitDate, Date journeyCloseDate, Map<String,Long> journeyMetricsPrior, Map<String,Long> journeyMetricsDuring, Map<String,Long> journeyMetricsPost, Date journeyNodeEntryDate, String journeyOutstandingDeliveryRequestID, JourneyHistory journeyHistory)
  {
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
    this.journeyHistory = journeyHistory;
  }

  /*****************************************
  *
  *  constructor -- copy
  *
  *****************************************/

  public JourneyState(JourneyState journeyState)
  {
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
    this.journeyHistory = journeyState.getJourneyHistory();
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
    struct.put("journeyHistory", JourneyHistory.serde().pack(journeyState.getJourneyHistory()));
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
    JourneyHistory journeyHistory = JourneyHistory.serde().unpack(new SchemaAndValue(schema.field("journeyHistory").schema(), valueStruct.get("journeyHistory")));
    
    //
    //  return
    //

    return new JourneyState(journeyInstanceID, journeyID, journeyNodeID, journeyParameters, journeyActionManagerContext, journeyEntryDate, journeyExitDate, journeyCloseDate, journeyMetricsPrior, journeyMetricsDuring, journeyMetricsPost, journeyNodeEntryDate, journeyOutstandingDeliveryRequestID, journeyHistory);
  }
}
