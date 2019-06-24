/*****************************************************************************
*
*  JourneyState.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(2));
    schemaBuilder.field("journeyInstanceID", Schema.STRING_SCHEMA);
    schemaBuilder.field("journeyID", Schema.STRING_SCHEMA);
    schemaBuilder.field("journeyNodeID", Schema.STRING_SCHEMA);
    schemaBuilder.field("journeyParameters", ParameterMap.schema());
    schemaBuilder.field("journeyEntryDate", Timestamp.SCHEMA);
    schemaBuilder.field("journeyExitDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("journeyCloseDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("journeyMetricsPrior", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT64_SCHEMA).name("journeystate_metrics_prior").schema());
    schemaBuilder.field("journeyMetricsDuring", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT64_SCHEMA).name("journeystate_metrics_during").schema());
    schemaBuilder.field("journeyMetricsPost", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT64_SCHEMA).name("journeystate_metrics_post").schema());
    schemaBuilder.field("journeyNodeEntryDate", Timestamp.SCHEMA);
    schemaBuilder.field("journeyOutstandingJourneyRequestID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("journeyOutstandingJourneyInstanceID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("journeyOutstandingDeliveryRequestID", Schema.OPTIONAL_STRING_SCHEMA);
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
  private Date journeyEntryDate;
  private Date journeyExitDate;
  private Date journeyCloseDate;
  private Map<String,Long> journeyMetricsPrior;
  private Map<String,Long> journeyMetricsDuring;
  private Map<String,Long> journeyMetricsPost;
  private Date journeyNodeEntryDate;
  private String journeyOutstandingJourneyRequestID;
  private String journeyOutstandingJourneyInstanceID;
  private String journeyOutstandingDeliveryRequestID;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getJourneyInstanceID() { return journeyInstanceID; }
  public String getJourneyID() { return journeyID; }
  public String getJourneyNodeID() { return journeyNodeID; }
  public ParameterMap getJourneyParameters() { return journeyParameters; }
  public Date getJourneyEntryDate() { return journeyEntryDate; }
  public Date getJourneyExitDate() { return journeyExitDate; }
  public Date getJourneyCloseDate() { return journeyCloseDate; }
  public Map<String,Long> getJourneyMetricsPrior() { return journeyMetricsPrior; }
  public Map<String,Long> getJourneyMetricsDuring() { return journeyMetricsDuring; }
  public Map<String,Long> getJourneyMetricsPost() { return journeyMetricsPost; }
  public Date getJourneyNodeEntryDate() { return journeyNodeEntryDate; }
  public String getJourneyOutstandingJourneyRequestID() { return journeyOutstandingJourneyRequestID; }
  public String getJourneyOutstandingJourneyInstanceID() { return journeyOutstandingJourneyInstanceID; }
  public String getJourneyOutstandingDeliveryRequestID() { return journeyOutstandingDeliveryRequestID; }

  /*****************************************
  *
  *  setters
  *
  *****************************************/

  public void setJourneyNodeID(String journeyNodeID, Date journeyNodeEntryDate) { this.journeyNodeID = journeyNodeID; this.journeyNodeEntryDate = journeyNodeEntryDate; this.journeyOutstandingJourneyRequestID = null; this.journeyOutstandingJourneyInstanceID = null; this.journeyOutstandingDeliveryRequestID = null; }
  public void setJourneyOutstandingJourneyRequestID(String journeyOutstandingJourneyRequestID) { this.journeyOutstandingJourneyRequestID = journeyOutstandingJourneyRequestID; }
  public void setJourneyOutstandingJourneyInstanceID(String journeyOutstandingJourneyInstanceID) { this.journeyOutstandingJourneyInstanceID = journeyOutstandingJourneyInstanceID; }
  public void setJourneyOutstandingDeliveryRequestID(String journeyOutstandingDeliveryRequestID) { this.journeyOutstandingDeliveryRequestID = journeyOutstandingDeliveryRequestID; }
  public void setJourneyExitDate(Date journeyExitDate) { this.journeyExitDate = journeyExitDate; }
  public void setJourneyCloseDate(Date journeyCloseDate) { this.journeyCloseDate = journeyCloseDate; }

  /*****************************************
  *
  *  constructor -- standard
  *
  *****************************************/

  public JourneyState(EvolutionEventContext context, Journey journey, Map<String, Object> journeyParameters, Date journeyEntryDate)
  {
    this.journeyInstanceID = context.getUniqueKey();
    this.journeyID = journey.getJourneyID();
    this.journeyNodeID = journey.getStartNodeID();
    this.journeyParameters = new ParameterMap(journeyParameters);
    this.journeyEntryDate = journeyEntryDate;
    this.journeyExitDate = null;
    this.journeyCloseDate = null;
    this.journeyMetricsPrior = new HashMap<String,Long>();
    this.journeyMetricsDuring = new HashMap<String,Long>();
    this.journeyMetricsPost = new HashMap<String,Long>();
    this.journeyNodeEntryDate = journeyEntryDate;
    this.journeyOutstandingJourneyRequestID = null;
    this.journeyOutstandingJourneyInstanceID = null;
    this.journeyOutstandingDeliveryRequestID = null;    
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public JourneyState(String journeyInstanceID, String journeyID, String journeyNodeID, ParameterMap journeyParameters, Date journeyEntryDate, Date journeyExitDate, Date journeyCloseDate, Map<String,Long> journeyMetricsPrior, Map<String,Long> journeyMetricsDuring, Map<String,Long> journeyMetricsPost, Date journeyNodeEntryDate, String journeyOutstandingJourneyRequestID, String journeyOutstandingJourneyInstanceID, String journeyOutstandingDeliveryRequestID)
  {
    this.journeyInstanceID = journeyInstanceID;
    this.journeyID = journeyID;
    this.journeyNodeID = journeyNodeID;
    this.journeyParameters = journeyParameters;
    this.journeyEntryDate = journeyEntryDate;
    this.journeyExitDate = journeyExitDate;
    this.journeyCloseDate = journeyCloseDate;
    this.journeyMetricsPrior = journeyMetricsPrior;
    this.journeyMetricsDuring = journeyMetricsDuring;
    this.journeyMetricsPost = journeyMetricsPost;
    this.journeyNodeEntryDate = journeyNodeEntryDate;
    this.journeyOutstandingJourneyRequestID = journeyOutstandingJourneyRequestID;
    this.journeyOutstandingJourneyInstanceID = journeyOutstandingJourneyInstanceID;
    this.journeyOutstandingDeliveryRequestID = journeyOutstandingDeliveryRequestID;
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
    this.journeyEntryDate = journeyState.getJourneyEntryDate();
    this.journeyExitDate = journeyState.getJourneyExitDate();
    this.journeyCloseDate = journeyState.getJourneyCloseDate();
    this.journeyMetricsPrior = new HashMap<String,Long>(journeyState.getJourneyMetricsPrior());
    this.journeyMetricsDuring = new HashMap<String,Long>(journeyState.getJourneyMetricsDuring());
    this.journeyMetricsPost = new HashMap<String,Long>(journeyState.getJourneyMetricsPost());
    this.journeyNodeEntryDate = journeyState.getJourneyNodeEntryDate();
    this.journeyOutstandingJourneyRequestID = journeyState.getJourneyOutstandingJourneyRequestID();
    this.journeyOutstandingJourneyInstanceID = journeyState.getJourneyOutstandingJourneyInstanceID();
    this.journeyOutstandingDeliveryRequestID = journeyState.getJourneyOutstandingDeliveryRequestID();
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
    struct.put("journeyEntryDate", journeyState.getJourneyEntryDate());
    struct.put("journeyExitDate", journeyState.getJourneyExitDate());
    struct.put("journeyCloseDate", journeyState.getJourneyCloseDate());
    struct.put("journeyMetricsPrior", journeyState.getJourneyMetricsPrior());
    struct.put("journeyMetricsDuring", journeyState.getJourneyMetricsDuring());
    struct.put("journeyMetricsPost", journeyState.getJourneyMetricsPost());
    struct.put("journeyNodeEntryDate", journeyState.getJourneyNodeEntryDate());
    struct.put("journeyOutstandingJourneyRequestID", journeyState.getJourneyOutstandingJourneyRequestID());
    struct.put("journeyOutstandingJourneyInstanceID", journeyState.getJourneyOutstandingJourneyInstanceID());
    struct.put("journeyOutstandingDeliveryRequestID", journeyState.getJourneyOutstandingDeliveryRequestID());
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
    Date journeyEntryDate = (Date) valueStruct.get("journeyEntryDate");
    Date journeyExitDate = (Date) valueStruct.get("journeyExitDate");
    Date journeyCloseDate = (schemaVersion >= 2) ? (Date) valueStruct.get("journeyCloseDate") : journeyExitDate;
    Map<String,Long> journeyMetricsPrior = (schemaVersion >= 2) ? (Map<String,Long>) valueStruct.get("journeyMetricsPrior") : new HashMap<String,Long>();
    Map<String,Long> journeyMetricsDuring = (schemaVersion >= 2) ? (Map<String,Long>) valueStruct.get("journeyMetricsDuring") : new HashMap<String,Long>();
    Map<String,Long> journeyMetricsPost = (schemaVersion >= 2) ? (Map<String,Long>) valueStruct.get("journeyMetricsPost") : new HashMap<String,Long>();
    Date journeyNodeEntryDate = (Date) valueStruct.get("journeyNodeEntryDate");
    String journeyOutstandingJourneyRequestID = valueStruct.getString("journeyOutstandingJourneyRequestID");
    String journeyOutstandingJourneyInstanceID = valueStruct.getString("journeyOutstandingJourneyInstanceID");
    String journeyOutstandingDeliveryRequestID = valueStruct.getString("journeyOutstandingDeliveryRequestID");

    //
    //  return
    //

    return new JourneyState(journeyInstanceID, journeyID, journeyNodeID, journeyParameters, journeyEntryDate, journeyExitDate, journeyCloseDate, journeyMetricsPrior, journeyMetricsDuring, journeyMetricsPost, journeyNodeEntryDate, journeyOutstandingJourneyRequestID, journeyOutstandingJourneyInstanceID, journeyOutstandingDeliveryRequestID);
  }
}
