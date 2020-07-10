/*****************************************************************************
*
*  JourneyMetric.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SubscriberStreamEvent;
import com.evolving.nglm.core.SubscriberStreamOutput;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;

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

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JourneyMetric extends SubscriberStreamOutput implements Comparable
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
    schemaBuilder.name("journey_metric");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(subscriberStreamOutputSchema().version(),8));
    for (Field field : subscriberStreamOutputSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("journeyMetricID", Schema.STRING_SCHEMA);
    schemaBuilder.field("journeyInstanceID", Schema.STRING_SCHEMA);
    schemaBuilder.field("journeyID", Schema.STRING_SCHEMA);
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("journeyExitDate", Timestamp.SCHEMA);
    schemaBuilder.field("journeyMetricsPrior", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT64_SCHEMA).name("journeymetric_metrics_prior").schema());
    schemaBuilder.field("journeyMetricsDuring", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT64_SCHEMA).name("journeymetric_metrics_during").schema());
    schemaBuilder.field("journeyMetricsPost", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT64_SCHEMA).name("journeymetric_metrics_post").schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<JourneyMetric> serde = new ConnectSerde<JourneyMetric>(schema, false, JourneyMetric.class, JourneyMetric::pack, JourneyMetric::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<JourneyMetric> serde() { return serde; }
  public Schema subscriberStreamEventSchema() { return schema(); }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private String journeyMetricID;
  private String journeyInstanceID;
  private String journeyID;
  private String subscriberID;
  private Date journeyExitDate;
  private Map<String,Long> journeyMetricsPrior;
  private Map<String,Long> journeyMetricsDuring;
  private Map<String,Long> journeyMetricsPost;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getJourneyMetricID() { return journeyMetricID; }
  public String getJourneyInstanceID() { return journeyInstanceID; }
  public String getJourneyID() { return journeyID; }
  public String getSubscriberID() { return subscriberID; }
  public Date getJourneyExitDate() { return journeyExitDate; }
  public Map<String,Long> getJourneyMetricsPrior() { return journeyMetricsPrior; }
  public Map<String,Long> getJourneyMetricsDuring() { return journeyMetricsDuring; }
  public Map<String,Long> getJourneyMetricsPost() { return journeyMetricsPost; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public JourneyMetric(EvolutionEventContext context, String subscriberID, JourneyState journeyState)
  {
    this.journeyMetricID = context.getUniqueKey();
    this.journeyInstanceID = journeyState.getJourneyInstanceID();
    this.journeyID = journeyState.getJourneyID();
    this.subscriberID = subscriberID;
    this.journeyExitDate = journeyState.getJourneyExitDate();
    this.journeyMetricsPrior = new HashMap<String,Long>(journeyState.getJourneyMetricsPrior());
    this.journeyMetricsDuring = new HashMap<String,Long>(journeyState.getJourneyMetricsDuring());
    this.journeyMetricsPost = new HashMap<String,Long>(journeyState.getJourneyMetricsPost());
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  private JourneyMetric(SchemaAndValue schemaAndValue, String journeyMetricID, String journeyInstanceID, String journeyID, String subscriberID, Date journeyExitDate, Map<String,Long> journeyMetricsPrior, Map<String,Long> journeyMetricsDuring, Map<String,Long> journeyMetricsPost)
  {
    super(schemaAndValue);
    this.journeyMetricID = journeyMetricID;
    this.journeyInstanceID = journeyInstanceID;
    this.journeyID = journeyID;
    this.subscriberID = subscriberID;
    this.journeyExitDate = journeyExitDate;
    this.journeyMetricsPrior = journeyMetricsPrior;
    this.journeyMetricsDuring = journeyMetricsDuring;
    this.journeyMetricsPost = journeyMetricsPost;
  }

  /*****************************************
  *
  *  constructor -- copy
  *
  *****************************************/

  public JourneyMetric(JourneyMetric journeyMetric)
  {
    super(journeyMetric);
    this.journeyMetricID = journeyMetric.getJourneyMetricID();
    this.journeyInstanceID = journeyMetric.getJourneyInstanceID();
    this.journeyID = journeyMetric.getJourneyID();
    this.subscriberID = journeyMetric.getSubscriberID();
    this.journeyExitDate = journeyMetric.getJourneyExitDate();
    this.journeyMetricsPrior = new HashMap<String,Long>(journeyMetricsPrior);
    this.journeyMetricsDuring = new HashMap<String,Long>(journeyMetricsDuring);
    this.journeyMetricsPost = new HashMap<String,Long>(journeyMetricsPost);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    JourneyMetric journeyMetric = (JourneyMetric) value;
    Struct struct = new Struct(schema);
    packSubscriberStreamOutput(struct,journeyMetric);
    struct.put("journeyMetricID", journeyMetric.getJourneyMetricID());
    struct.put("journeyInstanceID", journeyMetric.getJourneyInstanceID());
    struct.put("journeyID", journeyMetric.getJourneyID());
    struct.put("subscriberID", journeyMetric.getSubscriberID());
    struct.put("journeyExitDate", journeyMetric.getJourneyExitDate());
    struct.put("journeyMetricsPrior", journeyMetric.getJourneyMetricsPrior());
    struct.put("journeyMetricsDuring", journeyMetric.getJourneyMetricsDuring());
    struct.put("journeyMetricsPost", journeyMetric.getJourneyMetricsPost());
    return struct;
  }
  
  //
  //  subscriberStreamEventPack
  //

  public Object subscriberStreamEventPack(Object value) { return pack(value); }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static JourneyMetric unpack(SchemaAndValue schemaAndValue)
  {
    //
    //  data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion1(schema.version()) : null;

    //
    //  unpack
    //

    Struct valueStruct = (Struct) value;
    String journeyMetricID = valueStruct.getString("journeyMetricID");
    String journeyInstanceID = valueStruct.getString("journeyInstanceID");
    String journeyID = valueStruct.getString("journeyID");
    String subscriberID = valueStruct.getString("subscriberID");
    Date journeyExitDate = (Date) valueStruct.get("journeyExitDate");
    Map<String,Long> journeyMetricsPrior = (Map<String,Long>) valueStruct.get("journeyMetricsPrior");
    Map<String,Long> journeyMetricsDuring = (Map<String,Long>) valueStruct.get("journeyMetricsDuring");
    Map<String,Long> journeyMetricsPost = (Map<String,Long>) valueStruct.get("journeyMetricsPost");
    
    //
    //  return
    //

    return new JourneyMetric(schemaAndValue, journeyMetricID, journeyInstanceID, journeyID, subscriberID, journeyExitDate, journeyMetricsPrior, journeyMetricsDuring, journeyMetricsPost);
  }

  /*****************************************
  *
  *  compareTo
  *
  *****************************************/

  public int compareTo(Object obj)
  {
    int result = -1;
    if (obj instanceof JourneyMetric)
      {
        JourneyMetric entry = (JourneyMetric) obj;
        result = journeyExitDate.compareTo(entry.getJourneyExitDate());
        if (result == 0) result = journeyMetricID.compareTo(entry.getJourneyMetricID());
      }
    return result;
  }
}
