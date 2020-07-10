/*****************************************************************************
*
*  JourneyStatistic.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SubscriberStreamEvent;
import com.evolving.nglm.core.SubscriberStreamOutput;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;
import com.evolving.nglm.evolution.Journey.SubscriberJourneyStatus;
import com.evolving.nglm.evolution.Journey.SubscriberJourneyStatusField;
import com.evolving.nglm.evolution.JourneyHistory.NodeHistory;
import com.evolving.nglm.evolution.JourneyHistory.RewardHistory;
import com.evolving.nglm.evolution.JourneyHistory.StatusHistory;
import org.apache.kafka.connect.data.*;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class ExternalAPIOutput extends SubscriberStreamOutput implements Comparable
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
    schemaBuilder.name("external_api_output");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(subscriberStreamOutputSchema().version(),1));
    for (Field field : subscriberStreamOutputSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("topicID", Schema.STRING_SCHEMA);
    schemaBuilder.field("jsonString", Schema.STRING_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<ExternalAPIOutput> serde = new ConnectSerde<ExternalAPIOutput>(schema, false, ExternalAPIOutput.class, ExternalAPIOutput::pack, ExternalAPIOutput::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<ExternalAPIOutput> serde() { return serde; }
  
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private String topicID;
  private String jsonString;
  
  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getTopicID() { return topicID; }
  public String getJsonString() { return jsonString; }

  /*****************************************
  *
  *  constructor simple
  *
  *****************************************/

  public ExternalAPIOutput(String topicID, String jsonString)
  {
    this.topicID = topicID;
    this.jsonString = jsonString;
  }

  /*****************************************
  *
  *  constructor unpack
  *
  *****************************************/

  public ExternalAPIOutput(SchemaAndValue schemaAndValue, String topicID, String jsonString)
  {
    super(schemaAndValue);
    this.topicID = topicID;
    this.jsonString = jsonString;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    ExternalAPIOutput externalAPIOutput = (ExternalAPIOutput) value;
    Struct struct = new Struct(schema);
    packSubscriberStreamOutput(struct,externalAPIOutput);
    struct.put("topicID", externalAPIOutput.getTopicID());
    struct.put("jsonString", externalAPIOutput.getJsonString());
    return struct;
  }
  
  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static ExternalAPIOutput unpack(SchemaAndValue schemaAndValue)
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
    String topicID = valueStruct.getString("topicID");
    String jsonString = valueStruct.getString("jsonString");
    //
    //  return
    //

    return new ExternalAPIOutput(schemaAndValue, topicID, jsonString);
  }
  
  /*****************************************
  *
  *  compareTo
  *
  *****************************************/

  public int compareTo(Object obj)
  {
    int result = -1;
    if (obj instanceof ExternalAPIOutput)
      {
        ExternalAPIOutput entry = (ExternalAPIOutput) obj;
        result = topicID.compareTo(entry.getTopicID());
      }
    return result;
  }
  
}
