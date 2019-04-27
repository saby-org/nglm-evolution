/*****************************************
*
*  JourneyRequest.java
*
*****************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SubscriberStreamEvent;
import com.evolving.nglm.core.SubscriberStreamOutput;
import com.evolving.nglm.evolution.ActionManager.Action;
import com.evolving.nglm.evolution.ActionManager.ActionType;
import com.evolving.nglm.evolution.EvolutionEngine.EvolutionEventContext;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import java.util.Date;

public class JourneyRequest implements SubscriberStreamEvent, SubscriberStreamOutput, Action
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
    schemaBuilder.name("journey_request");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("journeyRequestID", Schema.STRING_SCHEMA);
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("eventDate", Timestamp.SCHEMA);
    schemaBuilder.field("journeyID", Schema.STRING_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<JourneyRequest> serde = new ConnectSerde<JourneyRequest>(schema, false, JourneyRequest.class, JourneyRequest::pack, JourneyRequest::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<JourneyRequest> serde() { return serde; }
  public Schema subscriberStreamEventSchema() { return schema(); }

  /****************************************
  *
  *  data
  *
  *****************************************/

  private String journeyRequestID;
  private String subscriberID;
  private Date eventDate;
  private String journeyID;

  //
  //  transient
  //

  private boolean eligible;
      
  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getJourneyRequestID() { return journeyRequestID; }
  public String getSubscriberID() { return subscriberID; }
  public Date getEventDate() { return eventDate; }
  public String getJourneyID() { return journeyID; }
  public boolean getEligible() { return eligible; }
  public ActionType getActionType() { return ActionType.JourneyRequest; }

  //
  //  setters
  //

  public void setEligible(boolean eligible) { this.eligible = eligible; }

  /*****************************************
  *
  *  constructor -- journey
  *
  *****************************************/

  public JourneyRequest(EvolutionEventContext context, String journeyID)
  {
    this.journeyRequestID = context.getUniqueKey();
    this.subscriberID = context.getSubscriberState().getSubscriberID();
    this.eventDate = context.now();
    this.journeyID = journeyID;
    this.eligible = false;
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public JourneyRequest(String journeyRequestID, String subscriberID, Date eventDate, String journeyID)
  {
    this.journeyRequestID = journeyRequestID;
    this.subscriberID = subscriberID;
    this.eventDate = eventDate;
    this.journeyID = journeyID;
    this.eligible = false;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    JourneyRequest journeyRequest = (JourneyRequest) value;
    Struct struct = new Struct(schema);
    struct.put("journeyRequestID", journeyRequest.getJourneyRequestID());
    struct.put("subscriberID", journeyRequest.getSubscriberID());
    struct.put("eventDate", journeyRequest.getEventDate());
    struct.put("journeyID", journeyRequest.getJourneyID());
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

  public static JourneyRequest unpack(SchemaAndValue schemaAndValue)
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
    String journeyRequestID = valueStruct.getString("journeyRequestID");
    String subscriberID = valueStruct.getString("subscriberID");
    Date eventDate = (Date) valueStruct.get("eventDate");
    String journeyID = valueStruct.getString("journeyID");

    
    //
    //  return
    //

    return new JourneyRequest(journeyRequestID, subscriberID, eventDate, journeyID);
  }
}
