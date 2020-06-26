package com.evolving.nglm.evolution;

import java.util.Date;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SubscriberStreamEvent;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.ActionManager.Action;
import com.evolving.nglm.evolution.ActionManager.ActionType;

public class ExecuteActionOtherSubscriber implements SubscriberStreamEvent, Action
{
  

  /*****************************************
   *
   * schema
   *
   *****************************************/

  //
  // schema
  //

  private static Schema schema = null;
  static
    {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct();
      schemaBuilder.name("execute_action_other_subscriber");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
      schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
      schemaBuilder.field("actionManagerClassName", Schema.STRING_SCHEMA);
      schemaBuilder.field("originatingSubscriberID", Schema.STRING_SCHEMA);
      schemaBuilder.field("originalJourneyID", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("originatingNodeID", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("outstandingDeliveryRequestID", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("originatedJourneyState", JourneyState.serde().optionalSchema());
      
      schema = schemaBuilder.build();
    };

  //
  // serde
  //

  private static ConnectSerde<ExecuteActionOtherSubscriber> serde = new ConnectSerde<ExecuteActionOtherSubscriber>(schema, false, ExecuteActionOtherSubscriber.class, ExecuteActionOtherSubscriber::pack, ExecuteActionOtherSubscriber::unpack);

  //
  // accessor
  //

  public static Schema schema()
  {
    return schema;
  }

  public static ConnectSerde<ExecuteActionOtherSubscriber> serde()
  {
    return serde;
  }

  public Schema subscriberStreamEventSchema()
  {
    return schema();
  }

  /*****************************************
   *
   * data
   *
   *****************************************/
  private String subscriberID;
  private String actionManagerClassName;
  private String originatingSubscriberID;
  private String originalJourneyID;
  private String originatingNodeID;
  private String outstandingDeliveryRequestID;
  private JourneyState originatedJourneyState;

  //
  // accessors
  //

  @Override
  public String getSubscriberID()
  {
    return subscriberID;
  }
  
  public String getActionManagerClassName()
  {
    return actionManagerClassName;
  }

  public void setActionManagerClassName(String actionManagerClassName)
  {
    this.actionManagerClassName = actionManagerClassName;
  }

  public String getOriginatingSubscriberID()
  {
    return originatingSubscriberID;
  }

  public void setOriginatingSubscriberID(String originatingSubscriberID)
  {
    this.originatingSubscriberID = originatingSubscriberID;
  }

  public String getOriginalJourneyID()
  {
    return originalJourneyID;
  }

  public void setOriginalJourneyID(String originalJourneyID)
  {
    this.originalJourneyID = originalJourneyID;
  }

  public String getOriginatingNodeID()
  {
    return originatingNodeID;
  }

  public void setOriginatingNodeID(String originatingNodeID)
  {
    this.originatingNodeID = originatingNodeID;
  }

  public JourneyState getOriginatedJourneyState()
  {
    return originatedJourneyState;
  }
  
  public String getOutstandingDeliveryRequestID()
  {
    return outstandingDeliveryRequestID;
  }

  public void setOutstandingDeliveryRequestID(String outstandingDeliveryRequestID)
  {
    this.outstandingDeliveryRequestID = outstandingDeliveryRequestID;
  }

  public void setOriginatedJourneyState(JourneyState originatedJourneyState)
  {
    this.originatedJourneyState = originatedJourneyState;
  }

  public void setSubscriberID(String subscriberID)
  {
    this.subscriberID = subscriberID;
  }
  
  public Date getEventDate()
  {
    return SystemTime.getCurrentTime();
  }

  /*****************************************
   *
   * constructor and unpack
   *
   *****************************************/

  public ExecuteActionOtherSubscriber(String subscriberID, String actionManagerClassName, String originatingSubscriberID, String originalJourneyID, String originatingNodeID, String outstandingDeliveryRequestID, JourneyState originatedJourneyState)
    {
      this.subscriberID = subscriberID;
      this.actionManagerClassName = actionManagerClassName;
      this.originatingSubscriberID = getOriginatingSubscriberID();
      this.originalJourneyID = originalJourneyID;
      this.originatingNodeID = originatingNodeID;
      this.outstandingDeliveryRequestID = outstandingDeliveryRequestID;
      this.originatedJourneyState = originatedJourneyState;
    }

  /*****************************************
   *
   * pack
   *
   *****************************************/

  public static Object pack(Object value)
  {
    ExecuteActionOtherSubscriber executeActionOtherSubscriber = (ExecuteActionOtherSubscriber) value;
    Struct struct = new Struct(schema);
    struct.put("subscriberID", executeActionOtherSubscriber.getSubscriberID());
    struct.put("actionManagerClassName", executeActionOtherSubscriber.getActionManagerClassName());
    struct.put("originatingSubscriberID", executeActionOtherSubscriber.getOriginatingSubscriberID());
    struct.put("originalJourneyID", executeActionOtherSubscriber.getOriginalJourneyID());
    struct.put("originatingNodeID", executeActionOtherSubscriber.getOriginatingNodeID());
    struct.put("outstandingDeliveryRequestID", executeActionOtherSubscriber.getOutstandingDeliveryRequestID());
    struct.put("originatedJourneyState", JourneyState.serde().packOptional(executeActionOtherSubscriber.getOriginatedJourneyState()));
    return struct;
  }

  //
  // subscriberStreamEventPack
  //

  public Object subscriberStreamEventPack(Object value)
  {
    return pack(value);
  }

  /*****************************************
   *
   * unpack
   *
   *****************************************/

  public static ExecuteActionOtherSubscriber unpack(SchemaAndValue schemaAndValue)
  {
    //
    // data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;

    //
    // unpack
    //

    Struct valueStruct = (Struct) value;
    String subscriberID = valueStruct.getString("subscriberID");
    String actionManagerClassName = valueStruct.getString("actionManagerClassName");
    String originatingSubscriberID = valueStruct.getString("originatingSubscriberID");
    String originalJourneyID = valueStruct.getString("originalJourneyID");
    String originatingNodeID = valueStruct.getString("originatingNodeID");
    String outstandingDeliveryRequestID = valueStruct.getString("outstandingDeliveryRequestID");
    JourneyState originatedJourneyState = JourneyState.serde().unpackOptional((new SchemaAndValue(schema.field("originatedJourneyState").schema(), valueStruct.get("originatedJourneyState"))));
    //
    // return
    //

    return new ExecuteActionOtherSubscriber(subscriberID, actionManagerClassName, originatingSubscriberID, originalJourneyID, originatingNodeID, outstandingDeliveryRequestID, originatedJourneyState);
  }

  @Override
  public String toString()
  {
    return "ExecuteActionOtherSubscriber [subscriberID=" + subscriberID + ", actionManagerClassName=" + actionManagerClassName + ", originatingSubscriberID=" + originatingSubscriberID + ", originalJourneyID=" + originalJourneyID + ", originatingNodeID=" + originatingNodeID + ", outstandingDeliveryRequestID=" + outstandingDeliveryRequestID + ", originatedJourneyState=" + originatedJourneyState + "]";
  }

  @Override
  public ActionType getActionType()
  {
    return ActionType.ExecuteActionOtherSubscriber;
  }
}
