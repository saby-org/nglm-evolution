/****************************************************************************
*
*  TokenChange.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Date;

import org.apache.kafka.connect.data.*;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SubscriberStreamOutput;
import com.evolving.nglm.evolution.ActionManager.Action;
import com.evolving.nglm.evolution.ActionManager.ActionType;
import com.evolving.nglm.evolution.DeliveryRequest.Module;


public class WorkflowEvent extends SubscriberStreamOutput implements EvolutionEngineEvent
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
    schemaBuilder.name("workflow_change");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(subscriberStreamOutputSchema().version(),1));
    for (Field field : subscriberStreamOutputSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("eventDateTime", Timestamp.builder().schema());
    schemaBuilder.field("eventID", Schema.STRING_SCHEMA);
    schemaBuilder.field("workflowID", Schema.STRING_SCHEMA);
    schemaBuilder.field("moduleID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("featureID", Schema.OPTIONAL_STRING_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  // serde
  //

  private static ConnectSerde<WorkflowEvent> serde = new ConnectSerde<WorkflowEvent>(schema, false, WorkflowEvent.class, WorkflowEvent::pack, WorkflowEvent::unpack);

  //
  // accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<WorkflowEvent> serde() { return serde; }

  /****************************************
  *
  * data
  *
  ****************************************/

  private String subscriberID;
  private Date eventDateTime;
  private String eventID;
  private String workflowID;
  private String moduleID;
  private String featureID;
  
  /****************************************
  *
  * accessors - basic
  *
  ****************************************/

  //
  // accessor
  //

  @Override
  public String getSubscriberID() { return subscriberID; }
  public Date geteventDateTime() { return eventDateTime; }
  public String getEventID() { return eventID; }
  public String getWorkflowID() { return workflowID; }
  public String getModuleID() { return moduleID; }
  public String getFeatureID() { return featureID; }
  public ActionType getActionType() { return ActionType.TokenChange; }

  //
  //  setters
  //
/*
  public void setSubscriberID(String subscriberID) { this.subscriberID = subscriberID; }
  public void seteventDateTime(Date eventDateTime) { this.eventDateTime = eventDateTime; }
  public void setEventID(String eventID) { this.eventID = eventID; }
  public void setTokenCode(String tokenCode) { this.tokenCode = tokenCode; }
  public void setAction(String action) { this.action = action; }
  public void setReturnStatus(String returnStatus) { this.returnStatus = returnStatus; }
  public void setOrigin(String origin) { this.origin = origin; }
  public void setModuleID(String moduleID) { this.moduleID = moduleID; }
  public void setFeatureID(String featureID) { this.featureID = featureID; }
  */


  /*****************************************
  *
  * constructor (simple)
  *
  *****************************************/

  public WorkflowEvent(String subscriberID, Date eventDateTime, String eventID, String workflowID, String moduleID, String featureID)
  {
    this.subscriberID = subscriberID;
    this.eventDateTime = eventDateTime;
    this.eventID = eventID;
    this.workflowID = workflowID;
    this.moduleID = moduleID;
    this.featureID = featureID;
  }

  /*****************************************
   *
   * constructor unpack
   *
   *****************************************/
  public WorkflowEvent(SchemaAndValue schemaAndValue, String subscriberID, Date eventDateTime, String eventID, String workflowID, String moduleID, String featureID)
  {
    super(schemaAndValue);
    this.subscriberID = subscriberID;
    this.eventDateTime = eventDateTime;
    this.eventID = eventID;
    this.workflowID = workflowID;
    this.moduleID = moduleID;
    this.featureID = featureID;
  }

  /*****************************************
  *
  * pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    WorkflowEvent tokenChange = (WorkflowEvent) value;
    Struct struct = new Struct(schema);
    packSubscriberStreamOutput(struct,tokenChange);
    struct.put("subscriberID",tokenChange.getSubscriberID());
    struct.put("eventDateTime",tokenChange.geteventDateTime());
    struct.put("eventID", tokenChange.getEventID());
    struct.put("workflowID", tokenChange.getWorkflowID());
    struct.put("moduleID", tokenChange.getModuleID());
    struct.put("featureID", tokenChange.getFeatureID());
    return struct;
  }

  /*****************************************
  *
  * unpack
  *
  *****************************************/

  public static WorkflowEvent unpack(SchemaAndValue schemaAndValue)
  {
    //
    // data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion1(schema.version()) : null;

    //
    // unpack
    //

    Struct valueStruct = (Struct) value;
    String subscriberID = valueStruct.getString("subscriberID");
    Date eventDateTime = (Date) valueStruct.get("eventDateTime");
    String eventID = valueStruct.getString("eventID");
    String workflowID = valueStruct.getString("workflowID");
    String moduleID =  valueStruct.getString("moduleID") ;
    String featureID = valueStruct.getString("featureID");

    //
    // validate
    //

    //
    // return
    //

    return new WorkflowEvent(schemaAndValue, subscriberID, eventDateTime, eventID, workflowID, moduleID, featureID);
  }
  
  
  @Override
  public Date getEventDate()
  {
    return geteventDateTime();
  }
  
  @Override
  public Schema subscriberStreamEventSchema()
  {
    return schema;
  }
  
  @Override
  public Object subscriberStreamEventPack(Object value)
  {
    return pack(value);
  }
  
  @Override
  public String getEventName()
  {
    return "workflow change";
  }
}
