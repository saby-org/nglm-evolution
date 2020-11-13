/*****************************************************************************
*
*  VoucherAction.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Date;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.ActionManager.Action;
import com.evolving.nglm.evolution.ActionManager.ActionType;
import com.evolving.nglm.evolution.EvolutionEngine.VoucherActionManager.Operation;

public class VoucherAction implements EvolutionEngineEvent, Action, VoucherRedemption, VoucherValidation
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
      schemaBuilder.name("VoucherAction");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
      schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
      schemaBuilder.field("eventDate", Timestamp.SCHEMA);
      schemaBuilder.field("voucherCode", Schema.STRING_SCHEMA);
      schemaBuilder.field("actionStatus", Schema.STRING_SCHEMA);
      schemaBuilder.field("actionStatusCode", Schema.INT32_SCHEMA);
      schemaBuilder.field("operation", Schema.STRING_SCHEMA);
      schema = schemaBuilder.build();
    };

  //
  // serde
  //

  private static ConnectSerde<VoucherAction> serde = new ConnectSerde<VoucherAction>(schema, false, VoucherAction.class, VoucherAction::pack, VoucherAction::unpack);

  //
  // accessor
  //

  public static Schema schema()
  {
    return schema;
  }

  public static ConnectSerde<VoucherAction> serde()
  {
    return serde;
  }

  public Schema subscriberStreamEventSchema()
  {
    return schema();
  }

  //
  // logger
  //

  private static final Logger log = LoggerFactory.getLogger(VoucherAction.class);

  /****************************************
   *
   * data
   *
   ****************************************/
  
  private String subscriberID;
  private Date eventDate;
  private String voucherCode;
  private String actionStatus;
  private Integer actionStatusCode;
  private String operation;
  
  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  @Override public String getEventName() 
  { 
    if (Operation.fromExternalRepresentation(operation) == Operation.Redeem)
      {
        return "VoucherRedemption";
      }
    else if (Operation.fromExternalRepresentation(operation) == Operation.Validate)
      {
        return "VoucherValidation";
      }
    return operation; 
  }
  @Override public String getSubscriberID() { return subscriberID; }
  @Override public Date getEventDate() { return eventDate; }
  public String getVoucherCode() { return voucherCode; };
  public String getActionStatus() { return actionStatus; }
  public Integer getActionStatusCode() { return actionStatusCode; }
  public String getOperation() { return operation; }
  @Override public ActionType getActionType() { return ActionType.VoucherChange; }
  
  /*****************************************
  *
  *  setters
  *
  *****************************************/
  
  public void setActionStatus(String actionStatus) { this.actionStatus = actionStatus; }
  public void setActionStatusCode(Integer actionStatusCode) { this.actionStatusCode = actionStatusCode; }
  
  /*****************************************
  *
  * constructor
  *
  *****************************************/
  
  public VoucherAction(String subscriberID, Date eventDate, String voucherCode, String actionStatus, Integer actionStatusCode, String operation)
  {
    super();
    this.subscriberID = subscriberID;
    this.eventDate = eventDate;
    this.voucherCode = voucherCode;
    this.actionStatus = actionStatus;
    this.actionStatusCode = actionStatusCode;
    this.operation = operation;
  }
  
  /*****************************************
  *
  * pack
  *
  *****************************************/

 public static Object pack(Object value)
 {
   VoucherAction event = (VoucherAction) value;
   Struct struct = new Struct(schema);
   struct.put("subscriberID", event.getSubscriberID());
   struct.put("eventDate", event.getEventDate());
   struct.put("voucherCode", event.getVoucherCode());
   struct.put("actionStatus", event.getActionStatus());
   struct.put("actionStatusCode", event.getActionStatusCode());
   struct.put("operation", event.getOperation());
   return struct;
 }
 
 //
 // subscriberStreamEventPack
 //

 @Override public Object subscriberStreamEventPack(Object value) { return pack(value); }
 
 /*****************************************
 *
 * unpack
 *
 *****************************************/

 public static VoucherAction unpack(SchemaAndValue schemaAndValue)
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
   Date eventDate = (Date) valueStruct.get("eventDate");
   String voucherCode = valueStruct.getString("voucherCode");
   String actionStatus = valueStruct.getString("actionStatus");
   Integer actionStatusCode = valueStruct.getInt32("actionStatusCode");
   String operation = valueStruct.getString("operation");

   //
   // return
   //

   return new VoucherAction(subscriberID, eventDate, voucherCode, actionStatus, actionStatusCode, operation);
 }

@Override
public String toString()
{
  return "VoucherAction [subscriberID=" + subscriberID + ", eventDate=" + eventDate + ", voucherCode=" + voucherCode + ", actionStatus=" + actionStatus + ", operation=" + operation + "]";
}

}
