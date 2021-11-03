/*****************************************************************************
*
*  VoucherAction.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.SubscriberStreamOutput;
import org.apache.kafka.connect.data.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.ActionManager.Action;
import com.evolving.nglm.evolution.ActionManager.ActionType;
import com.evolving.nglm.evolution.EvolutionEngine.VoucherActionManager.Operation;

public class VoucherAction extends SubscriberStreamOutput implements EvolutionEngineEvent, Action, VoucherRedemption, VoucherValidation
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
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(subscriberStreamOutputSchema().version(),2));
      for (Field field : subscriberStreamOutputSchema().fields()) schemaBuilder.field(field.name(), field.schema());
      schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
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
  
  public VoucherAction(String subscriberID, String voucherCode, String actionStatus, Integer actionStatusCode, String operation)
  {
    this.subscriberID = subscriberID;
    this.voucherCode = voucherCode;
    this.actionStatus = actionStatus;
    this.actionStatusCode = actionStatusCode;
    this.operation = operation;
  }

  public VoucherAction(SchemaAndValue schemaAndValue, String subscriberID, String voucherCode, String actionStatus, Integer actionStatusCode, String operation)
  {
    super(schemaAndValue);
    this.subscriberID = subscriberID;
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
   packSubscriberStreamOutput(struct,event);
   struct.put("subscriberID", event.getSubscriberID());
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
   Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion1(schema.version()) : null;

   //
   // unpack
   //

   Struct valueStruct = (Struct) value;
   String subscriberID = valueStruct.getString("subscriberID");
   String voucherCode = valueStruct.getString("voucherCode");
   String actionStatus = valueStruct.getString("actionStatus");
   Integer actionStatusCode = valueStruct.getInt32("actionStatusCode");
   String operation = valueStruct.getString("operation");

   //
   // return
   //

   return new VoucherAction(schemaAndValue, subscriberID, voucherCode, actionStatus, actionStatusCode, operation);
 }

@Override
public String toString()
{
  return "VoucherAction [subscriberID=" + subscriberID + ", voucherCode=" + voucherCode + ", actionStatus=" + actionStatus + ", operation=" + operation + "]";
}

}
