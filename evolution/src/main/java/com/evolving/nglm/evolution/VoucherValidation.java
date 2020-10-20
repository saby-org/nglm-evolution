/*****************************************************************************
*
*  VoucherValidation.java
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

public class VoucherValidation implements EvolutionEngineEvent, Action
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
      schemaBuilder.name("VoucherValidation");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
      schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
      schemaBuilder.field("eventDate", Timestamp.SCHEMA);
      schemaBuilder.field("voucherCode", Schema.STRING_SCHEMA);
      schemaBuilder.field("validationStatus", Schema.STRING_SCHEMA);
      schema = schemaBuilder.build();
    };

  //
  // serde
  //

  private static ConnectSerde<VoucherValidation> serde = new ConnectSerde<VoucherValidation>(schema, false, VoucherValidation.class, VoucherValidation::pack, VoucherValidation::unpack);

  //
  // accessor
  //

  public static Schema schema()
  {
    return schema;
  }

  public static ConnectSerde<VoucherValidation> serde()
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

  private static final Logger log = LoggerFactory.getLogger(VoucherValidation.class);

  /****************************************
   *
   * data
   *
   ****************************************/
  
  private String subscriberID;
  private Date eventDate;
  private String voucherCode;
  private String validationStatus;
  
  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  @Override public String getEventName() { return "VoucherValidation"; }
  @Override public String getSubscriberID() { return subscriberID; }
  @Override public Date getEventDate() { return eventDate; }
  public String getVoucherCode() { return voucherCode; };
  public String getValidationStatus() { return validationStatus; }
  @Override public ActionType getActionType() { return ActionType.VoucherChange; }
  
  /*****************************************
  *
  *  setters
  *
  *****************************************/
  
  public void setValidationStatus(String validationStatus) { this.validationStatus = validationStatus; }
  
  /*****************************************
  *
  * constructor
  *
  *****************************************/
  
  public VoucherValidation(String subscriberID, Date eventDate, String voucherCode, String validationStatus)
  {
    super();
    this.subscriberID = subscriberID;
    this.eventDate = eventDate;
    this.voucherCode = voucherCode;
    this.validationStatus = validationStatus;
  }
  
  /*****************************************
  *
  * pack
  *
  *****************************************/

 public static Object pack(Object value)
 {
   VoucherValidation event = (VoucherValidation) value;
   Struct struct = new Struct(schema);
   struct.put("subscriberID", event.getSubscriberID());
   struct.put("eventDate", event.getEventDate());
   struct.put("voucherCode", event.getValidationStatus());
   struct.put("validationStatus", event.getValidationStatus());
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

 public static VoucherValidation unpack(SchemaAndValue schemaAndValue)
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
   String validationStatus = valueStruct.getString("validationStatus");

   //
   // return
   //

   return new VoucherValidation(subscriberID, eventDate, voucherCode, validationStatus);
 }

}
