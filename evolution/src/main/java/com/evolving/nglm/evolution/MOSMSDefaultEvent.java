/*****************************************************************************
*
*  MOSMSDefaultEvent.java
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
import com.evolving.nglm.evolution.EvolutionEngineEvent;

public class MOSMSDefaultEvent implements EvolutionEngineEvent, MONotificationEvent
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
      schemaBuilder.name("MOSMSDefaultEvent");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
      schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
      schemaBuilder.field("eventDate", Timestamp.SCHEMA);
      schemaBuilder.field("channelName", Schema.STRING_SCHEMA);
      schemaBuilder.field("sourceAddress", Schema.STRING_SCHEMA);
      schemaBuilder.field("destinationAddress", Schema.STRING_SCHEMA);
      schemaBuilder.field("messageText", Schema.STRING_SCHEMA);
      schema = schemaBuilder.build();
    };

  //
  // serde
  //

  private static ConnectSerde<MOSMSDefaultEvent> serde = new ConnectSerde<MOSMSDefaultEvent>(schema, false, MOSMSDefaultEvent.class, MOSMSDefaultEvent::pack, MOSMSDefaultEvent::unpack);

  //
  // accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<MOSMSDefaultEvent> serde() { return serde; }
  public Schema subscriberStreamEventSchema() { return schema(); }
  
  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(MOSMSDefaultEvent.class);

  /****************************************
   *
   * data
   *
   ****************************************/
  private String subscriberID;
  private Date originTimesTamp;
  private String channelName;
  private String sourceAddress;
  private String destinationAddress;
  private String messageText;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  @Override public String getEventName() { return "MOSMS"; }
  @Override public String getSubscriberID() { return subscriberID; }
  @Override public Date getEventDate() { return originTimesTamp; }
  public String getChannelName() { return channelName; };
  public String getSourceAddress() { return sourceAddress; }
  public String getDestinationAddress() { return destinationAddress; }
  public String getMessageText() { return messageText; }
  
  /*****************************************
  *
  *  setters
  *
  *****************************************/
  
  /*****************************************
  *
  * constructor
  *
  *****************************************/
  
  public MOSMSDefaultEvent()
  {
  }
  
  public MOSMSDefaultEvent(String subscriberID, Date originTimesTamp, String channelName, String sourceAddress, String destinationAddress, String messageText)
  {
    super();
    this.subscriberID = subscriberID;
    this.originTimesTamp = originTimesTamp;
    this.channelName = channelName;
    this.sourceAddress = sourceAddress;
    this.destinationAddress = destinationAddress;
    this.messageText = messageText;
  }
  
  /*****************************************
  *
  * pack
  *
  *****************************************/

 public static Object pack(Object value)
 {
   MOSMSDefaultEvent event = (MOSMSDefaultEvent) value;
   Struct struct = new Struct(schema);
   struct.put("subscriberID", event.getSubscriberID());
   struct.put("eventDate", event.getEventDate());
   struct.put("channelName", event.getChannelName());
   struct.put("sourceAddress", event.getSourceAddress());
   struct.put("destinationAddress", event.getDestinationAddress());
   struct.put("messageText", event.getMessageText());
   return struct;
 }
 
 //
 // subscriberStreamEventPack
 //

 @Override public Object subscriberStreamEventPack(Object value) { return pack(value); }
 
 @Override
 public void fillWithMOInfos(String subscriberID, Date originTimesTamp, String channelName, String sourceAddress, String destinationAddress, String messageText)
 {
   this.subscriberID = subscriberID;
   this.originTimesTamp = originTimesTamp;
   this.channelName = channelName;
   this.sourceAddress = sourceAddress;
   this.destinationAddress = destinationAddress;
   this.messageText = messageText;   
 }
 
 /*****************************************
 *
 * unpack
 *
 *****************************************/

 public static MOSMSDefaultEvent unpack(SchemaAndValue schemaAndValue)
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
   Date originTimesTamp = (Date) valueStruct.get("eventDate");
   String channelName = valueStruct.getString("channelName");
   String sourceAddress = valueStruct.getString("sourceAddress");
   String destinationAddress = valueStruct.getString("destinationAddress");
   String messageText = valueStruct.getString("messageText");

   //
   // return
   //

   return new MOSMSDefaultEvent(subscriberID, originTimesTamp, channelName, sourceAddress, destinationAddress, messageText);
 }


}
