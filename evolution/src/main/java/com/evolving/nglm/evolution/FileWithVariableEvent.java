/*****************************************************************************
*
*  FileWithVariableEvent.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Date;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;

public class FileWithVariableEvent implements EvolutionEngineEvent
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
    schemaBuilder.name("file_with_variable_event");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("eventDate", Timestamp.SCHEMA);
    schemaBuilder.field("fileID", Schema.STRING_SCHEMA);
    schemaBuilder.field("parameterMap", ParameterMap.schema());
    schema = schemaBuilder.build();
  };
  
  //
  // serde
  //

  private static ConnectSerde<FileWithVariableEvent> serde = new ConnectSerde<FileWithVariableEvent>(schema, false, FileWithVariableEvent.class, FileWithVariableEvent::pack, FileWithVariableEvent::unpack);

  //
  // accessor
  //

  public static Schema schema()
  {
    return schema;
  }

  public static ConnectSerde<FileWithVariableEvent> serde()
  {
    return serde;
  }

  public Schema subscriberStreamEventSchema() { return schema(); }

  //
  // logger
  //

  private static final Logger log = LoggerFactory.getLogger(FileWithVariableEvent.class);
  
  /****************************************
  *
  * data
  *
  ****************************************/
 
 private String subscriberID;
 private Date eventDate;
 private String fileID;
 private ParameterMap parameterMap;
 
 /*****************************************
 *
 *  accessors
 *
 *****************************************/

  //TODO: this should probably extends SubscriberStreamOutput instead
  @Override public DeliveryRequest.DeliveryPriority getDeliveryPriority(){return DeliveryRequest.DeliveryPriority.Low; }
  @Override public String getEventName() { return "FileWithVariableEvent"; }
  @Override public String getSubscriberID() { return subscriberID; }
  @Override public Date getEventDate() { return eventDate; }
  public String getFileID() { return fileID; }
  public ParameterMap getParameterMap() { return parameterMap; }
  
  /*****************************************
  *
  * constructor
  *
  *****************************************/
  
  public FileWithVariableEvent(String subscriberID, Date eventDate, String fileID, Map<String, Object> params)
  {
    this.subscriberID = subscriberID;
    this.eventDate = eventDate;
    this.fileID = fileID;
    this.parameterMap = new ParameterMap();
    for (String key : params.keySet())
      {
        this.parameterMap.put(key, params.get(key));
      }
  }
  
  /*****************************************
  *
  * pack
  *
  *****************************************/

 public static Object pack(Object value)
 {
   FileWithVariableEvent event = (FileWithVariableEvent) value;
   Struct struct = new Struct(schema);
   struct.put("subscriberID", event.getSubscriberID());
   struct.put("eventDate", event.getEventDate());
   struct.put("fileID", event.getFileID());
   struct.put("parameterMap", ParameterMap.pack(event.getParameterMap()));
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

 public static FileWithVariableEvent unpack(SchemaAndValue schemaAndValue)
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
   String fileID = valueStruct.getString("fileID");
   ParameterMap parameterMap = ParameterMap.unpack(new SchemaAndValue(schema.field("parameterMap").schema(), valueStruct.get("parameterMap")));

   //
   // return
   //

   return new FileWithVariableEvent(subscriberID, eventDate, fileID, parameterMap);
 }
 
 @Override
 public String toString()
 {
   return "FileWithVariableEvent [subscriberID=" + subscriberID + ", eventDate=" + eventDate + ", fileID=" + fileID + ", parameterMap=" + parameterMap + "]";
 }

}
