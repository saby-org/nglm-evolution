/*****************************************
*
*  FileWithVariableEvent.java
*
*****************************************/

package com.evolving.nglm.evolution;

import java.util.Date;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SubscriberStreamEvent;

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
  //  serde
  //

  private static ConnectSerde<FileWithVariableEvent> serde = new ConnectSerde<FileWithVariableEvent>(schema, false, FileWithVariableEvent.class, FileWithVariableEvent::pack, FileWithVariableEvent::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<FileWithVariableEvent> serde() { return serde; }
  public Schema subscriberStreamEventSchema() { return schema(); }

  /****************************************
  *
  *  data
  *
  *****************************************/

  private String subscriberID;
  private Date eventDate;
  private ParameterMap parameterMap;
  private String fileID;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getSubscriberID() { return subscriberID; }
  public Date getEventDate() { return eventDate; }
  public ParameterMap getParameterMap() { return parameterMap; }
  @Override public DeliveryRequest.DeliveryPriority getDeliveryPriority(){return DeliveryRequest.DeliveryPriority.Standard; }
  public String getFileID() { return fileID; }
  public String getEventName() { return "FileWithVariableEvent"; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public FileWithVariableEvent(String subscriberID, Date eventDate, String fileID, Map<String, Object> params)
  {
    /*****************************************
    *
    *  simple attributes
    *
    *****************************************/

    this.subscriberID = subscriberID;
    this.eventDate = eventDate;
    this.fileID = fileID;

    /*****************************************
    *
    *  parameterMap
    *
    *****************************************/

    this.parameterMap = new ParameterMap();
    for (String key : params.keySet())
      {
        this.parameterMap.put(key, params.get(key));
      }
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public FileWithVariableEvent(String subscriberID, Date eventDate, ParameterMap parameterMap, String fileID)
  {
    this.subscriberID = subscriberID;
    this.eventDate = eventDate;
    this.parameterMap = parameterMap;
    this.fileID = fileID;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    FileWithVariableEvent fileWithVariableEvent = (FileWithVariableEvent) value;
    Struct struct = new Struct(schema);
    struct.put("subscriberID", fileWithVariableEvent.getSubscriberID());
    struct.put("eventDate", fileWithVariableEvent.getEventDate());
    struct.put("parameterMap", ParameterMap.pack(fileWithVariableEvent.getParameterMap()));
    struct.put("fileID", fileWithVariableEvent.getFileID());
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

  public static FileWithVariableEvent unpack(SchemaAndValue schemaAndValue)
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
    String subscriberID = valueStruct.getString("subscriberID");
    Date eventDate = (Date) valueStruct.get("eventDate");
    ParameterMap parameterMap = ParameterMap.unpack(new SchemaAndValue(schema.field("parameterMap").schema(), valueStruct.get("parameterMap")));
    String fileID = valueStruct.getString("fileID");
    
    //
    //  return
    //

    return new FileWithVariableEvent(subscriberID, eventDate, parameterMap, fileID);
  }
}
