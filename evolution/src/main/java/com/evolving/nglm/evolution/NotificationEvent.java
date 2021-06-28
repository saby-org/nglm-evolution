/****************************************************************************
*
*  TokenChange.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.*;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SubscriberStreamOutput;
import com.evolving.nglm.evolution.ActionManager.Action;
import com.evolving.nglm.evolution.ActionManager.ActionType;
import com.evolving.nglm.evolution.DeliveryManagerForNotifications.MessageStatus;
import com.evolving.nglm.evolution.DeliveryRequest.Module;


public class NotificationEvent extends SubscriberStreamOutput implements EvolutionEngineEvent
{
  
  /*****************************************
  *me
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
    schemaBuilder.name("notification_event");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(subscriberStreamOutputSchema().version(),1));
    for (Field field : subscriberStreamOutputSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("subscriberID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("eventDateTime", Timestamp.builder().schema());
    schemaBuilder.field("eventID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("templateID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("tags", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).optional());
    schemaBuilder.field("channelID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("contactType", SchemaBuilder.string().optional().defaultValue("unknown").schema());
    schemaBuilder.field("source", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).optional());
    
    schema = schemaBuilder.build();
  };

  //
  // serde
  //

  private static ConnectSerde<NotificationEvent> serde = new ConnectSerde<NotificationEvent>(schema, false, NotificationEvent.class, NotificationEvent::pack, NotificationEvent::unpack);

  //
  // accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<NotificationEvent> serde() { return serde; }

  /****************************************
  *
  * data
  *
  ****************************************/

  private String subscriberID;
  private Date eventDateTime;
  private String eventID;
  private String templateID;
  private Map<String, String> tags;
  private String channelID;
  private String contactType;
  private Map<String, String> source;
    
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
 
  public String getTemplateID()
  {
    return templateID;
  }

  public Map<String, String> getTags()
  {
    return tags;
  }

  
  public String getChannelID()
  {
    return channelID;
  }
  public String getContactType() { return contactType; }
  public Map<String, String> getSource() { return source; }
  

  //
  //  setters
  //

  public void setSubscriberID(String subscriberID) { this.subscriberID = subscriberID; }
  public void seteventDateTime(Date eventDateTime) { this.eventDateTime = eventDateTime; }
  public void setEventID(String eventID) { this.eventID = eventID; }
  public void setTemplateID(String templateID) { this.templateID = templateID; }
  public void setTags(Map<String, String> tags) { this.tags = tags; }
  public void setChannelID(String channelID) { this.channelID = channelID; }
  public void setContactType(String contactType) { this.contactType = contactType; }
  public void setSource(Map<String, String> source) { this.source = source; }

  
 
  /*****************************************
  *
  * constructor (simple)
  *
  *****************************************/

  public NotificationEvent(String subscriberID, Date eventDateTime, String eventID, String templateID, Map<String, String> tags, String channelID, String contactType, Map<String, String> source)
  {
    this.subscriberID = subscriberID;
    this.eventDateTime = eventDateTime;
    this.eventID = eventID;
    this.templateID = templateID;
    this.tags = tags;
    this.channelID = channelID;
    this.contactType = contactType;
    this.source = source;
  }

  /*****************************************
   *
   * constructor unpack
   *
   *****************************************/
  public NotificationEvent(SchemaAndValue schemaAndValue, String subscriberID, Date eventDateTime, String eventID, String templateID, Map<String, String> tags, String channelID, String contactType, Map<String, String> source)
  {
    super(schemaAndValue);
    this.subscriberID = subscriberID;
    this.eventDateTime = eventDateTime;
    this.eventID = eventID;
    this.templateID = templateID;
    this.tags = tags;
    this.channelID = channelID;
    this.contactType = contactType;
    this.source = source;
  }
  

  /*****************************************
  *
  * pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    NotificationEvent notificationEvent = (NotificationEvent) value;
    Struct struct = new Struct(schema);
    packSubscriberStreamOutput(struct,notificationEvent);
    struct.put("subscriberID",notificationEvent.getSubscriberID());
    struct.put("eventDateTime",notificationEvent.geteventDateTime());
    struct.put("eventID", notificationEvent.getEventID());
    struct.put("templateID", notificationEvent.getTemplateID());
    struct.put("tags", notificationEvent.getTags());
    struct.put("channelID", notificationEvent.getChannelID());
    struct.put("contactType", notificationEvent.getContactType());
    struct.put("source", notificationEvent.getSource());
    return struct;
  }

  /*****************************************
  *
  * unpack
  *
  *****************************************/

  public static NotificationEvent unpack(SchemaAndValue schemaAndValue)
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
    String templateID = valueStruct.getString("templateID");
    Map<String, String> tags = (Map<String, String>) valueStruct.get("tags");
    String channelID = valueStruct.getString("channelID");
    String contactType = valueStruct.getString("contactType");
    Map<String, String> source = (Map<String, String>) valueStruct.get("source");
    
    //
    // validate
    //

    //
    // return
    //

    return new NotificationEvent(schemaAndValue, subscriberID, eventDateTime, eventID,templateID, tags, channelID, contactType, source);
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
    return "Notification Event";
  }
}
