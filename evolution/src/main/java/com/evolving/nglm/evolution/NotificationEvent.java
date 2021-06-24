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
    schemaBuilder.name("notification_event");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(subscriberStreamOutputSchema().version(),1));
    for (Field field : subscriberStreamOutputSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("subscriberID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("eventDateTime", Timestamp.builder().schema());
    schemaBuilder.field("eventID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("destination", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("language", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("templateID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("tags", SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA)).name("notification_tags").schema());
    schemaBuilder.field("restricted", Schema.OPTIONAL_BOOLEAN_SCHEMA);
    schemaBuilder.field("returnCode", Schema.OPTIONAL_INT32_SCHEMA);
    schemaBuilder.field("returnCodeDetails", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("channelID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("notificationParameters", ParameterMap.serde().optionalSchema());
    schemaBuilder.field("contactType", SchemaBuilder.string().defaultValue("unknown").schema());
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
  private String destination;
  private String language;
  private String templateID;
  private Map<String, List<String>> tags;
  private boolean restricted;
  private MessageStatus status;
  private int returnCode;
  private String returnCodeDetails;
  private String channelID;
  private ParameterMap notificationParameters;
  private String contactType;
  
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
  public String getDestination()
  {
    return destination;
  }

  public String getLanguage()
  {
    return language;
  }

  public String getTemplateID()
  {
    return templateID;
  }

  public Map<String, List<String>> getTags()
  {
    return tags;
  }

  public boolean getRestricted()
  {
    return restricted;
  }

  public MessageStatus getMessageStatus()
  {
    return status;
  }

  public int getReturnCode()
  {
    return returnCode;
  }

  public String getReturnCodeDetails()
  {
    return returnCodeDetails;
  }

  public String getChannelID()
  {
    return channelID;
  }
  
  public ParameterMap getNotificationParameters()
  {
    return notificationParameters;
  }
  public String getContactType() { return contactType; }

  //
  //  setters
  //

  public void setSubscriberID(String subscriberID) { this.subscriberID = subscriberID; }
  public void seteventDateTime(Date eventDateTime) { this.eventDateTime = eventDateTime; }
  public void setEventID(String eventID) { this.eventID = eventID; }
  public void setdestination(String destination) { this.destination = destination; }
  public void setLanguage(String language) { this.language = language; }
  public void setTemplateID(String templateID) { this.templateID = templateID; }
  public void setTags(Map<String, List<String>> tags) { this.tags = tags; }
  public void setRestricted(boolean restricted) { this.restricted = restricted; }
  public void setMessageStatus(MessageStatus status) { this.status = status; }
  public void setReturnCode(int returnCode) { this.returnCode = returnCode; }
  public void setReturnCodeDetails(String returnCodeDetails) { this.returnCodeDetails = returnCodeDetails; }
  public void setChannelID(String channelID) { this.channelID = channelID; }
  public void setNotificationParameters(ParameterMap notificationParameters) { this.notificationParameters = notificationParameters; }
  public void setContactType(String contactType) { this.contactType = contactType; }

  /*****************************************
  *
  * constructor (simple)
  *
  *****************************************/

  public NotificationEvent(String subscriberID, Date eventDateTime, String eventID, String destination, String language, String templateID, Map<String, List<String>> tags, boolean restricted, MessageStatus status, String returnCodeDetails, String channelID, ParameterMap notificationParameters, String contactType)
  {
    this(subscriberID, eventDateTime, eventID, destination, language, templateID, tags, channelID, notificationParameters, contactType);
  }

  /*****************************************
  *
  * constructor (simple)
  *
  *****************************************/

  public NotificationEvent(String subscriberID, Date eventDateTime, String eventID, String destination, String language, String templateID, Map<String, List<String>> tags, String channelID, ParameterMap notificationParameters, String contactType)
  {
    this.subscriberID = subscriberID;
    this.eventDateTime = eventDateTime;
    this.eventID = eventID;
    this.destination = destination;
    this.language = language;
    this.templateID = templateID;
    this.tags = tags;
    this.status = MessageStatus.PENDING;
    this.returnCode = status.getReturnCode();
    this.returnCodeDetails = null;
    this.channelID = channelID;
    this.notificationParameters = notificationParameters;
    this.contactType = contactType;
  }

  /*****************************************
   *
   * constructor unpack
   *
   *****************************************/
  public NotificationEvent(SchemaAndValue schemaAndValue, String subscriberID, Date eventDateTime, String eventID, String destination, String language, String templateID, Map<String, List<String>> tags, boolean restricted, MessageStatus status, String returnCodeDetails, String channelID, ParameterMap notificationParameters, String contactType)
  {
    super(schemaAndValue);
    this.subscriberID = subscriberID;
    this.eventDateTime = eventDateTime;
    this.eventID = eventID;
    this.destination = destination;
    this.language = language;
    this.templateID = templateID;
    this.tags = tags;
    this.status = status;
    this.returnCode = status.getReturnCode();
    this.returnCodeDetails = null;
    this.channelID = channelID;
    this.notificationParameters = notificationParameters;
    this.contactType = contactType;
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
    struct.put("destination", notificationEvent.getDestination());
    struct.put("language", notificationEvent.getLanguage());
    struct.put("templateID", notificationEvent.getTemplateID());
    struct.put("tags", notificationEvent.getTags());
    struct.put("restricted", notificationEvent.getRestricted());
    struct.put("returnCode", notificationEvent.getReturnCode());
    struct.put("returnCodeDetails", notificationEvent.getReturnCodeDetails());
    struct.put("channelID", notificationEvent.getChannelID());
    struct.put("notificationParameters", ParameterMap.serde().packOptional(notificationEvent.getNotificationParameters()));
    struct.put("contactType", notificationEvent.getContactType());
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
    String destination = valueStruct.getString("destination");
    String language = valueStruct.getString("language");
    String templateID = valueStruct.getString("templateID");
    Map<String, List<String>> tags = (Map<String, List<String>>) valueStruct.get("tags");
    boolean restricted = valueStruct.getBoolean("restricted");
    Integer returnCode = valueStruct.getInt32("returnCode");
    String returnCodeDetails = valueStruct.getString("returnCodeDetails");
    String channelID = valueStruct.getString("channelID");
    ParameterMap notificationParameters =ParameterMap.unpack(new SchemaAndValue(schema.field("notificationParameters").schema(), valueStruct.get("notificationParameters")));     
    MessageStatus status = MessageStatus.fromReturnCode(returnCode);
    String contactType = valueStruct.getString("contactType"); 
    
    //
    // validate
    //

    //
    // return
    //

    return new NotificationEvent(schemaAndValue, subscriberID, eventDateTime, eventID, destination, language, templateID, tags, restricted, status, returnCodeDetails, channelID, notificationParameters, contactType);
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
    return "token change";
  }
}
