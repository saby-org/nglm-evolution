package com.evolving.nglm.evolution;

import java.util.Date;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SubscriberStreamOutput;
import com.evolving.nglm.evolution.Badge.BadgeAction;

public class BadgeChange extends SubscriberStreamOutput implements EvolutionEngineEvent
{
  
  private static Schema schema = null;
  static
    {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct();
      schemaBuilder.name("badge_change");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(subscriberStreamOutputSchema().version(), 1));
      for (Field field : subscriberStreamOutputSchema().fields())
        {
          schemaBuilder.field(field.name(), field.schema());
        }
      schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
      schemaBuilder.field("eventDate", Timestamp.builder().schema());
      schemaBuilder.field("eventID", Schema.STRING_SCHEMA);
      schemaBuilder.field("action", Schema.STRING_SCHEMA);
      schemaBuilder.field("badgeID", Schema.STRING_SCHEMA);
      schemaBuilder.field("moduleID", Schema.STRING_SCHEMA);
      schemaBuilder.field("featureID", Schema.STRING_SCHEMA);
      schemaBuilder.field("origin", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("returnStatus", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("tenantID", Schema.INT16_SCHEMA);
      schemaBuilder.field("infos", ParameterMap.schema());
      schemaBuilder.field("responseEvent", Schema.BOOLEAN_SCHEMA);
      schema = schemaBuilder.build();
    }
  
  private static ConnectSerde<BadgeChange> serde = new ConnectSerde<BadgeChange>(schema, false, BadgeChange.class, BadgeChange::pack, BadgeChange::unpack);
  public static Schema schema() { return schema; }
  public static ConnectSerde<BadgeChange> serde() { return serde; }
  
  private String subscriberID;
  private Date eventDate;
  private String eventID;
  private BadgeAction action;
  private String badgeID;
  private String moduleID;
  private String featureID;
  private String origin;
  private RESTAPIGenericReturnCodes returnStatus;
  private int tenantID;
  private ParameterMap infos;
  private boolean responseEvent;
  
  @Override public String getSubscriberID()
  {
    return subscriberID;
  }

  @Override public Date getEventDate()
  {
    return eventDate;
  }
  
  @Override public String getEventName()
  {
    if (responseEvent)
      {
        return "Badge Change Result";
      }
    else
      {
        return "Badge Change Request";
      }
  }
  
  public String getEventID()
  {
    return eventID;
  }
  public BadgeAction getAction()
  {
    return action;
  }
  public String getBadgeID()
  {
    return badgeID;
  }
  public String getModuleID()
  {
    return moduleID;
  }
  public String getFeatureID()
  {
    return featureID;
  }
  public String getOrigin()
  {
    return origin;
  }
  public RESTAPIGenericReturnCodes getReturnStatus()
  {
    return returnStatus;
  }
  public void setReturnStatus(RESTAPIGenericReturnCodes returnStatus)
  {
    this.returnStatus = returnStatus;
  }
  public int getTenantID()
  {
    return tenantID;
  }
  public ParameterMap getInfos() { return infos; }
  public boolean IsResponseEvent() { return responseEvent; }
  public void changeToBadgeChangeResponse() { this.responseEvent = true; }
  
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
  
  public static Object pack(Object value)
  {
    BadgeChange badgeChange = (BadgeChange) value;
    Struct struct = new Struct(schema);
    packSubscriberStreamOutput(struct, badgeChange);
    struct.put("subscriberID", badgeChange.getSubscriberID());
    struct.put("eventDate", badgeChange.getEventDate());
    struct.put("eventID", badgeChange.getEventID());
    struct.put("action", badgeChange.getAction().getExternalRepresentation());
    struct.put("badgeID", badgeChange.getBadgeID());
    struct.put("moduleID", badgeChange.getModuleID());
    struct.put("featureID", badgeChange.getFeatureID());
    struct.put("origin", badgeChange.getOrigin());
    struct.put("returnStatus", badgeChange.getReturnStatus().getGenericResponseMessage());
    struct.put("tenantID", (short) badgeChange.getTenantID());
    struct.put("infos", ParameterMap.pack(badgeChange.getInfos()));
    struct.put("responseEvent", badgeChange.IsResponseEvent());
    return struct;
  }
  
  public static BadgeChange unpack(SchemaAndValue schemaAndValue)
  {
    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion1(schema.version()) : null;
    Struct valueStruct = (Struct) value;
    String subscriberID = valueStruct.getString("subscriberID");
    Date eventDateTime = (Date) valueStruct.get("eventDate");
    String eventID = valueStruct.getString("eventID");
    BadgeAction action = BadgeAction.fromExternalRepresentation(valueStruct.getString("action"));
    String badgeID = valueStruct.getString("badgeID");
    String moduleID = valueStruct.getString("moduleID");
    String featureID = valueStruct.getString("featureID");
    String origin = valueStruct.getString("origin");
    RESTAPIGenericReturnCodes returnStatus = RESTAPIGenericReturnCodes.fromGenericResponseMessage(valueStruct.getString("returnStatus"));
    int tenantID = valueStruct.getInt16("tenantID");
    ParameterMap infos = ParameterMap.unpack(new SchemaAndValue(schema.field("infos").schema(), valueStruct.get("infos")));
    boolean responseEvent = valueStruct.getBoolean("responseEvent");
    
    return new BadgeChange(schemaAndValue, subscriberID, eventDateTime, eventID, action, badgeID, moduleID, featureID, origin, returnStatus, tenantID, infos, responseEvent);
  }
  
  public BadgeChange(SchemaAndValue schemaAndValue, String subscriberID, Date eventDate, String eventID, BadgeAction action, String badgeID, String moduleID, String featureID, String origin, RESTAPIGenericReturnCodes returnStatus, int tenantID, ParameterMap infos, boolean responseEvent)
  {
    super(schemaAndValue);
    this.subscriberID = subscriberID;
    this.eventDate = eventDate;
    this.eventID = eventID;
    this.action = action;
    this.badgeID = badgeID;
    this.moduleID = moduleID;
    this.featureID = featureID;
    this.origin = origin;
    this.returnStatus = returnStatus;
    this.tenantID = tenantID;
    this.infos = infos;
    this.responseEvent =responseEvent;
  }
  
  public BadgeChange(BadgeChange badgeChange)
  {
    this.subscriberID = badgeChange.getSubscriberID();
    this.eventDate = badgeChange.getEventDate();
    this.eventID = badgeChange.getEventID();
    this.action = badgeChange.getAction();
    this.badgeID = badgeChange.getBadgeID();
    this.moduleID = badgeChange.getModuleID();
    this.featureID = badgeChange.getFeatureID();
    this.origin = badgeChange.getOrigin();
    this.returnStatus = badgeChange.getReturnStatus();
    this.tenantID = badgeChange.getTenantID();
    this.infos = badgeChange.getInfos();
    this.responseEvent = badgeChange.IsResponseEvent();
  }
  
  public BadgeChange(String subscriberID, Date eventDate, String eventID, BadgeAction action, String badgeID, String moduleID, String featureID, String origin, RESTAPIGenericReturnCodes returnStatus, int tenantID, ParameterMap infos)
  {
    super();
    this.subscriberID = subscriberID;
    this.eventDate = eventDate;
    this.eventID = eventID;
    this.action = action;
    this.badgeID = badgeID;
    this.moduleID = moduleID;
    this.featureID = featureID;
    this.origin = origin;
    this.returnStatus = returnStatus;
    this.tenantID = tenantID;
    this.infos = infos;
  }
  public BadgeChange copy()
  {
    return new BadgeChange(this);
  }
}
