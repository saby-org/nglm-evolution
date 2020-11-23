/*****************************************************************************
*
*  ContactPolicyCommunicationChannels.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.DeliveryRequest.DeliveryPriority;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ContactPolicyCommunicationChannels
{
  /*****************************************
  *
  *  enum
  *
  *****************************************/

  public enum ContactType
  {
    CallToAction("callToAction", "Call to Action (Low priority - Limited)", DeliveryPriority.Standard, true),
    Response("response", "Response (High priority)", DeliveryPriority.High, false),
    Reminder("reminder", "Reminder (Low priority - Limited)", DeliveryPriority.Standard, true),
    Announcement("announcement", "Announcement (Low priority - Limited)", DeliveryPriority.Standard, false),
    ActionNotification("actionNotification", "Action Notification (High priority)", DeliveryPriority.High, false),
    Unknown("(unknown)", "Unknown (Low priority)", DeliveryPriority.Standard, false);
    private String externalRepresentation;
    private String display;
    private DeliveryPriority deliveryPriority;
    private boolean restricted;
    private ContactType(String externalRepresentation, String display, DeliveryPriority deliveryPriority, boolean restricted) { this.externalRepresentation = externalRepresentation; this.display = display; this.deliveryPriority = deliveryPriority; this.restricted = restricted; }
    public String getExternalRepresentation() { return externalRepresentation; }
    public String getDisplay() { return display; }
    public DeliveryPriority getDeliveryPriority() { return deliveryPriority; }
    public boolean getRestricted() { return restricted; }
    public static ContactType fromExternalRepresentation(String externalRepresentation) { for (ContactType enumeratedValue : ContactType.values()) { if (enumeratedValue.getExternalRepresentation().equalsIgnoreCase(externalRepresentation)) return enumeratedValue; } return Unknown; }
  }
  
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
    schemaBuilder.name("contactPolicyCommunicationChannels");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("communicationChannelID", Schema.STRING_SCHEMA);
    schemaBuilder.field("communicationChannelName", Schema.STRING_SCHEMA);
    schemaBuilder.field("contactTypes", SchemaBuilder.array(Schema.STRING_SCHEMA).schema());
    schemaBuilder.field("messageLimits", SchemaBuilder.array(MessageLimits.schema()).schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<ContactPolicyCommunicationChannels> serde = new ConnectSerde<ContactPolicyCommunicationChannels>(schema, false, ContactPolicyCommunicationChannels.class, ContactPolicyCommunicationChannels::pack, ContactPolicyCommunicationChannels::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<ContactPolicyCommunicationChannels> serde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private String communicationChannelID;
  private String communicationChannelName;
  private Set<ContactType> contactTypes;
  private Set<MessageLimits> messageLimits;
  

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getCommunicationChannelID() { return communicationChannelID; }
  public String getCommunicationChannelName() { return communicationChannelName; }
  public Set<ContactType> getContactTypes() { return contactTypes; }
  public Set<MessageLimits> getMessageLimits() { return messageLimits; }
  
  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public ContactPolicyCommunicationChannels(String communicationChannelID, String communicationChannelName, Set<ContactType> contactTypes, Set<MessageLimits> messageLimits)
  {
    this.communicationChannelID = communicationChannelID;
    this.communicationChannelName = communicationChannelName;
    this.contactTypes = contactTypes;
    this.messageLimits = messageLimits;
  }
  
  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    ContactPolicyCommunicationChannels contactPolicyCommunicationChannel = (ContactPolicyCommunicationChannels) value;
    Struct struct = new Struct(schema);
    struct.put("communicationChannelID", contactPolicyCommunicationChannel.getCommunicationChannelID());
    struct.put("communicationChannelName", contactPolicyCommunicationChannel.getCommunicationChannelName());
    struct.put("contactTypes", packContactTypes(contactPolicyCommunicationChannel.getContactTypes()));
    struct.put("messageLimits", packMessageLimits(contactPolicyCommunicationChannel.getMessageLimits()));
    return struct;
  }
  
  /*****************************************
  *
  *  packContactTypes
  *
  *****************************************/

  private static List<Object> packContactTypes(Set<ContactType> contactTypes)
  {
    List<Object> result = new ArrayList<Object>();
    for (ContactType contactType : contactTypes)
      {
        result.add(contactType.getExternalRepresentation());
      }
    return result;
  }
  
  /****************************************
  *
  *  packMessageLimits
  *
  ****************************************/

  private static List<Object> packMessageLimits(Set<MessageLimits> messageLimits)
  {
    List<Object> result = new ArrayList<Object>();
    for (MessageLimits limit : messageLimits)
      {
        result.add(MessageLimits.pack(limit));
      }
    return result;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static ContactPolicyCommunicationChannels unpack(SchemaAndValue schemaAndValue)
  {
    //
    //  data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion1(schema.version()) : null;

    //
    //  unpack
    //

    Struct valueStruct = (Struct) value;
    String communicationChannelID = valueStruct.getString("communicationChannelID");
    String communicationChannelName = valueStruct.getString("communicationChannelName");
    Set<ContactType> contactTypes = unpackContactTypes(schema.field("contactTypes").schema(), valueStruct.get("contactTypes"));
    Set<MessageLimits> mesageLimits = unpackMessageLimits(schema.field("messageLimits").schema(), valueStruct.get("messageLimits"));
    
    //
    //  return
    //

    return new ContactPolicyCommunicationChannels(communicationChannelID, communicationChannelName, contactTypes, mesageLimits);
  }
  
  /*****************************************
  *
  *  unpackContactTypes
  *
  *****************************************/

  private static Set<ContactType> unpackContactTypes(Schema schema, Object value)
  {
    Set<ContactType> result = new HashSet<ContactType>();
    List<String> valueArray = (List<String>) value;
    for (String contactType : valueArray)
      {
        result.add(ContactType.fromExternalRepresentation(contactType));
      }

    //
    //  return
    //

    return result;
  }
  
  /*****************************************
  *
  *  unpackMessageLimits
  *
  *****************************************/

  private static Set<MessageLimits> unpackMessageLimits(Schema schema, Object value)
  {
    //
    //  get schema for MessageLimits
    //

    Schema messageLimitSchema = schema.valueSchema();

    //
    //  unpack
    //

    Set<MessageLimits> result = new HashSet<MessageLimits>();
    List<Object> valueArray = (List<Object>) value;
    for (Object limit : valueArray)
      {
        result.add(MessageLimits.unpack(new SchemaAndValue(messageLimitSchema, limit)));
      }

    //
    //  return
    //

    return result;
  }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public ContactPolicyCommunicationChannels(JSONObject jsonRoot) throws GUIManagerException
  {
    this.communicationChannelID = JSONUtilities.decodeString(jsonRoot, "communicationChannelID", true);
    this.communicationChannelName = JSONUtilities.decodeString(jsonRoot, "communicationChannelName", true);
    this.contactTypes = decodeContactTypes(JSONUtilities.decodeJSONArray(jsonRoot, "contactTypes", true));
    this.messageLimits = decodeMessageLimits(JSONUtilities.decodeJSONArray(jsonRoot, "messageLimits", false));
  }
  
  /*****************************************
  *
  *  decodeContactTypes
  *
  *****************************************/

  private Set<ContactType> decodeContactTypes(JSONArray jsonArray) throws GUIManagerException
  {
    Set<ContactType> contactTypes = new HashSet<ContactType>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        contactTypes.add(ContactType.fromExternalRepresentation((String) jsonArray.get(i)));
      }
    return contactTypes;
  }
  
  /*****************************************
  *
  *  decodeMessageLimits
  *
  *****************************************/

  private Set<MessageLimits> decodeMessageLimits(JSONArray jsonArray) throws GUIManagerException
  {
    Set<MessageLimits> result = new HashSet<MessageLimits>();
    if (jsonArray != null)
      {
        for (int i=0; i<jsonArray.size(); i++)
          {
            result.add(new MessageLimits((JSONObject) jsonArray.get(i)));
          }
      }
    return result;
  }

  /*****************************************
  *
  *  validate
  *
  *****************************************/

  public void validate(Date date, String communicationChannelID) throws GUIManagerException
  {
    /*****************************************
    *
    *  validate communicationChannel exist
    *
    *****************************************/
//    CommunicationChannel communicationChannel = Deployment.getCommunicationChannels().get(communicationChannelID);
//    if (communicationChannel == null) throw new GUIManagerException("unknown communicationChannel ", communicationChannelID);
  }

  /*****************************************
  *
  *  schedule
  *
  *****************************************/

  public Date schedule(String communicationChannelID, Date now)
  {
    return now;
  }
}
