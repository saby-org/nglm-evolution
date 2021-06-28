/*****************************************************************************
*
*  AssignSubscriberIDs.java
*
*****************************************************************************/

package com.evolving.nglm.core;

import com.evolving.nglm.evolution.DeliveryRequest;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class AssignSubscriberIDs implements com.evolving.nglm.core.SubscriberStreamEvent
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
    schemaBuilder.name("assignsubscriberids");
    schemaBuilder.version(com.evolving.nglm.core.SchemaUtilities.packSchemaVersion(3));
    schemaBuilder.field("subscriberID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("eventDate", Timestamp.SCHEMA);
    schemaBuilder.field("subscriberAction", SchemaBuilder.string().defaultValue("standard").schema());
    schemaBuilder.field("alternateIDs", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA).name("assignsubscriberids_alternateids").schema());
    schemaBuilder.field("tenantID", Schema.INT16_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<AssignSubscriberIDs> serde = new ConnectSerde<AssignSubscriberIDs>(schema, false, AssignSubscriberIDs.class, AssignSubscriberIDs::pack, AssignSubscriberIDs::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<AssignSubscriberIDs> serde() { return serde; }
  public Schema subscriberStreamEventSchema() { return schema(); }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private String subscriberID;
  private Date eventDate;
  private SubscriberAction subscriberAction;
  private Map<String,String> alternateIDs;
  private int tenantID;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public String getSubscriberID() { return subscriberID; }
  public Date getEventDate() { return eventDate; }
  public SubscriberAction getSubscriberAction() { return subscriberAction; }
  public Map<String,String> getAlternateIDs() { return alternateIDs; }
  public int getTenantID() { return tenantID; }

  @Override public DeliveryRequest.DeliveryPriority getDeliveryPriority(){return DeliveryRequest.DeliveryPriority.High; }

  /****************************************
  *
  *  setters
  *
  ****************************************/

  public void setSubscriberID(String subscriberID) { this.subscriberID = subscriberID; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public AssignSubscriberIDs(String subscriberID, Date eventDate, SubscriberAction subscriberAction, Map<String,String> alternateIDs, int tenantID)
  {
    /*****************************************
    *
    *  data
    *
    *****************************************/

    this.subscriberID = subscriberID;
    this.eventDate = eventDate;
    this.tenantID = tenantID;

    /*****************************************
    *
    *  subscriberAction
    *
    *****************************************/

    switch (subscriberAction)
      {
        case Delete:
        case DeleteImmediate:
          this.subscriberAction = subscriberAction;
          this.alternateIDs = new HashMap<String,String>();
          for (String configuredAlternateID : Deployment.getAlternateIDs().keySet())
            {
              this.alternateIDs.put(configuredAlternateID, null);
            }
          break;

        default:
          this.subscriberAction = subscriberAction;
          this.alternateIDs = alternateIDs;
          break;
      }
  }

  //
  //  default constructor (assign)
  //

  public AssignSubscriberIDs(String subscriberID, Date eventDate, Map<String,String> alternateIDs, int tenantID)
  {
    this(subscriberID, eventDate, SubscriberAction.Standard, alternateIDs, tenantID);
  }

  /*****************************************
  *
  *  constructor (copy)
  *
  *****************************************/

  public AssignSubscriberIDs(AssignSubscriberIDs assignSubscriberIDs)
  {
    this.subscriberID = assignSubscriberIDs.getSubscriberID();
    this.eventDate = assignSubscriberIDs.getEventDate();
    this.subscriberAction = assignSubscriberIDs.getSubscriberAction();
    this.alternateIDs = new HashMap<String,String>(assignSubscriberIDs.getAlternateIDs());
    this.tenantID = assignSubscriberIDs.getTenantID();
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    AssignSubscriberIDs assignSubscriberIDs = (AssignSubscriberIDs) value;
    Struct struct = new Struct(schema);
    struct.put("subscriberID", assignSubscriberIDs.getSubscriberID());
    struct.put("eventDate", assignSubscriberIDs.getEventDate());
    struct.put("subscriberAction", assignSubscriberIDs.getSubscriberAction().getExternalRepresentation());
    struct.put("alternateIDs", assignSubscriberIDs.getAlternateIDs());
    struct.put("tenantID", (short)assignSubscriberIDs.getTenantID());
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

  public static AssignSubscriberIDs unpack(SchemaAndValue schemaAndValue)
  {
    //
    //  data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? com.evolving.nglm.core.SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;

    //
    //  unpack
    //

    Struct valueStruct = (Struct) value;
    String subscriberID = valueStruct.getString("subscriberID");
    Date eventDate = (Date) valueStruct.get("eventDate");
    SubscriberAction subscriberAction = (schemaVersion >= 2) ? SubscriberAction.fromExternalRepresentation(valueStruct.getString("subscriberAction")) : SubscriberAction.Standard;
    Map<String,String> alternateIDs = (Map<String,String>) valueStruct.get("alternateIDs");
    int tenantID = schema.field("tenantID") != null ? valueStruct.getInt16("tenantID") : 1; // by default tenantID = 1
    
    //
    //  return
    //

    return new AssignSubscriberIDs(subscriberID, eventDate, subscriberAction, alternateIDs, tenantID);
  }
}
