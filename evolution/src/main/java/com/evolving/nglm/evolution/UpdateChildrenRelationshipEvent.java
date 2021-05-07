/*****************************************************************************
*
*  UpdateChildrenRelationshipEvent.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Date;

import org.apache.kafka.connect.data.*;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.DeliveryRequest.DeliveryPriority;

public class UpdateChildrenRelationshipEvent implements EvolutionEngineEvent
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
    schemaBuilder.name("updateChildrenRelationshipEvent");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("eventDate", Timestamp.SCHEMA);
    schemaBuilder.field("relationshipDisplay", Schema.STRING_SCHEMA);
    schemaBuilder.field("children", Schema.STRING_SCHEMA);
    schemaBuilder.field("isDeletion", Schema.BOOLEAN_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  // serde
  //

  private static ConnectSerde<UpdateChildrenRelationshipEvent> serde = new ConnectSerde<UpdateChildrenRelationshipEvent>(schema, false, UpdateChildrenRelationshipEvent.class, UpdateChildrenRelationshipEvent::pack, UpdateChildrenRelationshipEvent::unpack);

 
  //
  // accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<UpdateChildrenRelationshipEvent> serde() { return serde; }
  public Schema subscriberStreamEventSchema() { return schema(); }

  /****************************************
  *
  * data
  *
  ****************************************/

  private Date eventDate;
  private String subscriberID;
  private String relationshipDisplay;
  private String children;
  private boolean isDeletion;
  
  /****************************************
  *
  * accessors
  *
  ****************************************/

  public String getEventName() { return "updateChildrenRelationshipEvent"; }
  public String getSubscriberID() { return subscriberID; }
  public Date getEventDate() { return eventDate; }
  public String getRelationshipDisplay() { return relationshipDisplay; }
  public String getChildren() { return children; }
  public boolean isDeletion() { return isDeletion; }

  /*****************************************
  *
  * constructor
  *
  *****************************************/

  public UpdateChildrenRelationshipEvent(String subscriberID, Date eventDate, String relationshipDisplay, String children, boolean isDeletion)
  {
    this.subscriberID = subscriberID;
    this.eventDate = eventDate;
    this.relationshipDisplay = relationshipDisplay;
    this.children = children;
    this.isDeletion = isDeletion;
  }

  /*****************************************
  *
  * pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    UpdateChildrenRelationshipEvent updateChildrenRelationshipEvent = (UpdateChildrenRelationshipEvent) value;
    Struct struct = new Struct(schema);
    struct.put("subscriberID", updateChildrenRelationshipEvent.getSubscriberID());
    struct.put("eventDate", updateChildrenRelationshipEvent.getEventDate());
    struct.put("relationshipDisplay", updateChildrenRelationshipEvent.getRelationshipDisplay());
    struct.put("children", updateChildrenRelationshipEvent.getChildren());
    struct.put("isDeletion", updateChildrenRelationshipEvent.isDeletion());
    return struct;
  }

  //
  // subscriberStreamEventPack
  //

  public Object subscriberStreamEventPack(Object value) { return pack(value); }

  /*****************************************
  *
  * unpack
  *
  *****************************************/

  public static UpdateChildrenRelationshipEvent unpack(SchemaAndValue schemaAndValue)
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
    Date eventDate = (Date) valueStruct.get("eventDate");
    String relationshipDisplay = valueStruct.getString("relationshipDisplay");;
    String children = valueStruct.getString("children");
    boolean isDeletion = valueStruct.getBoolean("isDeletion");
    
    //
    // return
    //

    return new UpdateChildrenRelationshipEvent(subscriberID, eventDate, relationshipDisplay, children, isDeletion);
  }
  @Override
  public DeliveryPriority getDeliveryPriority()
  {
    return DeliveryRequest.DeliveryPriority.Standard;
  }
}
