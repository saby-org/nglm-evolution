/****************************************************************************
*
*  RecordAlternateID.java 
*
****************************************************************************/

package com.evolving.nglm.core;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RecordAlternateID
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
    schemaBuilder.name("record_alternateid");
    schemaBuilder.version(com.evolving.nglm.core.SchemaUtilities.packSchemaVersion(3));
    schemaBuilder.field("idField", Schema.STRING_SCHEMA);
    schemaBuilder.field("alternateID", Schema.STRING_SCHEMA);
    schemaBuilder.field("allSubscriberIDs", SchemaBuilder.array(Schema.STRING_SCHEMA).schema());
    schemaBuilder.field("eventDate", Timestamp.SCHEMA);
    schemaBuilder.field("subscriberAction", SchemaBuilder.string().defaultValue("standard").schema());
    schemaBuilder.field("tenantID", Schema.INT16_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static com.evolving.nglm.core.ConnectSerde<RecordAlternateID> serde = new com.evolving.nglm.core.ConnectSerde<RecordAlternateID>(schema, false, RecordAlternateID.class, RecordAlternateID::pack, RecordAlternateID::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static com.evolving.nglm.core.ConnectSerde<RecordAlternateID> serde() { return serde; }
  public Schema subscriberStreamEventSchema() { return schema(); }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private String idField;
  private String alternateID;
  private Set<String> allSubscriberIDs;
  private Date eventDate;
  private com.evolving.nglm.core.SubscriberStreamEvent.SubscriberAction subscriberAction;
  private int tenantID;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public String getIDField() { return idField; }
  public String getAlternateID() { return alternateID; }
  public Set<String> getAllSubscriberIDs() { return allSubscriberIDs; }
  public Date getEventDate() { return eventDate; }
  public com.evolving.nglm.core.SubscriberStreamEvent.SubscriberAction getSubscriberAction() { return subscriberAction; }
  public int getTenantID() { return tenantID; }
  
  /*****************************************
  *
  *  constructor (simple/unpack)
  *
  *****************************************/

  public RecordAlternateID(String idField, String alternateID, Set<String> allSubscriberIDs, Date eventDate, com.evolving.nglm.core.SubscriberStreamEvent.SubscriberAction subscriberAction, int tenantID)
  {
    this.idField = idField;
    this.alternateID = alternateID;
    this.allSubscriberIDs = allSubscriberIDs;
    this.eventDate = eventDate;
    this.subscriberAction = subscriberAction;
    this.tenantID = tenantID;
  }

  /*****************************************
  *
  *  constructor (copy)
  *
  *****************************************/

  public RecordAlternateID(RecordAlternateID recordAlternateID)
  {
    this.idField = recordAlternateID.getIDField();
    this.alternateID = recordAlternateID.getAlternateID();
    this.allSubscriberIDs = new HashSet<String>(recordAlternateID.getAllSubscriberIDs());
    this.eventDate = recordAlternateID.getEventDate();
    this.subscriberAction = recordAlternateID.getSubscriberAction();
    this.tenantID = recordAlternateID.getTenantID();
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    RecordAlternateID recordAlternateID = (RecordAlternateID) value;
    Struct struct = new Struct(schema);
    struct.put("idField", recordAlternateID.getIDField());
    struct.put("alternateID", recordAlternateID.getAlternateID());
    struct.put("allSubscriberIDs", packAllSubscriberIDs(recordAlternateID.getAllSubscriberIDs()));
    struct.put("eventDate", recordAlternateID.getEventDate());
    struct.put("subscriberAction", recordAlternateID.getSubscriberAction().getExternalRepresentation());
    struct.put("tenantID", (short)recordAlternateID.getTenantID());
    return struct;
  }

  //
  //  subscriberStreamEventPack
  //

  public Object subscriberStreamEventPack(Object value) { return pack(value); }

  /*****************************************
  *
  *  packAllSubscriberIDs
  *
  *****************************************/

  private static List<Object> packAllSubscriberIDs(Set<String> allSubscriberIDs)
  {
    List<Object> result = new ArrayList<Object>();
    for (String subscriberID : allSubscriberIDs)
      {
        result.add(subscriberID);
      }
    return result;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static RecordAlternateID unpack(SchemaAndValue schemaAndValue)
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
    String idField = valueStruct.getString("idField");
    String alternateID = valueStruct.getString("alternateID");
    Set<String> allSubscriberIDs = unpackAllSubscriberIDs((List<String>) valueStruct.get("allSubscriberIDs"));
    Date eventDate = (Date) valueStruct.get("eventDate");
    com.evolving.nglm.core.SubscriberStreamEvent.SubscriberAction subscriberAction = (schemaVersion >= 2) ? com.evolving.nglm.core.SubscriberStreamEvent.SubscriberAction.fromExternalRepresentation(valueStruct.getString("subscriberAction")) : com.evolving.nglm.core.SubscriberStreamEvent.SubscriberAction.Standard;
    int tenantID = schema.field("tenantID") != null ? valueStruct.getInt16("tenantID") : 1; // by default tenantID = 1
    //
    //  return
    //

    return new RecordAlternateID(idField, alternateID, allSubscriberIDs, eventDate, subscriberAction, tenantID);
  }

  /*****************************************
  *
  *  unpackAllSubscriberIDs
  *
  *****************************************/

  private static Set<String> unpackAllSubscriberIDs(List<String> allSubscriberIDs)
  {
    Set<String> result = new HashSet<String>();
    for (String subscriberID : allSubscriberIDs)
      {
        result.add(subscriberID);
      }
    return result;
  }
  @Override
  public String toString()
  {
    String result =  "RecordAlternateID [idField=" + idField + ", alternateID=" + alternateID + ", allSubscriberIDs= [";
    if(allSubscriberIDs != null)
      {
        for(String s : allSubscriberIDs)
          {
            result = result + s + " ";
          }
      }
    
    result = result + "], eventDate=" + eventDate + ", subscriberAction=" + subscriberAction + ", tenantID=" + tenantID + "]";
    return result;
  }
  
  
}
