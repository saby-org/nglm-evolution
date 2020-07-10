/*****************************************************************************
*
*  ProfileChangeEvent.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Date;

import org.apache.kafka.connect.data.*;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SubscriberStreamOutput;
import com.evolving.nglm.evolution.EvolutionEngineEvent;
import com.evolving.nglm.evolution.ParameterMap;

public class ProfileChangeEvent extends SubscriberStreamOutput implements EvolutionEngineEvent
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
    schemaBuilder.name("profileChangeEvent");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(subscriberStreamOutputSchema().version(),8));
    for (Field field : subscriberStreamOutputSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("eventDate", Timestamp.SCHEMA);
    schemaBuilder.field("oldValues", ParameterMap.schema());
    schemaBuilder.field("newValues", ParameterMap.schema());
    schema = schemaBuilder.build();
  };

  //
  // serde
  //

  private static ConnectSerde<ProfileChangeEvent> serde = new ConnectSerde<ProfileChangeEvent>(schema, false, ProfileChangeEvent.class, ProfileChangeEvent::pack, ProfileChangeEvent::unpack);

  //
  // constants
  //
  
  public final static String CRITERION_FIELD_NAME_OLD_PREFIX = "profilechange.old.";
  public final static String CRITERION_FIELD_NAME_NEW_PREFIX = "profilechange.new.";
  public final static String CRITERION_FIELD_NAME_IS_UPDATED_PREFIX = "profilechange.updated."; 
  
  //
  // accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<ProfileChangeEvent> serde() { return serde; }
  public Schema subscriberStreamEventSchema() { return schema(); }

  /****************************************
  *
  * data
  *
  ****************************************/

  private Date eventDate;
  private String subscriberID;
  private ParameterMap oldValues;
  private ParameterMap newValues;
  
  /****************************************
  *
  * accessors
  *
  ****************************************/

  public String getEventName() { return "profile update"; }
  public String getSubscriberID() { return subscriberID; }
  public Date getEventDate() { return eventDate; }
  public ParameterMap getOldValues() { return oldValues; }
  public ParameterMap getNewValues() { return newValues; }
    
  /****************************************
  *
  * criterionField methods
  *
  ****************************************/
  
  public Object getOldValue(String fieldName) 
  {
    return oldValues.get(fieldName.substring(ProfileChangeEvent.CRITERION_FIELD_NAME_OLD_PREFIX.length()));
  }
  
  public Object getNewValue(String fieldName)
  {
    return newValues.get(fieldName.substring(ProfileChangeEvent.CRITERION_FIELD_NAME_NEW_PREFIX.length()));
  }
  
  public Object getIsProfileFieldUpdated(String fieldName) 
  {
    return newValues.containsKey(fieldName.substring(ProfileChangeEvent.CRITERION_FIELD_NAME_IS_UPDATED_PREFIX.length()));
  }

  /*****************************************
  *
  * constructor
  *
  *****************************************/

  public ProfileChangeEvent(String subscriberID, Date eventDate, ParameterMap oldValues, ParameterMap newValues)
  {
    this.subscriberID = subscriberID;
    this.eventDate = eventDate;
    this.oldValues = oldValues;
    this.newValues = newValues;
  }

  /*****************************************
  *
  * constructor unpack
  *
  *****************************************/

  public ProfileChangeEvent(SchemaAndValue schemaAndValue, String subscriberID, Date eventDate, ParameterMap oldValues, ParameterMap newValues)
  {
    super(schemaAndValue);
    this.subscriberID = subscriberID;
    this.eventDate = eventDate;
    this.oldValues = oldValues;
    this.newValues = newValues;
  }

  /*****************************************
  *
  * pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    ProfileChangeEvent profileChangeEvent = (ProfileChangeEvent) value;
    Struct struct = new Struct(schema);
    packSubscriberStreamOutput(struct,profileChangeEvent);
    struct.put("subscriberID", profileChangeEvent.getSubscriberID());
    struct.put("eventDate", profileChangeEvent.getEventDate());
    struct.put("oldValues", ParameterMap.pack(profileChangeEvent.getOldValues()));
    struct.put("newValues", ParameterMap.pack(profileChangeEvent.getNewValues()));
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

  public static ProfileChangeEvent unpack(SchemaAndValue schemaAndValue)
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
    ParameterMap oldValues = ParameterMap.unpack(new SchemaAndValue(schema.field("oldValues").schema(), valueStruct.get("oldValues")));
    ParameterMap newValues = ParameterMap.unpack(new SchemaAndValue(schema.field("newValues").schema(), valueStruct.get("newValues")));

    //
    // return
    //

    return new ProfileChangeEvent(schemaAndValue, subscriberID, eventDate, oldValues, newValues);
  }
}
