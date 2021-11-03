/*****************************************************************************
*
*  ProfileSegmentChangeEvent.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import org.apache.kafka.connect.data.*;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SubscriberStreamOutput;

public class ProfileSegmentChangeEvent extends SubscriberStreamOutput implements EvolutionEngineEvent
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
    schemaBuilder.name("profileSegmentChange");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(subscriberStreamOutputSchema().version(),9));
    for (Field field : subscriberStreamOutputSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("oldValues", ParameterMap.schema());
    schemaBuilder.field("newValues", ParameterMap.schema());
    schema = schemaBuilder.build();
  };

  //
  // serde
  //

  private static ConnectSerde<ProfileSegmentChangeEvent> serde = new ConnectSerde<ProfileSegmentChangeEvent>(schema, false, ProfileSegmentChangeEvent.class, ProfileSegmentChangeEvent::pack, ProfileSegmentChangeEvent::unpack);

  //
  // constants
  //
  
  public final static String CRITERION_FIELD_NAME_OLD_PREFIX = "segmentchange.old.";
  public final static String CRITERION_FIELD_NAME_NEW_PREFIX = "segmentchange.new.";
  public final static String CRITERION_FIELD_NAME_IS_UPDATED_PREFIX = "segmentchange.isupdated.";

  //
  // enum
  //
  
  public static enum SEGMENT_ENTERING_LEAVING {
    ENTERING,
    LEAVING
  }
  
  //
  // accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<ProfileSegmentChangeEvent> serde() { return serde; }
  public Schema subscriberStreamEventSchema() { return schema(); }

  /****************************************
  *
  * data
  *
  ****************************************/

  private String subscriberID;
  private ParameterMap oldValues;
  private ParameterMap newValues;
  
  /****************************************
  *
  * accessors
  *
  ****************************************/

  public String getEventName() { return "segment update"; }
  public String getSubscriberID() { return subscriberID; }
  public ParameterMap getOldValues() { return oldValues; }
  public ParameterMap getNewValues() { return newValues; }
  
  /****************************************
  *
  * criterionField methods
  *
  ****************************************/

  public Object isDimensionUpdated(String dimensionName) {
    return newValues.containsKey(dimensionName.substring(CRITERION_FIELD_NAME_IS_UPDATED_PREFIX.length()));
  }
  
  public Object getOldSegment(String dimensionName) {
    return oldValues.get(dimensionName.substring(CRITERION_FIELD_NAME_OLD_PREFIX.length()));    
  }
  
  public Object getNewSegment(String dimensionName) {
    return newValues.get(dimensionName.substring(CRITERION_FIELD_NAME_NEW_PREFIX.length()));
  }

  /*****************************************
  *
  * constructor
  *
  *****************************************/

  public ProfileSegmentChangeEvent(String subscriberID, ParameterMap oldValues, ParameterMap newValues)
  {
    this.subscriberID = subscriberID;
    this.oldValues = oldValues;
    this.newValues = newValues;
  }

  /*****************************************
  *
  * constructor unpack
  *
  *****************************************/

  public ProfileSegmentChangeEvent(SchemaAndValue schemaAndValue, String subscriberID, ParameterMap oldValues, ParameterMap newValues)
  {
    super(schemaAndValue);
    this.subscriberID = subscriberID;
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
    ProfileSegmentChangeEvent profileSegmentChangeEvent = (ProfileSegmentChangeEvent) value;
    Struct struct = new Struct(schema);
    packSubscriberStreamOutput(struct,profileSegmentChangeEvent);
    struct.put("subscriberID", profileSegmentChangeEvent.getSubscriberID());
    struct.put("oldValues", ParameterMap.pack(profileSegmentChangeEvent.getOldValues()));
    struct.put("newValues", ParameterMap.pack(profileSegmentChangeEvent.getNewValues()));
    
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

  public static ProfileSegmentChangeEvent unpack(SchemaAndValue schemaAndValue)
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
    ParameterMap oldValues = ParameterMap.unpack(new SchemaAndValue(schema.field("oldValues").schema(), valueStruct.get("oldValues")));
    ParameterMap newValues = ParameterMap.unpack(new SchemaAndValue(schema.field("newValues").schema(), valueStruct.get("newValues")));

    //
    // return
    //

    return new ProfileSegmentChangeEvent(schemaAndValue, subscriberID, oldValues, newValues);
  }
}
