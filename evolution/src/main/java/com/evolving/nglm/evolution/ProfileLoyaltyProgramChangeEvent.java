/*****************************************************************************
*
*  ProfileLoyaltyProgramChangeEvent.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Date;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SubscriberStreamOutput;
import com.evolving.nglm.evolution.EvolutionEngineEvent;
import com.evolving.nglm.evolution.LoyaltyProgram.LoyaltyProgramType;

public class ProfileLoyaltyProgramChangeEvent implements EvolutionEngineEvent, SubscriberStreamOutput
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
    schemaBuilder.name("profileLoyaltyProgramChangeEvent");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("subscriberID", Schema.STRING_SCHEMA);
    schemaBuilder.field("eventDate", Timestamp.SCHEMA);
    schemaBuilder.field("loyaltyProgramID", Schema.STRING_SCHEMA);
    schemaBuilder.field("loyaltyProgramType", Schema.STRING_SCHEMA);
    schemaBuilder.field("infos", ParameterMap.schema());
    schema = schemaBuilder.build();
  };

  //
  // serde
  //

  private static ConnectSerde<ProfileLoyaltyProgramChangeEvent> serde = new ConnectSerde<ProfileLoyaltyProgramChangeEvent>(schema, false, ProfileLoyaltyProgramChangeEvent.class, ProfileLoyaltyProgramChangeEvent::pack, ProfileLoyaltyProgramChangeEvent::unpack);

  //
  // accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<ProfileLoyaltyProgramChangeEvent> serde() { return serde; }
  public Schema subscriberStreamEventSchema() { return schema(); }
  
  //
  // constant Enum
  //
  
  public static enum ProfileLoyaltyProgramChangeCriterionFieldType {
    LOYALTY_PROGRAM_NAME,
    ENTERING,
    LEAVING,
    TIER_UPDATE
  }

  /****************************************
  *
  * data
  *
  ****************************************/

  private Date eventDate;
  private String subscriberID;
  private String loyaltyProgramID;
  private LoyaltyProgramType loyaltyProgramType;
  private ParameterMap infos;
  
  /****************************************
  *
  * accessors
  *
  ****************************************/

  public String getEventName() { return "profileLoyaltyProgramChangeEvent"; }
  public String getSubscriberID() { return subscriberID; }
  public Date getEventDate() { return eventDate; }
  public String getLoyaltyProgramID() { return loyaltyProgramID; }
  public LoyaltyProgramType getLoyaltyProgramType() { return loyaltyProgramType; }
  public ParameterMap getInfos() { return infos; }

  
  /*****************************************
  *
  * constructor
  *
  *****************************************/

  public ProfileLoyaltyProgramChangeEvent(String subscriberID, Date eventDate, String loyaltyProgramID, LoyaltyProgramType loyaltyProgramType, ParameterMap infos)
  {
    this.subscriberID = subscriberID;
    this.eventDate = eventDate;
    this.loyaltyProgramID = loyaltyProgramID;
    this.loyaltyProgramType = loyaltyProgramType;
    this.infos = infos;
  }

  /*****************************************
  *
  * pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    ProfileLoyaltyProgramChangeEvent profileLoyaltyProgramChangeEvent = (ProfileLoyaltyProgramChangeEvent) value;
    Struct struct = new Struct(schema);
    struct.put("subscriberID", profileLoyaltyProgramChangeEvent.getSubscriberID());
    struct.put("eventDate", profileLoyaltyProgramChangeEvent.getEventDate());
    struct.put("loyaltyProgramID", profileLoyaltyProgramChangeEvent.getLoyaltyProgramID());
    struct.put("loyaltyProgramType", profileLoyaltyProgramChangeEvent.getLoyaltyProgramType().name());
    struct.put("infos", ParameterMap.pack(profileLoyaltyProgramChangeEvent.getInfos()));
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

  public static ProfileLoyaltyProgramChangeEvent unpack(SchemaAndValue schemaAndValue)
  {
    //
    // data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;

    //
    // unpack
    //

    Struct valueStruct = (Struct) value;
    String subscriberID = valueStruct.getString("subscriberID");
    Date eventDate = (Date) valueStruct.get("eventDate");
    String loyaltyProgramID = valueStruct.getString("loyaltyProgramID");
    LoyaltyProgramType loyaltyProgramType = LoyaltyProgramType.valueOf(valueStruct.getString("loyaltyProgramType"));
    ParameterMap infos = ParameterMap.unpack(new SchemaAndValue(schema.field("infos").schema(), valueStruct.get("infos")));

    //
    // return
    //

    return new ProfileLoyaltyProgramChangeEvent(subscriberID, eventDate, loyaltyProgramID, loyaltyProgramType, infos);
  }
}
