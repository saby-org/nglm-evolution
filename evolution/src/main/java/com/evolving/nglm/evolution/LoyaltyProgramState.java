/*****************************************************************************
*
*  PointBalance.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.EvolutionUtilities.TimeUnit;
import com.evolving.nglm.evolution.PointFulfillmentRequest.PointOperation;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;


public class LoyaltyProgramState
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
    schemaBuilder.name("loyalty_program_subscriber_state");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("loyaltyProgramEpoch", Schema.INT64_SCHEMA);
    schemaBuilder.field("loyaltyProgramName", Schema.STRING_SCHEMA);
    schemaBuilder.field("loyaltyProgramEnrollmentDate", Timestamp.builder().schema());
    schemaBuilder.field("tierID", Schema.STRING_SCHEMA);
    schemaBuilder.field("tierName", Schema.STRING_SCHEMA);
    schemaBuilder.field("tierEnrollmentDate", Timestamp.builder().schema());

    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<LoyaltyProgramState> serde = new ConnectSerde<LoyaltyProgramState>(schema, false, LoyaltyProgramState.class, LoyaltyProgramState::pack, LoyaltyProgramState::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<LoyaltyProgramState> serde() { return serde; }
  
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private long loyaltyProgramEpoch;
  private String loyaltyProgramName;
  private Date loyaltyProgramEnrollmentDate;
  private String tierID;
  private String tierName;
  private Date tierEnrollmentDate;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public long getLoyaltyProgramEpoch() { return loyaltyProgramEpoch; }
  public String getLoyaltyProgramName() { return loyaltyProgramName; }
  public Date getLoyaltyProgramEnrollmentDate() { return loyaltyProgramEnrollmentDate; }
  public String getTierID() { return tierID; }
  public String getTierName() { return tierName; }
  public Date getTierEnrollmentDate() { return tierEnrollmentDate; }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public LoyaltyProgramState(long loyaltyProgramEpoch, String loyaltyProgramName, Date loyaltyProgramEnrollmentDate, String tierID, String tierName, Date tierEnrollmentDate)
  {
    this.loyaltyProgramEpoch = loyaltyProgramEpoch;
    this.loyaltyProgramName = loyaltyProgramName;
    this.loyaltyProgramEnrollmentDate = loyaltyProgramEnrollmentDate;
    this.tierID = tierID;
    this.tierName = tierName;
    this.tierEnrollmentDate = tierEnrollmentDate;
  }

  /*****************************************
  *
  *  constructor -- copy
  *
  *****************************************/

  public LoyaltyProgramState(LoyaltyProgramState subscriberState)
  {
    this.loyaltyProgramEpoch = subscriberState.getLoyaltyProgramEpoch();
    this.loyaltyProgramName = subscriberState.getLoyaltyProgramName();
    this.loyaltyProgramEnrollmentDate = subscriberState.getLoyaltyProgramEnrollmentDate();
    this.tierID = subscriberState.getTierID();
    this.tierName = subscriberState.getTierName();
    this.tierEnrollmentDate = subscriberState.getTierEnrollmentDate();
  }

  /*****************************************
  *
  *  update
  *
  *****************************************/

  public boolean update(long loyaltyProgramEpoch, String loyaltyProgramName, String tierID, String tierName, Date tierEnrollmentDate)
  {
    this.loyaltyProgramEpoch = loyaltyProgramEpoch;
    this.loyaltyProgramName = loyaltyProgramName;
    this.tierID = tierID;
    this.tierName = tierName;
    this.tierEnrollmentDate = tierEnrollmentDate;
    return true;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    LoyaltyProgramState subscriberState = (LoyaltyProgramState) value;
    Struct struct = new Struct(schema);
    struct.put("loyaltyProgramEpoch", subscriberState.getLoyaltyProgramEpoch());
    struct.put("loyaltyProgramName", subscriberState.getLoyaltyProgramName());
    struct.put("loyaltyProgramEnrollmentDate", subscriberState.getLoyaltyProgramEnrollmentDate());
    struct.put("tierID", subscriberState.getTierID());
    struct.put("tierName", subscriberState.getTierName());
    struct.put("tierEnrollmentDate", subscriberState.getTierEnrollmentDate());
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static LoyaltyProgramState unpack(SchemaAndValue schemaAndValue)
  {
    //
    //  data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion0(schema.version()) : null;

    //
    //  unpack
    //

    Struct valueStruct = (Struct) value;
    long loyaltyProgramEpoch = valueStruct.getInt64("loyaltyProgramEpoch");
    String loyaltyProgramName = valueStruct.getString("loyaltyProgramName");
    Date loyaltyProgramEnrollmentDate = (Date) valueStruct.get("loyaltyProgramEnrollmentDate");
    String tierID = valueStruct.getString("tierID");
    String tierName = valueStruct.getString("tierName");
    Date tierEnrollmentDate = (Date) valueStruct.get("tierEnrollmentDate");
    
    //  
    //  return
    //

    return new LoyaltyProgramState(loyaltyProgramEpoch, loyaltyProgramName, loyaltyProgramEnrollmentDate, tierID, tierName, tierEnrollmentDate);
  }
}
