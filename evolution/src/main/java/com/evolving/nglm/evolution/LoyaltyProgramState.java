/*****************************************************************************
*
*  PointBalance.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.EvolutionUtilities.TimeUnit;
import com.evolving.nglm.evolution.LoyaltyProgram.LoyaltyProgramOperation;
import com.evolving.nglm.evolution.LoyaltyProgramHistory.TierHistory;
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
import java.util.Map;
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
    schemaBuilder.field("loyaltyProgramExitDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("tierID", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("tierName", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("tierEnrollmentDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("loyaltyProgramHistory", LoyaltyProgramHistory.schema());

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
  private Date loyaltyProgramExitDate;
  private String tierID;
  private String tierName;
  private Date tierEnrollmentDate;
  private LoyaltyProgramHistory loyaltyProgramHistory;
  
  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public long getLoyaltyProgramEpoch() { return loyaltyProgramEpoch; }
  public String getLoyaltyProgramName() { return loyaltyProgramName; }
  public Date getLoyaltyProgramEnrollmentDate() { return loyaltyProgramEnrollmentDate; }
  public Date getLoyaltyProgramExitDate() { return loyaltyProgramExitDate; }
  public String getTierID() { return tierID; }
  public String getTierName() { return tierName; }
  public Date getTierEnrollmentDate() { return tierEnrollmentDate; }
  public LoyaltyProgramHistory getLoyaltyProgramHistory() { return loyaltyProgramHistory; }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public LoyaltyProgramState(long loyaltyProgramEpoch, String loyaltyProgramName, Date loyaltyProgramEnrollmentDate, Date loyaltyProgramExitDate, String tierID, String tierName, Date tierEnrollmentDate, LoyaltyProgramHistory loyaltyProgramHistory)
  {
    this.loyaltyProgramEpoch = loyaltyProgramEpoch;
    this.loyaltyProgramName = loyaltyProgramName;
    this.loyaltyProgramEnrollmentDate = loyaltyProgramEnrollmentDate;
    this.loyaltyProgramExitDate = loyaltyProgramExitDate;
    this.tierID = tierID;
    this.tierName = tierName;
    this.tierEnrollmentDate = tierEnrollmentDate;
    this.loyaltyProgramHistory = loyaltyProgramHistory;
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
    this.loyaltyProgramExitDate = subscriberState.getLoyaltyProgramExitDate();
    this.tierID = subscriberState.getTierID();
    this.tierName = subscriberState.getTierName();
    this.tierEnrollmentDate = subscriberState.getTierEnrollmentDate();
    this.loyaltyProgramHistory = subscriberState.getLoyaltyProgramHistory();
  }

  /*****************************************
  *
  *  update
  *
  *****************************************/

  public boolean update(long loyaltyProgramEpoch, LoyaltyProgramOperation operation, String loyaltyProgramName, String tierID, String tierName, Date enrollmentDate, String deliveryRequestID)
  {
    
    //
    //  get previous state
    //
    
    TierHistory lastTierEntered = null;
    if(loyaltyProgramHistory != null){
      lastTierEntered = loyaltyProgramHistory.getLastTierEntered();
    }
    String fromTier = (lastTierEntered == null ? null : lastTierEntered.getToTierID());
    

    switch (operation) {
    case Optin:

      //
      //  update current state
      //
      
      this.loyaltyProgramEpoch = loyaltyProgramEpoch;
      this.loyaltyProgramName = loyaltyProgramName;
      if(this.loyaltyProgramEnrollmentDate == null){ this.loyaltyProgramEnrollmentDate = enrollmentDate; }
      
      this.tierID = tierID;
      this.tierName = tierName;
      this.tierEnrollmentDate = enrollmentDate;

      //
      //  update history
      //
      
      loyaltyProgramHistory.addTierHistory(fromTier, tierID, enrollmentDate, deliveryRequestID);
      
      break;

    case Optout:
      
      //
      //  update current state
      //
      
      this.loyaltyProgramEpoch = loyaltyProgramEpoch;
      this.loyaltyProgramName = loyaltyProgramName;
      if(this.loyaltyProgramEnrollmentDate == null){ this.loyaltyProgramEnrollmentDate = enrollmentDate; }
      this.loyaltyProgramExitDate = enrollmentDate;
      
      this.tierID = null;
      this.tierName = null;
      this.tierEnrollmentDate = null;

      //
      //  update history
      //
      
      loyaltyProgramHistory.addTierHistory(fromTier, tierID, enrollmentDate, deliveryRequestID);
      
      break;

    default:
      break;
    }
    
    //
    //  return
    //
    
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
    struct.put("loyaltyProgramExitDate", subscriberState.getLoyaltyProgramExitDate());
    struct.put("tierID", subscriberState.getTierID());
    struct.put("tierName", subscriberState.getTierName());
    struct.put("tierEnrollmentDate", subscriberState.getTierEnrollmentDate());
    struct.put("loyaltyProgramHistory", LoyaltyProgramHistory.serde().pack(subscriberState.getLoyaltyProgramHistory()));
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
    Date loyaltyProgramExitDate = (Date) valueStruct.get("loyaltyProgramExitDate");
    String tierID = valueStruct.getString("tierID");
    String tierName = valueStruct.getString("tierName");
    Date tierEnrollmentDate = (Date) valueStruct.get("tierEnrollmentDate");
    LoyaltyProgramHistory loyaltyProgramHistory = LoyaltyProgramHistory.serde().unpack(new SchemaAndValue(schema.field("loyaltyProgramHistory").schema(), valueStruct.get("loyaltyProgramHistory")));
    
    //  
    //  return
    //

    return new LoyaltyProgramState(loyaltyProgramEpoch, loyaltyProgramName, loyaltyProgramEnrollmentDate, loyaltyProgramExitDate, tierID, tierName, tierEnrollmentDate, loyaltyProgramHistory);
  }
}
