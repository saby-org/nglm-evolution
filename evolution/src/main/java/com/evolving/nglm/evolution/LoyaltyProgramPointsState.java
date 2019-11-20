/*****************************************************************************
*
*  LoyaltyProgramPointsState.java
*
*****************************************************************************/

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
import com.evolving.nglm.evolution.LoyaltyProgram.LoyaltyProgramOperation;
import com.evolving.nglm.evolution.LoyaltyProgram.LoyaltyProgramType;
import com.evolving.nglm.evolution.LoyaltyProgramHistory.TierHistory;


public class LoyaltyProgramPointsState extends LoyaltyProgramState
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
    schemaBuilder.name("loyalty_program_points_subscriber_state");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("tierName", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("previousTierName", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("tierEnrollmentDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("loyaltyProgramHistory", LoyaltyProgramHistory.schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<LoyaltyProgramPointsState> serde = new ConnectSerde<LoyaltyProgramPointsState>(schema, false, LoyaltyProgramPointsState.class, LoyaltyProgramPointsState::pack, LoyaltyProgramPointsState::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<LoyaltyProgramPointsState> serde() { return serde; }
  
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private String tierName;
  private String previousTierName;
  private Date tierEnrollmentDate;
  private LoyaltyProgramHistory loyaltyProgramHistory;
  
  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getTierName() { return tierName; }
  public String getPreviousTierName() { return previousTierName; }
  public Date getTierEnrollmentDate() { return tierEnrollmentDate; }
  public LoyaltyProgramHistory getLoyaltyProgramHistory() { return loyaltyProgramHistory; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public LoyaltyProgramPointsState(LoyaltyProgramType loyaltyProgramType, long loyaltyProgramEpoch, String loyaltyProgramName, String loyaltyProgramID, Date loyaltyProgramEnrollmentDate, Date loyaltyProgramExitDate, String tierName, String previousTierName, Date tierEnrollmentDate, LoyaltyProgramHistory loyaltyProgramHistory)
  {
    super(loyaltyProgramType, loyaltyProgramEpoch, loyaltyProgramName, loyaltyProgramID, loyaltyProgramEnrollmentDate, loyaltyProgramExitDate);
    this.tierName = tierName;
    this.previousTierName = previousTierName;
    this.tierEnrollmentDate = tierEnrollmentDate;
    this.loyaltyProgramHistory = loyaltyProgramHistory;
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public LoyaltyProgramPointsState(SchemaAndValue schemaAndValue, String tierName, String previousTierName, Date tierEnrollmentDate, LoyaltyProgramHistory loyaltyProgramHistory)
  {
    super(schemaAndValue);
    this.tierName = tierName;
    this.previousTierName = previousTierName;
    this.tierEnrollmentDate = tierEnrollmentDate;
    this.loyaltyProgramHistory = loyaltyProgramHistory;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    LoyaltyProgramPointsState subscriberState = (LoyaltyProgramPointsState) value;
    Struct struct = new Struct(schema);
    packCommon(struct, subscriberState);
    struct.put("tierName", subscriberState.getTierName());
    struct.put("previousTierName", subscriberState.getPreviousTierName());
    struct.put("tierEnrollmentDate", subscriberState.getTierEnrollmentDate());
    struct.put("loyaltyProgramHistory", LoyaltyProgramHistory.serde().pack(subscriberState.getLoyaltyProgramHistory()));
    return struct;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static LoyaltyProgramPointsState unpack(SchemaAndValue schemaAndValue)
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
    String tierName = valueStruct.getString("tierName");
    String previousTierName = valueStruct.getString("previousTierName");
    Date tierEnrollmentDate = (Date) valueStruct.get("tierEnrollmentDate");
    LoyaltyProgramHistory loyaltyProgramHistory = LoyaltyProgramHistory.serde().unpack(new SchemaAndValue(schema.field("loyaltyProgramHistory").schema(), valueStruct.get("loyaltyProgramHistory")));
    
    //  
    //  return
    //

    return new LoyaltyProgramPointsState(schemaAndValue, tierName, previousTierName, tierEnrollmentDate, loyaltyProgramHistory);
  }
  
  /*****************************************
  *
  *  update
  *
  *****************************************/

  public void update(long loyaltyProgramEpoch, LoyaltyProgramOperation operation, String loyaltyProgramName, String toTier, Date enrollmentDate, String deliveryRequestID)
  {
    
    //
    //  get previous state
    //
    
    TierHistory lastTierEntered = null;
    if(loyaltyProgramHistory != null){
      lastTierEntered = loyaltyProgramHistory.getLastTierEntered();
    }
    String fromTier = (lastTierEntered == null ? null : lastTierEntered.getToTier());
    

    switch (operation) {
    case Optin:

      //
      //  update current state
      //
      
      this.loyaltyProgramEpoch = loyaltyProgramEpoch;
      this.loyaltyProgramName = loyaltyProgramName;
      if(this.loyaltyProgramEnrollmentDate == null){ this.loyaltyProgramEnrollmentDate = enrollmentDate; }
      
      this.previousTierName = fromTier;
      this.tierName = toTier;
      this.tierEnrollmentDate = enrollmentDate;

      //
      //  update history
      //
      
      loyaltyProgramHistory.addTierHistory(fromTier, toTier, enrollmentDate, deliveryRequestID);
      
      break;

    case Optout:
      
      //
      //  update current state
      //
      
      this.loyaltyProgramEpoch = loyaltyProgramEpoch;
      this.loyaltyProgramName = loyaltyProgramName;
      if(this.loyaltyProgramEnrollmentDate == null){ this.loyaltyProgramEnrollmentDate = enrollmentDate; }
      this.loyaltyProgramExitDate = enrollmentDate;
      
      this.tierName = null;
      this.previousTierName = null;
      this.tierEnrollmentDate = null;

      //
      //  update history
      //
      
      loyaltyProgramHistory.addTierHistory(fromTier, toTier, enrollmentDate, deliveryRequestID);
      
      break;

    default:
      break;
    }
    
  }

}
