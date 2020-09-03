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
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.LoyaltyProgram.LoyaltyProgramOperation;
import com.evolving.nglm.evolution.LoyaltyProgram.LoyaltyProgramType;
import com.evolving.nglm.evolution.LoyaltyProgramHistory.TierHistory;
import com.evolving.nglm.evolution.LoyaltyProgramPoints.LoyaltyProgramTierChange;
import com.evolving.nglm.evolution.LoyaltyProgramPoints.Tier;


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
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),2));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("tierName", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("previousTierName", Schema.OPTIONAL_STRING_SCHEMA);
    schemaBuilder.field("tierEnrollmentDate", Timestamp.builder().optional().schema());
    schemaBuilder.field("statusPoints", SchemaBuilder.int32().defaultValue(0).schema());
    schemaBuilder.field("rewardPoints", SchemaBuilder.int32().defaultValue(0).schema());
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
  private int statusPoints;
  private int rewardPoints;
  private LoyaltyProgramHistory loyaltyProgramHistory;
  
  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getTierName() { return tierName; }
  public String getPreviousTierName() { return previousTierName; }
  public Date getTierEnrollmentDate() { return tierEnrollmentDate; }
  public int getStatusPoints() { return statusPoints; }
  public int getRewardPoints() { return rewardPoints; }
  public LoyaltyProgramHistory getLoyaltyProgramHistory() { return loyaltyProgramHistory; }

  //
  //  setters
  //

  public void setStatusPoints(int statusPoints) { this.statusPoints = statusPoints; }
  public void setRewardPoints(int rewardPoints) { this.rewardPoints = rewardPoints; }

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
    this.statusPoints = 0;
    this.rewardPoints = 0;
    this.loyaltyProgramHistory = loyaltyProgramHistory;
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public LoyaltyProgramPointsState(SchemaAndValue schemaAndValue, String tierName, String previousTierName, Date tierEnrollmentDate, int statusPoints, int rewardPoints, LoyaltyProgramHistory loyaltyProgramHistory)
  {
    super(schemaAndValue);
    this.tierName = tierName;
    this.previousTierName = previousTierName;
    this.tierEnrollmentDate = tierEnrollmentDate;
    this.statusPoints = statusPoints;
    this.rewardPoints = rewardPoints;
    this.loyaltyProgramHistory = loyaltyProgramHistory;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    LoyaltyProgramPointsState loyaltyProgramPointsState = (LoyaltyProgramPointsState) value;
    Struct struct = new Struct(schema);
    packCommon(struct, loyaltyProgramPointsState);
    struct.put("tierName", loyaltyProgramPointsState.getTierName());
    struct.put("previousTierName", loyaltyProgramPointsState.getPreviousTierName());
    struct.put("tierEnrollmentDate", loyaltyProgramPointsState.getTierEnrollmentDate());
    struct.put("statusPoints", loyaltyProgramPointsState.getStatusPoints());
    struct.put("rewardPoints", loyaltyProgramPointsState.getRewardPoints());
    struct.put("loyaltyProgramHistory", LoyaltyProgramHistory.serde().pack(loyaltyProgramPointsState.getLoyaltyProgramHistory()));
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
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion1(schema.version()) : null;

    //
    //  unpack
    //

    Struct valueStruct = (Struct) value;
    String tierName = valueStruct.getString("tierName");
    String previousTierName = valueStruct.getString("previousTierName");
    Date tierEnrollmentDate = (Date) valueStruct.get("tierEnrollmentDate");
    int statusPoints = (schemaVersion >= 2) ? valueStruct.getInt32("statusPoints") : 0;
    int rewardPoints = (schemaVersion >= 2) ? valueStruct.getInt32("rewardPoints") : 0;
    LoyaltyProgramHistory loyaltyProgramHistory = LoyaltyProgramHistory.serde().unpack(new SchemaAndValue(schema.field("loyaltyProgramHistory").schema(), valueStruct.get("loyaltyProgramHistory")));
    
    //  
    //  return
    //

    return new LoyaltyProgramPointsState(schemaAndValue, tierName, previousTierName, tierEnrollmentDate, statusPoints, rewardPoints, loyaltyProgramHistory);
  }
  
  /*****************************************
  *
  *  update
  *
  *****************************************/

  public void update(long loyaltyProgramEpoch, LoyaltyProgramOperation operation, String loyaltyProgramName, String toTier, Date enrollmentDate, String deliveryRequestID, LoyaltyProgramService loyaltyProgramService)
  {
    
    //
    //  get previous state
    //
    
    TierHistory lastTierEntered = null;
    if(loyaltyProgramHistory != null){
      lastTierEntered = loyaltyProgramHistory.getLastTierEntered();
    }
    String fromTier = (lastTierEntered == null ? null : lastTierEntered.getToTier());
    
    //
    // get the tier informations
    //
    Date now = SystemTime.getCurrentTime();
    LoyaltyProgramTierChange tierChangeType = null;
    if(loyaltyProgramHistory != null){
      String loyaltyProgramID = loyaltyProgramHistory.getLoyaltyProgramID();
      LoyaltyProgram loyaltyProgram = loyaltyProgramService.getActiveLoyaltyProgram(loyaltyProgramID, now);
      if (loyaltyProgram instanceof LoyaltyProgramPoints) {
        LoyaltyProgramPoints loyaltyProgramPoints = (LoyaltyProgramPoints) loyaltyProgram;
        Tier tier = loyaltyProgramPoints.getTier(toTier);
        Tier previousTier = loyaltyProgramPoints.getTier(fromTier);
        tierChangeType = Tier.changeFromTierToTier(previousTier, tier);
        }
    }
    switch (operation) {
    case Optin:

      //
      //  update current state
      //
      
      this.loyaltyProgramEpoch = loyaltyProgramEpoch;
      this.loyaltyProgramName = loyaltyProgramName; 
      this.loyaltyProgramEnrollmentDate = enrollmentDate;
      if(this.loyaltyProgramExitDate != null){ this.loyaltyProgramExitDate = null; }
      
      this.previousTierName = fromTier;
      this.tierName = toTier;
      this.tierEnrollmentDate = enrollmentDate;

      //
      //  update history
      //
      
      loyaltyProgramHistory.addTierHistory(fromTier, toTier, enrollmentDate, deliveryRequestID, tierChangeType);
      
      break;

    case Optout:
      
      //
      //  update current state
      //
      
      this.loyaltyProgramEpoch = loyaltyProgramEpoch;
      this.loyaltyProgramName = loyaltyProgramName;
      if(this.loyaltyProgramEnrollmentDate == null){ this.loyaltyProgramEnrollmentDate = enrollmentDate; }
      this.loyaltyProgramExitDate = enrollmentDate;

      this.previousTierName = fromTier;
      this.tierName = null;
      this.tierEnrollmentDate = enrollmentDate;

      //
      //  update history
      //
      
      loyaltyProgramHistory.addTierHistory(fromTier, toTier, enrollmentDate, deliveryRequestID,tierChangeType);
      
      break;

    default:
      break;
    }
    
  }
}
