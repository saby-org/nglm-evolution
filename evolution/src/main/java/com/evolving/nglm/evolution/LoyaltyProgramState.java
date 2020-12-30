/*****************************************************************************
*
*  LoyaltyProgramState.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.evolving.nglm.evolution.retention.Cleanable;
import com.evolving.nglm.evolution.retention.RetentionService;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.LoyaltyProgram.LoyaltyProgramType;


public class LoyaltyProgramState implements Cleanable
{
  /*****************************************
  *
  *  schema
  *
  *****************************************/

  //
  //  schema
  //

  private static Schema commonSchema = null;
  static
  {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("loyalty_program_subscriber_state");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(2));
    schemaBuilder.field("loyaltyProgramType", Schema.STRING_SCHEMA);
    schemaBuilder.field("loyaltyProgramEpoch", Schema.INT64_SCHEMA);
    schemaBuilder.field("loyaltyProgramID", Schema.STRING_SCHEMA);
    schemaBuilder.field("loyaltyProgramName", Schema.STRING_SCHEMA);
    schemaBuilder.field("loyaltyProgramEnrollmentDate", Timestamp.builder().schema());
    schemaBuilder.field("loyaltyProgramExitDate", Timestamp.builder().optional().schema());
    commonSchema = schemaBuilder.build();
  };

  //
  // serde
  //

  private static ConnectSerde<LoyaltyProgramState> commonSerde;
  static
  {
    List<ConnectSerde<? extends LoyaltyProgramState>> loyaltyProgramStateSerdes = new ArrayList<ConnectSerde<? extends LoyaltyProgramState>>();
    loyaltyProgramStateSerdes.add(LoyaltyProgramPointsState.serde());
    commonSerde = new ConnectSerde<LoyaltyProgramState>("loyaltyProgramState", false, loyaltyProgramStateSerdes.toArray(new ConnectSerde[0]));
  }

  //
  //  accessor
  //

  public static Schema commonSchema() { return commonSchema; }
  public static ConnectSerde<LoyaltyProgramState> commonSerde() { return commonSerde; }
  
  /*****************************************
  *
  *  data
  *
  *****************************************/

  protected LoyaltyProgramType loyaltyProgramType;
  protected long loyaltyProgramEpoch;
  protected String loyaltyProgramName;
  protected String loyaltyProgramID;
  protected Date loyaltyProgramEnrollmentDate;
  protected Date loyaltyProgramExitDate;
  
  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public LoyaltyProgramType getLoyaltyProgramType() { return loyaltyProgramType; }
  public long getLoyaltyProgramEpoch() { return loyaltyProgramEpoch; }
  public String getLoyaltyProgramName() { return loyaltyProgramName; }
  public String getLoyaltyProgramID() { return loyaltyProgramID; }
  public Date getLoyaltyProgramEnrollmentDate() { return loyaltyProgramEnrollmentDate; }
  public Date getLoyaltyProgramExitDate() { return loyaltyProgramExitDate; }

  @Override public Date getExpirationDate(RetentionService retentionService, int tenantID) { return getLoyaltyProgramExitDate(); }
  @Override public Duration getRetention(RetentionType type, RetentionService retentionService, int tenantID) {
    switch (type){
      case KAFKA_DELETION:
        return Duration.ofDays(Deployment.getKafkaRetentionDaysLoyaltyPrograms());
    }
    return null;
  }

  /*****************************************
  *
  *  constructor 
  *
  *****************************************/

  public LoyaltyProgramState(LoyaltyProgramType loyaltyProgramType, long loyaltyProgramEpoch, String loyaltyProgramName, String loyaltyProgramID, Date loyaltyProgramEnrollmentDate, Date loyaltyProgramExitDate)
  {
    this.loyaltyProgramType = loyaltyProgramType;
    this.loyaltyProgramEpoch = loyaltyProgramEpoch;
    this.loyaltyProgramName = loyaltyProgramName;
    this.loyaltyProgramID = loyaltyProgramID;
    this.loyaltyProgramEnrollmentDate = loyaltyProgramEnrollmentDate;
    this.loyaltyProgramExitDate = loyaltyProgramExitDate;
  }

  /*****************************************
  *
  *  packCommon
  *
  *****************************************/

  public static void packCommon(Struct struct, LoyaltyProgramState subscriberState)
  {
    struct.put("loyaltyProgramType", subscriberState.getLoyaltyProgramType().getExternalRepresentation());
    struct.put("loyaltyProgramEpoch", subscriberState.getLoyaltyProgramEpoch());
    struct.put("loyaltyProgramName", subscriberState.getLoyaltyProgramName());
    struct.put("loyaltyProgramID", subscriberState.getLoyaltyProgramID());
    struct.put("loyaltyProgramEnrollmentDate", subscriberState.getLoyaltyProgramEnrollmentDate());
    struct.put("loyaltyProgramExitDate", subscriberState.getLoyaltyProgramExitDate());
  }

  /*****************************************
  *
  *  constructor (unpack)
  *
  *****************************************/

  protected LoyaltyProgramState(SchemaAndValue schemaAndValue)
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
    LoyaltyProgramType loyaltyProgramType = LoyaltyProgramType.fromExternalRepresentation(valueStruct.getString("loyaltyProgramType"));
    long loyaltyProgramEpoch = valueStruct.getInt64("loyaltyProgramEpoch");
    String loyaltyProgramName = valueStruct.getString("loyaltyProgramName");
    String loyaltyProgramID = (schemaVersion >= 2) ? valueStruct.getString("loyaltyProgramID") : "";
    Date loyaltyProgramEnrollmentDate = (Date) valueStruct.get("loyaltyProgramEnrollmentDate");
    Date loyaltyProgramExitDate = (Date) valueStruct.get("loyaltyProgramExitDate");
    
    //  
    //  return
    //

    this.loyaltyProgramType = loyaltyProgramType;
    this.loyaltyProgramEpoch = loyaltyProgramEpoch;
    this.loyaltyProgramName = loyaltyProgramName;
    this.loyaltyProgramID = loyaltyProgramID;
    this.loyaltyProgramEnrollmentDate = loyaltyProgramEnrollmentDate;
    this.loyaltyProgramExitDate = loyaltyProgramExitDate;

  }
}
