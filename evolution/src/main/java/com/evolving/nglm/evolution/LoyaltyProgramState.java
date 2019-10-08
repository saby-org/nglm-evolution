/*****************************************************************************
*
*  LoyaltyProgramState.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.LoyaltyProgram.LoyaltyProgramType;


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

  private static Schema commonSchema = null;
  static
  {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("loyalty_program_subscriber_state");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("loyaltyProgramType", Schema.STRING_SCHEMA);
    schemaBuilder.field("loyaltyProgramEpoch", Schema.INT64_SCHEMA);
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
  public Date getLoyaltyProgramEnrollmentDate() { return loyaltyProgramEnrollmentDate; }
  public Date getLoyaltyProgramExitDate() { return loyaltyProgramExitDate; }

  /*****************************************
  *
  *  constructor 
  *
  *****************************************/

  public LoyaltyProgramState(LoyaltyProgramType loyaltyProgramType, long loyaltyProgramEpoch, String loyaltyProgramName, Date loyaltyProgramEnrollmentDate, Date loyaltyProgramExitDate)
  {
    this.loyaltyProgramType = loyaltyProgramType;
    this.loyaltyProgramEpoch = loyaltyProgramEpoch;
    this.loyaltyProgramName = loyaltyProgramName;
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
    Date loyaltyProgramEnrollmentDate = (Date) valueStruct.get("loyaltyProgramEnrollmentDate");
    Date loyaltyProgramExitDate = (Date) valueStruct.get("loyaltyProgramExitDate");
    
    //  
    //  return
    //

    this.loyaltyProgramType = loyaltyProgramType;
    this.loyaltyProgramEpoch = loyaltyProgramEpoch;
    this.loyaltyProgramName = loyaltyProgramName;
    this.loyaltyProgramEnrollmentDate = loyaltyProgramEnrollmentDate;
    this.loyaltyProgramExitDate = loyaltyProgramExitDate;

  }
}
