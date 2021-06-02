  /*****************************************
  *
  *  LoyaltyProgramMissionHistory.java
  *
  *****************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.LoyaltyProgramMission.LoyaltyProgramStepChange;

public class LoyaltyProgramMissionHistory 
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
    schemaBuilder.name("loyalty_program_mission_history");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("loyaltyProgramID", Schema.STRING_SCHEMA);
    schemaBuilder.field("stepHistory", SchemaBuilder.array(StepHistory.schema()).schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<LoyaltyProgramMissionHistory> serde = new ConnectSerde<LoyaltyProgramMissionHistory>(schema, false, LoyaltyProgramMissionHistory.class, LoyaltyProgramMissionHistory::pack, LoyaltyProgramMissionHistory::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<LoyaltyProgramMissionHistory> serde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/
  private String loyaltyProgramID;
  private List<StepHistory> stepHistory;
  
  /*****************************************
  *
  *  accessors
  *
  *****************************************/
  
  public String getLoyaltyProgramID() { return loyaltyProgramID; }
  public List<StepHistory> getStepHistory() { return stepHistory; }
  public void clearStepHistory() { if (stepHistory != null) stepHistory.clear(); }
  
  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    LoyaltyProgramMissionHistory loyaltyProgramHistory = (LoyaltyProgramMissionHistory) value;
    Struct struct = new Struct(schema);
    struct.put("loyaltyProgramID", loyaltyProgramHistory.getLoyaltyProgramID());
    struct.put("stepHistory", packStepHistory(loyaltyProgramHistory.getStepHistory()));
    return struct;
  }
  
  /*****************************************
  *
  *  packStepHistory
  *
  *****************************************/

  private static List<Object> packStepHistory(List<StepHistory> stepHistory)
  {
    List<Object> result = new ArrayList<Object>();
    for (StepHistory step : stepHistory)
      {
        result.add(StepHistory.pack(step));
      }
    return result;
  }
  
  /*****************************************
  *
  *  constructor -- external
  *
  *****************************************/

  public LoyaltyProgramMissionHistory(String loyaltyProgramID)
  {
    this.loyaltyProgramID = loyaltyProgramID;
    this.stepHistory = new ArrayList<StepHistory>();
  }
  
  /*****************************************
  *
  *  constructor -- copy
  *
  *****************************************/
  
  public LoyaltyProgramMissionHistory(LoyaltyProgramMissionHistory loyaltyProgramHistory)
  {
    this.loyaltyProgramID = loyaltyProgramHistory.getLoyaltyProgramID();
    this.stepHistory = new ArrayList<StepHistory>();
    for(StepHistory stat : loyaltyProgramHistory.getStepHistory())
      {
        this.stepHistory.add(stat);
      }
  }
  
  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public LoyaltyProgramMissionHistory(String loyaltyProgramID, List<StepHistory> stepHistory)
  {
    this.loyaltyProgramID = loyaltyProgramID;
    this.stepHistory = stepHistory;
  }
  
  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static LoyaltyProgramMissionHistory unpack(SchemaAndValue schemaAndValue)
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
    String loyaltyProgramID = valueStruct.getString("loyaltyProgramID");
    List<StepHistory> stepHistory =  unpackStepHistory(schema.field("stepHistory").schema(), valueStruct.get("stepHistory"));
    
    //
    //  return
    //

    return new LoyaltyProgramMissionHistory(loyaltyProgramID, stepHistory);
  }
  
  /*****************************************
  *
  *  unpackStepHistory
  *
  *****************************************/

  private static List<StepHistory> unpackStepHistory(Schema schema, Object value)
  {
    //
    //  get schema for stepHistory
    //

    Schema stepHistorySchema = schema.valueSchema();

    //
    //  unpack
    //

    List<StepHistory> result = new ArrayList<StepHistory>();
    List<Object> valueArray = (List<Object>) value;
    for (Object step : valueArray)
      {
        result.add(StepHistory.unpack(new SchemaAndValue(stepHistorySchema, step)));
      }

    //
    //  return
    //

    return result;
  }
  
  /*****************************************
  *
  *  addStepInformation
  *
  *****************************************/
  
  public void addStepHistory(String fromStep, String toStep, Date enrollmentDate, String deliveryRequestID, LoyaltyProgramStepChange stepUpdateType) 
  {
    StepHistory stepHistory = new StepHistory(fromStep, toStep, enrollmentDate, deliveryRequestID, stepUpdateType);
    this.stepHistory.add(stepHistory);

  }

  /*****************************************
  *
  *  getLastStepEntered
  *
  *****************************************/
  
  public StepHistory getLastStepEntered()
  {
    Comparator<StepHistory> cmp = new Comparator<StepHistory>() 
    {
      @Override
      public int compare(StepHistory step1, StepHistory step2) 
      {
        return step1.getTransitionDate().compareTo(step2.getTransitionDate());
      }
    };
    
    return (this.stepHistory!=null && !this.stepHistory.isEmpty()) ? Collections.max(this.stepHistory, cmp) : null;
  }
  
  public static class StepHistory
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
      schemaBuilder.name("step_history");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(2));
      schemaBuilder.field("fromStep", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("toStep", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("transitionDate", Timestamp.builder().optional().schema());
      schemaBuilder.field("deliveryRequestID", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("stepUpdateType", Schema.OPTIONAL_STRING_SCHEMA);
      schema = schemaBuilder.build();
    };

    //
    //  serde
    //

    private static ConnectSerde<StepHistory> serde = new ConnectSerde<StepHistory>(schema, false, StepHistory.class, StepHistory::pack, StepHistory::unpack);

    //
    //  accessor
    //

    public static Schema schema() { return schema; }
    public static ConnectSerde<StepHistory> serde() { return serde; }

    /*****************************************
    *
    *  data
    *
    *****************************************/

    private String fromStep;
    private String toStep;
    private Date transitionDate;
    private String deliveryRequestID;
    private LoyaltyProgramStepChange stepUpdateType;
    
    /*****************************************
    *
    *  accessors
    *
    *****************************************/
    
    public String getFromStep() { return fromStep; }
    public String getToStep() { return toStep; }
    public Date getTransitionDate() { return transitionDate; }
    public String getDeliveryRequestID() { return deliveryRequestID; }
    public LoyaltyProgramStepChange getStepUpdateType() { return stepUpdateType; }
    
    /*****************************************
    *
    *  pack
    *
    *****************************************/

    public static Object pack(Object value)
    {
      StepHistory stepHistory = (StepHistory) value;
      Struct struct = new Struct(schema);
      struct.put("fromStep", stepHistory.getFromStep());
      struct.put("toStep", stepHistory.getToStep());
      struct.put("transitionDate", stepHistory.getTransitionDate());
      struct.put("deliveryRequestID", stepHistory.getDeliveryRequestID());
      struct.put("stepUpdateType", stepHistory.getStepUpdateType().getExternalRepresentation());
      return struct;
    }
    
    /*****************************************
    *
    *  constructor -- unpack
    *
    *****************************************/

    public StepHistory(String fromStep, String toStep, Date transitionDate, String deliveryRequestID, LoyaltyProgramStepChange stepUpdateType)
    {
      this.fromStep = fromStep;
      this.toStep = toStep;
      this.transitionDate = transitionDate;
      this.deliveryRequestID = deliveryRequestID;
      this.stepUpdateType = stepUpdateType;
    }
    
    /*****************************************
    *
    *  unpack
    *
    *****************************************/

    public static StepHistory unpack(SchemaAndValue schemaAndValue)
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
      String fromStep = valueStruct.getString("fromStep");
      String toStep = valueStruct.getString("toStep");
      Date transitionDate = (Date) valueStruct.get("transitionDate");
      String deliveryRequestID = valueStruct.getString("deliveryRequestID");
      LoyaltyProgramStepChange loyaltyProgramStepChange = LoyaltyProgramStepChange.fromExternalRepresentation(valueStruct.getString("stepUpdateType"));
            
      //
      //  return
      //

      return new StepHistory(fromStep, toStep, transitionDate, deliveryRequestID, loyaltyProgramStepChange);
    }
    
    /*****************************************
    *
    *  toString -- used for elasticsearch
    *
    *****************************************/
    
    @Override
    public String toString()
    {
      return fromStep + ";" + toStep + ";" + transitionDate.getTime();
    }
    
  }
  
}
