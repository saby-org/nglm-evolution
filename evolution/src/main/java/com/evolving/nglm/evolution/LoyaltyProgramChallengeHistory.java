  /*****************************************
  *
  *  LoyaltyProgramChallengeHistory.java
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
import com.evolving.nglm.evolution.LoyaltyProgramChallenge.LoyaltyProgramLevelChange;

public class LoyaltyProgramChallengeHistory 
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
    schemaBuilder.name("loyalty_program_challenge_history");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("loyaltyProgramID", Schema.STRING_SCHEMA);
    schemaBuilder.field("levelHistory", SchemaBuilder.array(LevelHistory.schema()).schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<LoyaltyProgramChallengeHistory> serde = new ConnectSerde<LoyaltyProgramChallengeHistory>(schema, false, LoyaltyProgramChallengeHistory.class, LoyaltyProgramChallengeHistory::pack, LoyaltyProgramChallengeHistory::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<LoyaltyProgramChallengeHistory> serde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/
  private String loyaltyProgramID;
  private List<LevelHistory> levelHistory;
  
  /*****************************************
  *
  *  accessors
  *
  *****************************************/
  
  public String getLoyaltyProgramID() { return loyaltyProgramID; }
  public List<LevelHistory> getLevelHistory() { return levelHistory; }
  public List<LevelHistory> getAllLevelHistoryForThisPeriod(Integer occurrenceNumber) 
  { 
    return levelHistory.stream().filter(level -> level.getOccurrenceNumber() == occurrenceNumber).collect(Collectors.toList()); 
  }
  
  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    LoyaltyProgramChallengeHistory loyaltyProgramHistory = (LoyaltyProgramChallengeHistory) value;
    Struct struct = new Struct(schema);
    struct.put("loyaltyProgramID", loyaltyProgramHistory.getLoyaltyProgramID());
    struct.put("levelHistory", packLevelHistory(loyaltyProgramHistory.getLevelHistory()));
    return struct;
  }
  
  /*****************************************
  *
  *  packLevelHistory
  *
  *****************************************/

  private static List<Object> packLevelHistory(List<LevelHistory> levelHistory)
  {
    List<Object> result = new ArrayList<Object>();
    for (LevelHistory level : levelHistory)
      {
        result.add(LevelHistory.pack(level));
      }
    return result;
  }
  
  /*****************************************
  *
  *  constructor -- external
  *
  *****************************************/

  public LoyaltyProgramChallengeHistory(String loyaltyProgramID)
  {
    this.loyaltyProgramID = loyaltyProgramID;
    this.levelHistory = new ArrayList<LevelHistory>();
  }
  
  /*****************************************
  *
  *  constructor -- copy
  *
  *****************************************/
  
  public LoyaltyProgramChallengeHistory(LoyaltyProgramChallengeHistory loyaltyProgramHistory)
  {
    this.loyaltyProgramID = loyaltyProgramHistory.getLoyaltyProgramID();
    this.levelHistory = new ArrayList<LevelHistory>();
    for(LevelHistory stat : loyaltyProgramHistory.getLevelHistory())
      {
        this.levelHistory.add(stat);
      }
  }
  
  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public LoyaltyProgramChallengeHistory(String loyaltyProgramID, List<LevelHistory> levelHistory)
  {
    this.loyaltyProgramID = loyaltyProgramID;
    this.levelHistory = levelHistory;
  }
  
  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static LoyaltyProgramChallengeHistory unpack(SchemaAndValue schemaAndValue)
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
    List<LevelHistory> levelHistory =  unpackLevelHistory(schema.field("levelHistory").schema(), valueStruct.get("levelHistory"));
    
    //
    //  return
    //

    return new LoyaltyProgramChallengeHistory(loyaltyProgramID, levelHistory);
  }
  
  /*****************************************
  *
  *  unpackLevelHistory
  *
  *****************************************/

  private static List<LevelHistory> unpackLevelHistory(Schema schema, Object value)
  {
    //
    //  get schema for LevelHistory
    //

    Schema levelHistorySchema = schema.valueSchema();

    //
    //  unpack
    //

    List<LevelHistory> result = new ArrayList<LevelHistory>();
    List<Object> valueArray = (List<Object>) value;
    for (Object level : valueArray)
      {
        result.add(LevelHistory.unpack(new SchemaAndValue(levelHistorySchema, level)));
      }

    //
    //  return
    //

    return result;
  }
  
  /*****************************************
  *
  *  addLevelInformation
  *
  *****************************************/
  
  public void addLevelHistory(String fromLevel, String toLevel, Integer occurrenceNumber, Date enrollmentDate, String deliveryRequestID, LoyaltyProgramLevelChange levelUpdateType) 
  {
    LevelHistory levelHistory = new LevelHistory(fromLevel, toLevel, occurrenceNumber, enrollmentDate, deliveryRequestID, levelUpdateType);
    this.levelHistory.add(levelHistory);

  }

  /*****************************************
  *
  *  getLastLevelEntered
  *
  *****************************************/
  
  public LevelHistory getLastLevelEntered()
  {
    Comparator<LevelHistory> cmp = new Comparator<LevelHistory>() 
    {
      @Override
      public int compare(LevelHistory level1, LevelHistory level2) 
      {
        return level1.getTransitionDate().compareTo(level2.getTransitionDate());
      }
    };
    
    return (this.levelHistory!=null && !this.levelHistory.isEmpty()) ? Collections.max(this.levelHistory, cmp) : null;
  }
  
  public static class LevelHistory
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
      schemaBuilder.name("level_history");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(2));
      schemaBuilder.field("fromLevel", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("toLevel", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("occurrenceNumber", Schema.OPTIONAL_INT32_SCHEMA);
      schemaBuilder.field("transitionDate", Timestamp.builder().optional().schema());
      schemaBuilder.field("deliveryRequestID", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("levelUpdateType", Schema.OPTIONAL_STRING_SCHEMA);
      schema = schemaBuilder.build();
    };

    //
    //  serde
    //

    private static ConnectSerde<LevelHistory> serde = new ConnectSerde<LevelHistory>(schema, false, LevelHistory.class, LevelHistory::pack, LevelHistory::unpack);

    //
    //  accessor
    //

    public static Schema schema() { return schema; }
    public static ConnectSerde<LevelHistory> serde() { return serde; }

    /*****************************************
    *
    *  data
    *
    *****************************************/

    private String fromLevel;
    private String toLevel;
    private Date transitionDate;
    private String deliveryRequestID;
    private LoyaltyProgramLevelChange levelUpdateType;
    private Integer occurrenceNumber;
    
    /*****************************************
    *
    *  accessors
    *
    *****************************************/
    
    public String getFromLevel() { return fromLevel; }
    public String getToLevel() { return toLevel; }
    public Date getTransitionDate() { return transitionDate; }
    public String getDeliveryRequestID() { return deliveryRequestID; }
    public LoyaltyProgramLevelChange getLevelUpdateType() { return levelUpdateType; }
    public Integer getOccurrenceNumber() { return occurrenceNumber; }
    
    /*****************************************
    *
    *  pack
    *
    *****************************************/

    public static Object pack(Object value)
    {
      LevelHistory levelHistory = (LevelHistory) value;
      Struct struct = new Struct(schema);
      struct.put("fromLevel", levelHistory.getToLevel());
      struct.put("toLevel", levelHistory.getToLevel());
      struct.put("occurrenceNumber", levelHistory.getOccurrenceNumber());
      struct.put("transitionDate", levelHistory.getTransitionDate());
      struct.put("deliveryRequestID", levelHistory.getDeliveryRequestID());
      struct.put("levelUpdateType", levelHistory.getLevelUpdateType().getExternalRepresentation());
      return struct;
    }
    
    /*****************************************
    *
    *  constructor -- unpack
    *
    *****************************************/

    public LevelHistory(String fromLevel, String toLevel, Integer occurrenceNumber, Date transitionDate, String deliveryRequestID, LoyaltyProgramLevelChange levelUpdateType)
    {
      this.fromLevel = fromLevel;
      this.toLevel = toLevel;
      this.occurrenceNumber = occurrenceNumber;
      this.transitionDate = transitionDate;
      this.deliveryRequestID = deliveryRequestID;
      this.levelUpdateType = levelUpdateType;
    }
    
    /*****************************************
    *
    *  unpack
    *
    *****************************************/

    public static LevelHistory unpack(SchemaAndValue schemaAndValue)
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
      String fromLevel = valueStruct.getString("fromLevel");
      String toLevel = valueStruct.getString("toLevel");
      Integer occurrenceNumber = valueStruct.getInt32("occurrenceNumber");
      Date transitionDate = (Date) valueStruct.get("transitionDate");
      String deliveryRequestID = valueStruct.getString("deliveryRequestID");
      LoyaltyProgramLevelChange loyaltyProgramLevelChange = LoyaltyProgramLevelChange.fromExternalRepresentation(valueStruct.getString("levelUpdateType"));
            
      //
      //  return
      //

      return new LevelHistory(fromLevel, toLevel, occurrenceNumber, transitionDate, deliveryRequestID, loyaltyProgramLevelChange);
    }
    
    /*****************************************
    *
    *  toString -- used for elasticsearch
    *
    *****************************************/
    
    @Override
    public String toString()
    {
      return fromLevel + ";" + toLevel + ";" + transitionDate.getTime();
    }
    
  }
  
}
