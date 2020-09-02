package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.LoyaltyProgramPoints.LoyaltyProgramPointsEventInfos;
import com.evolving.nglm.evolution.LoyaltyProgramPoints.LoyaltyProgramTierChange;

public class LoyaltyProgramHistory 
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
    schemaBuilder.name("loyalty_program_history");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("loyaltyProgramID", Schema.STRING_SCHEMA);
    schemaBuilder.field("tierHistory", SchemaBuilder.array(TierHistory.schema()).schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<LoyaltyProgramHistory> serde = new ConnectSerde<LoyaltyProgramHistory>(schema, false, LoyaltyProgramHistory.class, LoyaltyProgramHistory::pack, LoyaltyProgramHistory::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<LoyaltyProgramHistory> serde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/
  private String loyaltyProgramID;
  private List<TierHistory> tierHistory;
  
  /*****************************************
  *
  *  accessors
  *
  *****************************************/
  
  public String getLoyaltyProgramID() { return loyaltyProgramID; }
  public List<TierHistory> getTierHistory() { return tierHistory; }
  
  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    LoyaltyProgramHistory loyaltyProgramHistory = (LoyaltyProgramHistory) value;
    Struct struct = new Struct(schema);
    struct.put("loyaltyProgramID", loyaltyProgramHistory.getLoyaltyProgramID());
    struct.put("tierHistory", packTierHistory(loyaltyProgramHistory.getTierHistory()));
    return struct;
  }
  
  /*****************************************
  *
  *  packTierHistory
  *
  *****************************************/

  private static List<Object> packTierHistory(List<TierHistory> tierHistory)
  {
    List<Object> result = new ArrayList<Object>();
    for (TierHistory tier : tierHistory)
      {
        result.add(TierHistory.pack(tier));
      }
    return result;
  }
  
  /*****************************************
  *
  *  constructor -- external
  *
  *****************************************/

  public LoyaltyProgramHistory(String loyaltyProgramID)
  {
    this.loyaltyProgramID = loyaltyProgramID;
    this.tierHistory = new ArrayList<TierHistory>();
  }
  
  /*****************************************
  *
  *  constructor -- copy
  *
  *****************************************/
  
  public LoyaltyProgramHistory(LoyaltyProgramHistory loyaltyProgramHistory)
  {
    this.loyaltyProgramID = loyaltyProgramHistory.getLoyaltyProgramID();
    
    this.tierHistory = new ArrayList<TierHistory>();
    for(TierHistory stat : loyaltyProgramHistory.getTierHistory())
      {
        this.tierHistory.add(stat);
      }
    
  }
  
  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public LoyaltyProgramHistory(String loyaltyProgramID, List<TierHistory> tierHistory)
  {
    this.loyaltyProgramID = loyaltyProgramID;
    this.tierHistory = tierHistory;
  }
  
  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static LoyaltyProgramHistory unpack(SchemaAndValue schemaAndValue)
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
    List<TierHistory> tierHistory =  unpackTierHistory(schema.field("tierHistory").schema(), valueStruct.get("tierHistory"));
    
    //
    //  return
    //

    return new LoyaltyProgramHistory(loyaltyProgramID, tierHistory);
  }
  
  /*****************************************
  *
  *  unpackTierHistory
  *
  *****************************************/

  private static List<TierHistory> unpackTierHistory(Schema schema, Object value)
  {
    //
    //  get schema for TierHistory
    //

    Schema tierHistorySchema = schema.valueSchema();

    //
    //  unpack
    //

    List<TierHistory> result = new ArrayList<TierHistory>();
    List<Object> valueArray = (List<Object>) value;
    for (Object tier : valueArray)
      {
        result.add(TierHistory.unpack(new SchemaAndValue(tierHistorySchema, tier)));
      }

    //
    //  return
    //

    return result;
  }
  
  /*****************************************
  *
  *  addTierInformation
  *
  *****************************************/
  
  public void addTierHistory(String fromTier, String toTier, Date enrollmentDate, String deliveryRequestID, LoyaltyProgramTierChange tierUpdateType) 
  {
    TierHistory tierHistory = new TierHistory(fromTier, toTier, enrollmentDate, deliveryRequestID, tierUpdateType);
    this.tierHistory.add(tierHistory);

  }

  /*****************************************
  *
  *  getLastTierEntered
  *
  *****************************************/
  
  public TierHistory getLastTierEntered()
  {
    Comparator<TierHistory> cmp = new Comparator<TierHistory>() 
    {
      @Override
      public int compare(TierHistory tier1, TierHistory tier2) 
      {
        return tier1.getTransitionDate().compareTo(tier2.getTransitionDate());
      }
    };
    
    return (this.tierHistory!=null && !this.tierHistory.isEmpty()) ? Collections.max(this.tierHistory, cmp) : null;
  }
  
  public static class TierHistory
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
      schemaBuilder.name("tier_history");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(2));
      schemaBuilder.field("fromTier", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("toTier", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("transitionDate", Timestamp.builder().optional().schema());
      schemaBuilder.field("deliveryRequestID", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("tierUpdateType", Schema.OPTIONAL_STRING_SCHEMA);
      schema = schemaBuilder.build();
    };

    //
    //  serde
    //

    private static ConnectSerde<TierHistory> serde = new ConnectSerde<TierHistory>(schema, false, TierHistory.class, TierHistory::pack, TierHistory::unpack);

    //
    //  accessor
    //

    public static Schema schema() { return schema; }
    public static ConnectSerde<TierHistory> serde() { return serde; }

    /*****************************************
    *
    *  data
    *
    *****************************************/

    private String fromTier;
    private String toTier;
    private Date transitionDate;
    private String deliveryRequestID;
    private LoyaltyProgramTierChange tierUpdateType;
    
    /*****************************************
    *
    *  accessors
    *
    *****************************************/
    
    public String getFromTier() { return fromTier; }
    public String getToTier() { return toTier; }
    public Date getTransitionDate() { return transitionDate; }
    public String getDeliveryRequestID() { return deliveryRequestID; }
    public LoyaltyProgramTierChange getTierUpdateType() { return tierUpdateType; }
    
    /*****************************************
    *
    *  pack
    *
    *****************************************/

    public static Object pack(Object value)
    {
      TierHistory tierHistory = (TierHistory) value;
      Struct struct = new Struct(schema);
      struct.put("fromTier", tierHistory.getFromTier());
      struct.put("toTier", tierHistory.getToTier());
      struct.put("transitionDate", tierHistory.getTransitionDate());
      struct.put("deliveryRequestID", tierHistory.getDeliveryRequestID());
      struct.put("tierUpdateType", tierHistory.getTierUpdateType().getExternalRepresentation());
      return struct;
    }
    
    /*****************************************
    *
    *  constructor -- unpack
    *
    *****************************************/

    public TierHistory(String fromTier, String toTier, Date transitionDate, String deliveryRequestID, LoyaltyProgramTierChange tierUpdateType)
    {
      this.fromTier = fromTier;
      this.toTier = toTier;
      this.transitionDate = transitionDate;
      this.deliveryRequestID = deliveryRequestID;
      this.tierUpdateType = tierUpdateType;
    }
    
    /*****************************************
    *
    *  unpack
    *
    *****************************************/

    public static TierHistory unpack(SchemaAndValue schemaAndValue)
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
      String fromTier = valueStruct.getString("fromTier");
      String toTier = valueStruct.getString("toTier");
      Date transitionDate = (Date) valueStruct.get("transitionDate");
      String deliveryRequestID = valueStruct.getString("deliveryRequestID");
      LoyaltyProgramTierChange tierUpdateType = (schemaVersion >= 2) ? LoyaltyProgramTierChange.fromExternalRepresentation(valueStruct.getString("tierUpdateType")) : null;
            
      //
      //  return
      //

      return new TierHistory(fromTier, toTier, transitionDate, deliveryRequestID, tierUpdateType);
    }
    
    /*****************************************
    *
    *  toString -- used for elasticsearch
    *
    *****************************************/
    
    @Override
    public String toString()
    {
      return fromTier + ";" + toTier + ";" + transitionDate.getTime();
    }
    
  }
  
}
