/*****************************************************************************
*
*  UCGState.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.ReferenceDataValue;
import com.evolving.nglm.core.SchemaUtilities;

import com.evolving.nglm.evolution.UCGRule.UCGRuleCalculationType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class UCGState implements ReferenceDataValue<String>
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(UCGState.class);

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
    schemaBuilder.name("ucg_state");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(2));
    schemaBuilder.field("ucgRule", UCGRule.schema());
    schemaBuilder.field("ucgGroups", SchemaBuilder.array(UCGGroup.schema()).schema());
    schemaBuilder.field("evaluationDate", Timestamp.SCHEMA);
    //this is added to not calculate this targetRatio at each subscriber evaluation
    schemaBuilder.field("targetRatio",Schema.FLOAT64_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<UCGState> serde = new ConnectSerde<UCGState>(schema, false, UCGState.class, UCGState::pack, UCGState::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<UCGState> serde() { return serde; }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private UCGRule ucgRule;
  private Set<UCGGroup> ucgGroups;
  private Date evaluationDate;

  private Double targetRatio;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public UCGRule getUCGRule() { return ucgRule; }
  public Set<UCGGroup> getUCGGroups() { return ucgGroups; }
  public Date getEvaluationDate() { return evaluationDate; }
  public Double getTargetRatio() { return targetRatio; }

  //
  //  convenience accessors
  //

  public String getUCGRuleID() { return ucgRule.getUCGRuleID(); }
  public Integer getRefreshEpoch() { return ucgRule.getRefreshEpoch(); }
  public int getRefreshWindowDays() { return ucgRule.getNoOfDaysForStayOut(); }

  //
  //  ReferenceDataValue
  //
  
  public static String getSingletonKey() { return "ucgState"; }
  @Override public String getKey() { return getSingletonKey(); }
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public UCGState(UCGRule ucgRule, Set<UCGGroup> ucgGroups, Date evaluationDate)
  {
    this.ucgRule = ucgRule;
    this.ucgGroups = ucgGroups;
    this.evaluationDate = evaluationDate;

    //
    // Check if shift probability is well defined in UCG Groups
    //

    boolean skipShiftProbabilityEvaluation = true;
    for (UCGGroup ucgGroup : ucgGroups)
    {
      if(ucgGroup.getShiftProbability() == null) {
        skipShiftProbabilityEvaluation = false;
        break;
      }
    }

    //
    // Shift probability evaluation
    //

    if(!skipShiftProbabilityEvaluation) {

      //
      // Retrieve the target ratio of customers that should be in the Universal Control Group (it is the same for each stratum).
      //

      this.targetRatio = calculateTargetRatio();

      //
      // Compute the shift probability for each stratum
      //    > shiftProbability is the probability for each customer in this stratum to be added in UCG (or removed if it is a negative probability).
      // If we did not succeed to retrieve the target ratio, shift probability will be set to 0 (no change).
      //    > userDelta is the number of customers that should move in (or out) the Universal Control Group to restore the target ratio for this stratum. 
      //    > suitableCustomersNbr is the number of customers available for a state change (depending if we want to add or remove customers from the UCG)
      // TODO The number of suitable customers is not the correct one, it should take into
      // account the fact that some of customers are not available (outside of the refresh window) due to the
      // *noOfDaysForStayOut* variable in UCGRule.
      //

      for (UCGGroup ucgGroup : this.ucgGroups)
        {
          if(ucgGroup.getShiftProbability() != null)
          {
            continue;
          }
          calculateAndApplyShiftProbabilityForUCGGroup(ucgGroup);
        }
    }
  }

  public UCGState(UCGRule ucgRule, Set<UCGGroup> ucgGroups, Date evaluationDate,Double targetRatio)
  {
    this(ucgRule,ucgGroups,evaluationDate);
    this.targetRatio = targetRatio;
  }

  private double calculateTargetRatio()
  {
    Double targetRatio = null;
    if(ucgRule.getCalculationType() == UCGRuleCalculationType.Percent)
    {
      targetRatio = ucgRule.getSize() / 100.0d;
      if(targetRatio < 0 || targetRatio > 1)
      {
        log.error("Unallowed UCG target percentage.");
        targetRatio = null;
      }
    }
    else if (ucgRule.getCalculationType() == UCGRuleCalculationType.Numeric)
    {
      int totalCustomers = 0;
      for (UCGGroup ucgGroup : ucgGroups)
      {
        //Stefan comment: should be a better way to get total subscribers.
        // Normally yes summing all ucg groups total subs should obtain total subscribers no but we have to find something more simple and sure
        totalCustomers += ucgGroup.getTotalSubscribers();
      }

      int targetSize = ucgRule.getSize();
      if (targetSize < 0)
      {
        log.error("NUMERIC number of customers wanted in UCG must not be a negative number.");
        targetRatio = null;
      }
      else if(targetSize >= totalCustomers)
      {
        targetRatio = 1.0d;
      }
      else
      {
        targetRatio = targetSize / (double) totalCustomers;
      }
    }
    else if (ucgRule.getCalculationType() == UCGRuleCalculationType.File)
    {
      log.warn("UCG engine will not have any impact in UCGRuleCalculationType.File mode.");
      targetRatio = null;
    }
    else
    {
      log.error("Unable to retrieve UCG target ratio for this UCG calculation type.");
      targetRatio = null;
    }
    return targetRatio;
  }

  public void calculateAndApplyShiftProbabilityForUCGGroup(UCGGroup ucgGroup)
  {
    double shiftProbability = 0.0d;
    if(targetRatio != null)
    {
      int userDelta = 0;
      int suitableCustomersNbr = 0;

      userDelta = (int) (ucgGroup.getTotalSubscribers() * targetRatio) - ucgGroup.getUCGSubscribers();
      if(userDelta >= 0)
      {
        suitableCustomersNbr = ucgGroup.getTotalSubscribers() - ucgGroup.getUCGSubscribers();
      }
      else
      {
        suitableCustomersNbr = ucgGroup.getUCGSubscribers();
      }


      if(suitableCustomersNbr != 0)
      {
        shiftProbability = userDelta / (double) suitableCustomersNbr;
      }
    }
    ucgGroup.setShiftProbability(shiftProbability);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    UCGState ucgState = (UCGState) value;
    Struct struct = new Struct(schema);
    struct.put("ucgRule", UCGRule.pack(ucgState.getUCGRule()));
    struct.put("ucgGroups", packUCGGroups(ucgState.getUCGGroups()));
    struct.put("evaluationDate", ucgState.getEvaluationDate());
    struct.put("targetRatio", ucgState.getTargetRatio());
    return struct;
  }

  /****************************************
  *
  *  packUCGGroups
  *
  ****************************************/

  private static List<Object> packUCGGroups(Set<UCGGroup> ucgGroups)
  {
    List<Object> result = new ArrayList<Object>();
    for (UCGGroup ucgGroup : ucgGroups)
      {
        result.add(UCGGroup.pack(ucgGroup));
      }
    return result;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static UCGState unpack(SchemaAndValue schemaAndValue)
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
    UCGRule ucgRule = UCGRule.unpack(new SchemaAndValue(schema.field("ucgRule").schema(), valueStruct.get("ucgRule")));
    Set<UCGGroup> ucgGroups = unpackUCGGroups(schema.field("ucgGroups").schema(), valueStruct.get("ucgGroups"));
    Date evaluationDate = (Date) valueStruct.get("evaluationDate");
    Double targetRatio = valueStruct.getFloat64("targetRatio");
    
    //
    //  return
    //

    return new UCGState(ucgRule, ucgGroups, evaluationDate,targetRatio);
  }

  /*****************************************
  *
  *  unpackUCGGroups
  *
  *****************************************/

  private static Set<UCGGroup> unpackUCGGroups(Schema schema, Object value)
  {
    //
    //  get schema for ucgGroup
    //

    Schema ucgGroupSchema = schema.valueSchema();

    //
    //  unpack
    //

    Set<UCGGroup> result = new HashSet<UCGGroup>();
    List<Object> valueArray = (List<Object>) value;
    for (Object ucgGroup : valueArray)
      {
        result.add(UCGGroup.unpack(new SchemaAndValue(ucgGroupSchema, ucgGroup)));
      }

    //
    //  return
    //

    return result;
  }

  /*****************************************
  *
  *  class UCGGroup
  *
  *****************************************/

  public static class UCGGroup
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
      schemaBuilder.name("ucg_group");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
      schemaBuilder.field("segmentIDs", SchemaBuilder.array(Schema.STRING_SCHEMA));
      schemaBuilder.field("ucgSubscribers", Schema.INT32_SCHEMA);
      schemaBuilder.field("totalSubscribers", Schema.INT32_SCHEMA);
      schemaBuilder.field("shiftProbability", Schema.OPTIONAL_FLOAT64_SCHEMA);
      schema = schemaBuilder.build();
    };

    //
    //  serde
    //

    private static ConnectSerde<UCGGroup> serde = new ConnectSerde<UCGGroup>(schema, false, UCGGroup.class, UCGGroup::pack, UCGGroup::unpack);

    //
    //  accessor
    //

    public static Schema schema() { return schema; }
    public static ConnectSerde<UCGGroup> serde() { return serde; }

    /****************************************
    *
    *  data
    *
    ****************************************/

    private Set<String> segmentIDs;
    private int ucgSubscribers;
    private int totalSubscribers;
    private Double shiftProbability;
    

    /****************************************
    *
    *  accessors
    *
    ****************************************/

    public Set<String> getSegmentIDs() { return segmentIDs; }
    public int getUCGSubscribers() { return ucgSubscribers; }
    public int getTotalSubscribers() { return totalSubscribers; }
    public Double getShiftProbability() { return shiftProbability; }
    
    //
    // setters
    //
    
    public void setShiftProbability(Double p) { this.shiftProbability = p; }
    public void setUCGSubscribers(int ucgSubscribers) { this.ucgSubscribers = ucgSubscribers; }
    //methods for increment and decrement ucg subscribers size
    //normally one method can suppli both operations but for a better code readability 2 methods were created
    public void incrementUCGSubscribers(int numberOfSubscribers) { this.ucgSubscribers += numberOfSubscribers; }
    public void decrementUCGSubscribers(int numberOfSubscribers) { this.ucgSubscribers = this.ucgSubscribers - numberOfSubscribers; }


    /*****************************************
    *
    *  constructor
    *
    *****************************************/

    public UCGGroup(Set<String> segmentIDs, int ucgSubscribers, int totalSubscribers)
    {
      this.segmentIDs = segmentIDs;
      this.ucgSubscribers = ucgSubscribers;
      this.totalSubscribers = totalSubscribers;
      this.shiftProbability = null;
    }
    
    /*****************************************
    *
    *  constructor "unpack"
    *
    *****************************************/
    
    private UCGGroup(Set<String> segmentIDs, int ucgSubscribers, int totalSubscribers, Double shiftProbability)
    {
      this.segmentIDs = segmentIDs;
      this.ucgSubscribers = ucgSubscribers;
      this.totalSubscribers = totalSubscribers;
      this.shiftProbability = shiftProbability;
    }

    /*****************************************
    *
    *  pack
    *
    *****************************************/

    public static Object pack(Object value)
    {
      UCGGroup ucgGroup = (UCGGroup) value;
      Struct struct = new Struct(schema);
      struct.put("segmentIDs", packSegmentIDs(ucgGroup.getSegmentIDs()));
      struct.put("ucgSubscribers", ucgGroup.getUCGSubscribers());
      struct.put("totalSubscribers" , ucgGroup.getTotalSubscribers());
      struct.put("shiftProbability" , ucgGroup.getShiftProbability());
      return struct;
    }

    /****************************************
    *
    *  packSegmentIDs
    *
    ****************************************/

    private static List<Object> packSegmentIDs(Set<String> segmentIDs)
    {
      List<Object> result = new ArrayList<Object>();
      for (String segmentID : segmentIDs)
        {
          result.add(segmentID);
        }
      return result;
    }

    /*****************************************
    *
    *  unpack
    *
    *****************************************/

    public static UCGGroup unpack(SchemaAndValue schemaAndValue)
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
      Set<String> segmentIDs = unpackSegmentIDs((List<String>) valueStruct.get("segmentIDs"));
      int ucgSubscribers = valueStruct.getInt32("ucgSubscribers");
      int totalSubscribers = valueStruct.getInt32("totalSubscribers");
      Double shiftProbability = valueStruct.getFloat64("shiftProbability");

      //
      //  return
      //

      return new UCGGroup(segmentIDs, ucgSubscribers, totalSubscribers, shiftProbability);
    }

    /*****************************************
    *
    *  unpackSegmentIDs
    *
    *****************************************/

    private static Set<String> unpackSegmentIDs(List<String> segmentIDs)
    {
      Set<String> result = new LinkedHashSet<String>();
      for (String segmentID : segmentIDs)
        {
          result.add(segmentID);
        }
      return result;
    }
  }
}
