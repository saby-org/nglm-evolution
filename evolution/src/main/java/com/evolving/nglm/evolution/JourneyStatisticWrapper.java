/*****************************************************************************
*
*  JourneyStatisticWrapper.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SubscriberStreamOutput;
import com.evolving.nglm.evolution.JourneyHistory.RewardHistory;
import com.evolving.nglm.core.ReferenceDataReader;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class JourneyStatisticWrapper implements SubscriberStreamOutput
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
    schemaBuilder.name("journey_statistic_wrapper");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(0));
    schemaBuilder.field("journeyID", Schema.STRING_SCHEMA);
    schemaBuilder.field("subscriberStratum", SchemaBuilder.array(Schema.STRING_SCHEMA));
    schemaBuilder.field("statusUpdated", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("lastRewards", RewardHistory.serde().optionalSchema());
    schemaBuilder.field("journeyStatistic", JourneyStatistic.serde().optionalSchema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<JourneyStatisticWrapper> serde = new ConnectSerde<JourneyStatisticWrapper>(schema, false, JourneyStatisticWrapper.class, JourneyStatisticWrapper::pack, JourneyStatisticWrapper::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<JourneyStatisticWrapper> serde() { return serde; }
  public Schema subscriberStreamEventSchema() { return schema(); }

  /*****************************************
  *
  *  data
  *  
  *  subscriberStratum is defined by a list of segments. 
  *   One segment for each dimension specified in UCG rules, and in the same position !
  *   The position of each segment is important because subscriberStratum will be used as a key and therefore must be unique.
  *
  *****************************************/

  private String journeyID;
  private List<String> subscriberStratum;
  private boolean statusUpdated;
  private RewardHistory lastRewards;
  private JourneyStatistic journeyStatistic;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getJourneyID() { return journeyID; }
  public List<String> getSubscriberStratum() { return subscriberStratum; }
  public boolean isStatusUpdated() { return statusUpdated; }
  public RewardHistory getLastRewards() { return lastRewards; }
  public JourneyStatistic getJourneyStatistic() { return journeyStatistic; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  public JourneyStatisticWrapper(String journeyID, SubscriberProfile subscriberProfile, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader, ReferenceDataReader<String,UCGState> ucgStateReader)
  {
    this.journeyID = journeyID;
    this.subscriberStratum = new ArrayList<String>();
    this.statusUpdated = false;
    this.lastRewards = null;
    this.journeyStatistic = null;
    
    //
    // Construct the user stratum from the UCG dimensions
    // Segments will be arranged in the same order than the one specified in UCG Rule
    //
    
    UCGState ucgState = ucgStateReader.get(UCGState.getSingletonKey());
    if (ucgState != null) 
      {
        List<String> dimensions = ucgState.getUCGRule().getSelectedDimensions();
        Map<String, String> subscriberGroups = subscriberProfile.getSegmentsMap(subscriberGroupEpochReader);
        
        for(String dimensionID : dimensions) 
          {
            String segmentID = subscriberGroups.get(dimensionID);
            if(segmentID != null)
              {
                this.subscriberStratum.add(segmentID);
              }
            else
              {
                // This should not happen
                throw new IllegalStateException("Required dimension (dimensionID: " + dimensionID + ") for UCG could not be found in the subscriber profile (subscriberID: " + subscriberProfile.getSubscriberID() + ").");
              }
          }
      }
    
  }
  
  /*****************************************
  *
  *  constructor (from subscriber profile)
  *  
  *  The subscriber stratum will be extracted from this subscriber profile.
  *
  *****************************************/

  public JourneyStatisticWrapper(SubscriberProfile subscriberProfile, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader, ReferenceDataReader<String,UCGState> ucgStateReader, boolean statusUpdated, JourneyStatistic journeyStatistic)
  {
    this(journeyStatistic.getJourneyID(), subscriberProfile, subscriberGroupEpochReader, ucgStateReader);
    this.statusUpdated = statusUpdated;
    this.journeyStatistic = journeyStatistic;
  }
  

  /*****************************************
  *
  *  constructor (from Reward History)
  *  
  *  The subscriber stratum will be extracted from this subscriber profile.
  *
  *****************************************/
  public JourneyStatisticWrapper(SubscriberProfile subscriberProfile, ReferenceDataReader<String,SubscriberGroupEpoch> subscriberGroupEpochReader, ReferenceDataReader<String,UCGState> ucgStateReader, RewardHistory lastRewards, String journeyID)
  {
    this(journeyID, subscriberProfile, subscriberGroupEpochReader, ucgStateReader);
    this.lastRewards = lastRewards;
  }

  /*****************************************
  *
  *  private constructor -- unpack
  *  
  *  This constructor is kept private because the position of each segment in the list is internal (retrieved from UCG rules)
  *  and must be maintained to keep the key unique.
  *
  *****************************************/
  private JourneyStatisticWrapper(String journeyID, List<String> subscriberStratum, boolean statusUpdated, RewardHistory lastRewards, JourneyStatistic journeyStatistic)
  {
    this.journeyID = journeyID;
    this.subscriberStratum = subscriberStratum;
    this.statusUpdated = statusUpdated;
    this.lastRewards = lastRewards;
    this.journeyStatistic = journeyStatistic;
  }

  /*****************************************
  *
  *  constructor -- copy
  *
  *****************************************/

  public JourneyStatisticWrapper(JourneyStatisticWrapper journeyStatisticWrapper)
  {
    //
    //  deep copy
    //
    this.journeyID = new String(journeyStatisticWrapper.getJourneyID());
    this.subscriberStratum = new ArrayList<String>();
    for(String segment : journeyStatisticWrapper.getSubscriberStratum())
      {
        this.subscriberStratum.add(segment);
      }
    this.statusUpdated = journeyStatisticWrapper.isStatusUpdated();
    this.lastRewards = null;
    if(journeyStatisticWrapper.getLastRewards() != null)
      {
        this.lastRewards = new RewardHistory(journeyStatisticWrapper.getLastRewards());
      }
    this.journeyStatistic = null;
    if(journeyStatisticWrapper.getJourneyStatistic() != null)
      {
        new JourneyStatistic(journeyStatisticWrapper.getJourneyStatistic());
      }
  }
  
  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    JourneyStatisticWrapper journeyStatisticWrapper = (JourneyStatisticWrapper) value;
    Struct struct = new Struct(schema);
    struct.put("journeyID", journeyStatisticWrapper.getJourneyID());
    struct.put("subscriberStratum", journeyStatisticWrapper.getSubscriberStratum());
    struct.put("statusUpdated", journeyStatisticWrapper.isStatusUpdated());
    struct.put("lastRewards", RewardHistory.serde().packOptional(journeyStatisticWrapper.getLastRewards()));
    struct.put("journeyStatistic", JourneyStatistic.serde().packOptional(journeyStatisticWrapper.getJourneyStatistic()));
    return struct;
  }
  
  //
  //  subscriberStreamEventPack
  //

  public Object subscriberStreamEventPack(Object value) { return pack(value); }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static JourneyStatisticWrapper unpack(SchemaAndValue schemaAndValue)
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
    String journeyID = valueStruct.getString("journeyID");
    List<String> subscriberStratum = (List<String>) valueStruct.get("subscriberStratum");
    boolean statusUpdated = valueStruct.getBoolean("statusUpdated");
    RewardHistory lastRewards = valueStruct.get("lastRewards") != null ? RewardHistory.serde().unpack(new SchemaAndValue(schema.field("lastRewards").schema(), valueStruct.get("lastRewards"))) : null;
    JourneyStatistic journeyStatistic = valueStruct.get("journeyStatistic") != null ? JourneyStatistic.serde().unpack(new SchemaAndValue(schema.field("journeyStatistic").schema(), valueStruct.get("journeyStatistic"))) : null;

    //
    //  return
    //

    return new JourneyStatisticWrapper(journeyID, subscriberStratum, statusUpdated, lastRewards, journeyStatistic);
  }
}
