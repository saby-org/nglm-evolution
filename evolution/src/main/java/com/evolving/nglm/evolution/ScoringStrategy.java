/*****************************************************************************
*
*  ScoringStrategy.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.SchemaUtilities;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;

public class ScoringStrategy extends GUIManagedObject
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
    schemaBuilder.name("scoring_strategy");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("offerType", Schema.STRING_SCHEMA);
    schemaBuilder.field("scoringGroups", SchemaBuilder.array(ScoringGroup.schema()).schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<ScoringStrategy> serde = new ConnectSerde<ScoringStrategy>(schema, false, ScoringStrategy.class, ScoringStrategy::pack, ScoringStrategy::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<ScoringStrategy> serde() { return serde; }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private OfferType offerType;
  private List<ScoringGroup> scoringGroups;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  //
  //  public
  //

  public String getScoringStrategyID() { return getGUIManagedObjectID(); }
  public OfferType getOfferType() { return offerType; }
  public List<ScoringGroup> getScoringGroups() { return scoringGroups; }

  /*****************************************
  *
  *  evaluateScoringGroups
  *
  *****************************************/

  public ScoringGroup evaluateScoringGroups(SubscriberEvaluationRequest evaluationRequest)
  {
    ScoringGroup result = null;
    for (ScoringGroup scoringGroup : scoringGroups)
      {
        if (scoringGroup.evaluateProfileCriteria(evaluationRequest))
          {
            result = scoringGroup;
            break;
          }
      }
    return result;
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public ScoringStrategy(SchemaAndValue schemaAndValue, OfferType offerType, List<ScoringGroup> scoringGroups)
  {
    super(schemaAndValue);
    this.offerType = offerType;
    this.scoringGroups = scoringGroups;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    ScoringStrategy scoringStrategy = (ScoringStrategy) value;
    Struct struct = new Struct(schema);
    packCommon(struct, scoringStrategy);
    struct.put("offerType", scoringStrategy.getOfferType().getID());
    struct.put("scoringGroups", packScoringGroups(scoringStrategy.getScoringGroups()));
    return struct;
  }

  /****************************************
  *
  *  packScoringGroups
  *
  ****************************************/

  private static List<Object> packScoringGroups(List<ScoringGroup> scoringGroups)
  {
    List<Object> result = new ArrayList<Object>();
    for (ScoringGroup scoringGroup : scoringGroups)
      {
        result.add(ScoringGroup.pack(scoringGroup));
      }
    return result;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static ScoringStrategy unpack(SchemaAndValue schemaAndValue)
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
    OfferType offerType = Deployment.getOfferTypes().get(valueStruct.getString("offerType"));
    List<ScoringGroup> scoringGroups = unpackScoringGroups(schema.field("scoringGroups").schema(), valueStruct.get("scoringGroups"));
    
    //
    //  validate
    //

    if (offerType == null) throw new SerializationException("unknown offerType: " + valueStruct.getString("offerType"));    

    //
    //  return
    //

    return new ScoringStrategy(schemaAndValue, offerType, scoringGroups);
  }
  
  /*****************************************
  *
  *  unpackScoringGroups
  *
  *****************************************/

  private static List<ScoringGroup> unpackScoringGroups(Schema schema, Object value)
  {
    //
    //  get schema for ScoringGroup
    //

    Schema scoringGroupSchema = schema.valueSchema();
    
    //
    //  unpack
    //

    List<ScoringGroup> result = new ArrayList<ScoringGroup>();
    List<Object> valueArray = (List<Object>) value;
    for (Object scoringGroup : valueArray)
      {
        result.add(ScoringGroup.unpack(new SchemaAndValue(scoringGroupSchema, scoringGroup)));
      }

    //
    //  return
    //

    return result;
  }

  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public ScoringStrategy(JSONObject jsonRoot, long epoch, GUIManagedObject existingScoringStrategyUnchecked) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingScoringStrategyUnchecked != null) ? existingScoringStrategyUnchecked.getEpoch() : epoch);

    /*****************************************
    *
    *  existingScoringStrategy
    *
    *****************************************/

    ScoringStrategy existingScoringStrategy = (existingScoringStrategyUnchecked != null && existingScoringStrategyUnchecked instanceof ScoringStrategy) ? (ScoringStrategy) existingScoringStrategyUnchecked : null;

    /*****************************************
    *
    *  attributes
    *
    *****************************************/
    
    //
    //  simple
    //

    this.offerType = Deployment.getOfferTypes().get(JSONUtilities.decodeString(jsonRoot, "offerTypeID", true));

    //
    //  standard scoring groups
    //

    ScoringGroup universalControlGroup = new ScoringGroup(JSONUtilities.decodeJSONObject(jsonRoot, "universalControlGroup", true), "universalControlGroup");
    ScoringGroup controlGroup = new ScoringGroup(JSONUtilities.decodeJSONObject(jsonRoot, "controlGroup", true), "controlGroup");
    ScoringGroup defaultGroup = new ScoringGroup(JSONUtilities.decodeJSONObject(jsonRoot, "defaultGroup", true), "defaultGroup");
    if (universalControlGroup.getProfileCriteria().size() > 0) throw new GUIManagerException("invalid standard scoring group (profileCriteria)", universalControlGroup.getName());
    if (controlGroup.getProfileCriteria().size() > 0) throw new GUIManagerException("invalid standard scoring group (profileCriteria)", controlGroup.getName());
    if (defaultGroup.getProfileCriteria().size() > 0) throw new GUIManagerException("invalid standard scoring group (profileCriteria)", defaultGroup.getName());
    universalControlGroup.getProfileCriteria().addAll(Deployment.getUniversalControlGroupCriteria());
    controlGroup.getProfileCriteria().addAll(Deployment.getControlGroupCriteria());
    
    //
    //  scoringGroups
    //

    this.scoringGroups = new ArrayList<ScoringGroup>();
    this.scoringGroups.add(universalControlGroup);
    this.scoringGroups.add(controlGroup);
    this.scoringGroups.addAll(decodeScoringGroups(JSONUtilities.decodeJSONArray(jsonRoot, "targetGroups", true), "targetGroup"));
    this.scoringGroups.add(defaultGroup);

    /*****************************************
    *
    *  validate
    *
    *****************************************/

    if (this.offerType == null) throw new GUIManagerException("unsupported offerType", JSONUtilities.decodeString(jsonRoot, "offerType", true));
    if (getRawEffectiveStartDate() != null) throw new GUIManagerException("unsupported start date", JSONUtilities.decodeString(jsonRoot, "effectiveStartDate", false));
    if (getRawEffectiveEndDate() != null) throw new GUIManagerException("unsupported end date", JSONUtilities.decodeString(jsonRoot, "effectiveEndDate", false));
    for (ScoringGroup scoringGroup : this.scoringGroups)
      {
        if (scoringGroup.getScoringSplits().size() < 1) throw new GUIManagerException("invalid scoringGroup (no scoringSplits)", scoringGroup.getName());
      }

    /*****************************************
    *
    *  epoch
    *
    *****************************************/

    if (epochChanged(existingScoringStrategy))
      {
        this.setEpoch(epoch);
      }
  }

  /*****************************************
  *
  *  decodeScoringGroups
  *
  *****************************************/

  private List<ScoringGroup> decodeScoringGroups(JSONArray jsonArray, String baseName)  throws GUIManagerException, JSONUtilitiesException
  {
    List<ScoringGroup> result = new ArrayList<ScoringGroup>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        result.add(new ScoringGroup((JSONObject) jsonArray.get(i), baseName + "-" + i));
      }
    return result;
  }

  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(ScoringStrategy existingScoringStrategy)
  {
    if (existingScoringStrategy != null && existingScoringStrategy.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingScoringStrategy.getGUIManagedObjectID());
        epochChanged = epochChanged || ! Objects.equals(getOfferType(), existingScoringStrategy.getOfferType());
        epochChanged = epochChanged || ! Objects.equals(scoringGroups, existingScoringStrategy.getScoringGroups());
        return epochChanged;
      }
    else
      {
        return true;
      }
  }

  /*****************************************
  *
  *  validate
  *
  *****************************************/

  public void validate(OfferObjectiveService offerObjectiveService, Date date) throws GUIManagerException
  {
    for (ScoringGroup scoringGroup : scoringGroups)
      {
        for (ScoringSplit scoringSplit : scoringGroup.getScoringSplits())
          {
            for (String offerObjectiveID : scoringSplit.getOfferObjectiveIDs())
              {
                if (offerObjectiveService.getActiveOfferObjective(offerObjectiveID, date) == null) throw new GUIManagerException("unknown offer objective", offerObjectiveID);
              }
          }
      }
  }
}
