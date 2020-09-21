/*****************************************************************************
*
*  ScoringStrategy.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManagedObject.GUIDependencyDef;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;

@GUIDependencyDef(objectType = "scoringStrategy", serviceClass = ScoringStrategyService.class, dependencies = { "segmentationdimensioneligibility","segmentationdimensionfileimport","segmentationdimensioneligibility", "offerobjective"})
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
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),2));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("scoringEngineID", SchemaBuilder.string().defaultValue("").schema());
    schemaBuilder.field("dimensionID", Schema.STRING_SCHEMA);
    schemaBuilder.field("scoringSegments", SchemaBuilder.array(ScoringSegment.schema()).schema());
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

  private ScoringEngine scoringEngine;
  private String dimensionID;
  private List<ScoringSegment> scoringSegments;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  //
  //  public
  //

  public String getScoringStrategyID() { return getGUIManagedObjectID(); }
  public ScoringEngine getScoringEngine() { return scoringEngine; }
  public String getDimensionID() { return dimensionID; }
  public List<ScoringSegment> getScoringSegments() { return scoringSegments; }

  /*****************************************
  *
  *  evaluateScoringSegments
  *
  *****************************************/
  
  public ScoringSegment evaluateScoringSegments(SubscriberEvaluationRequest evaluationRequest)
  {
    ScoringSegment result = null;
    SubscriberProfile sp = evaluationRequest.getSubscriberProfile();
    Map<String, String> segments = sp.getSegmentsMap(evaluationRequest.getSubscriberGroupEpochReader());
    String segment = segments.get(dimensionID);
    for (ScoringSegment scoringSegment : scoringSegments)
      {
        if (scoringSegment.getSegmentIDs().contains(segment)
            || scoringSegment.getSegmentIDs().isEmpty()) // Case of the last segment : matches everyone
          {
            result = scoringSegment;
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

  public ScoringStrategy(SchemaAndValue schemaAndValue, ScoringEngine scoringEngine, String dimensionID, List<ScoringSegment> scoringSegments)
  {
    super(schemaAndValue);
    this.scoringEngine = scoringEngine;
    this.dimensionID = dimensionID;
    this.scoringSegments = scoringSegments;
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
    struct.put("scoringEngineID", scoringStrategy.getScoringEngine().getID());
    struct.put("dimensionID", scoringStrategy.getDimensionID());
    struct.put("scoringSegments", packScoringSegments(scoringStrategy.getScoringSegments()));
    return struct;
  }

  /****************************************
  *
  *  packScoringSegments
  *
  ****************************************/

  private static List<Object> packScoringSegments(List<ScoringSegment> scoringSegments)
  {
    List<Object> result = new ArrayList<Object>();
    for (ScoringSegment scoringSegment : scoringSegments)
      {
        result.add(ScoringSegment.pack(scoringSegment));
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
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion1(schema.version()) : null;

    //
    //  unpack
    //

    Struct valueStruct = (Struct) value;
    ScoringEngine scoringEngine = (schemaVersion >= 2) ? Deployment.getScoringEngines().get(valueStruct.getString("scoringEngineID")) : Deployment.getScoringEngines().values().iterator().next();
    String dimensionID = valueStruct.getString("dimensionID");
    List<ScoringSegment> scoringSegments = unpackScoringSegments(schema.field("scoringSegments").schema(), valueStruct.get("scoringSegments"));
    
    //
    //  validate
    //

    if (scoringEngine == null) throw new SerializationException("unknown scoringEngine: " + valueStruct.getString("scoringEngineID"));

    //
    //  return
    //

    return new ScoringStrategy(schemaAndValue, scoringEngine, dimensionID, scoringSegments);
  }
  
  /*****************************************
  *
  *  unpackScoringSegments
  *
  *****************************************/

  private static List<ScoringSegment> unpackScoringSegments(Schema schema, Object value)
  {
    //
    //  get schema for ScoringSegment
    //

    Schema scoringSegmentSchema = schema.valueSchema();
    
    //
    //  unpack
    //

    List<ScoringSegment> result = new ArrayList<>();
    List<Object> valueArray = (List<Object>) value;
    for (Object scoringSegment : valueArray)
      {
        result.add(ScoringSegment.unpack(new SchemaAndValue(scoringSegmentSchema, scoringSegment)));
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
    
    this.scoringEngine = Deployment.getScoringEngines().get(JSONUtilities.decodeString(jsonRoot, "scoringEngineID", true));
    this.dimensionID = JSONUtilities.decodeString(jsonRoot, "dimensionID", true);

    //
    //  scoring segments
    //
    
    scoringSegments = decodeScoringSegments(JSONUtilities.decodeJSONArray(jsonRoot, "scoringSegments", true));

    /*****************************************
    *
    *  validate
    *
    *****************************************/

    if (this.scoringEngine == null) throw new GUIManagerException("unsupported scoringEngine", JSONUtilities.decodeString(jsonRoot, "scoringEngineID", true));
    if (getRawEffectiveStartDate() != null) throw new GUIManagerException("unsupported start date", JSONUtilities.decodeString(jsonRoot, "effectiveStartDate", false));
    if (getRawEffectiveEndDate() != null) throw new GUIManagerException("unsupported end date", JSONUtilities.decodeString(jsonRoot, "effectiveEndDate", false));
    
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
  *  decodeScoringSegments
  *
  *****************************************/

  private List<ScoringSegment> decodeScoringSegments(JSONArray jsonArray)  throws GUIManagerException, JSONUtilitiesException
  {
    List<ScoringSegment> result = new ArrayList<>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        result.add(new ScoringSegment((JSONObject) jsonArray.get(i)));
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
        epochChanged = epochChanged || ! Objects.equals(getScoringEngine(), existingScoringStrategy.getScoringEngine());
        epochChanged = epochChanged || ! Objects.equals(getDimensionID(), existingScoringStrategy.getDimensionID());
        epochChanged = epochChanged || ! Objects.equals(scoringSegments, existingScoringStrategy.getScoringSegments());
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
    for (ScoringSegment scoringSegments : scoringSegments)
      {
        Set<String> allOfferObjectivesID = new HashSet<>();
        allOfferObjectivesID.addAll(scoringSegments.getOfferObjectiveIDs());
        allOfferObjectivesID.addAll(scoringSegments.getAlwaysAppendOfferObjectiveIDs());
        for (String offerObjectiveID : allOfferObjectivesID)
          {
            if (offerObjectiveService.getActiveOfferObjective(offerObjectiveID, date) == null) throw new GUIManagerException("unknown offer objective", offerObjectiveID);
          }
      }
    // TODO : validate segments
    
  }
  
  @Override public Map<String, List<String>> getGUIDependencies()
  {
    Map<String, List<String>> result = new HashMap<String, List<String>>();
    List<String> segmentationDimensionIDs = new ArrayList<>();
    List<String> allOfferObjectivesID = new ArrayList<>();
    segmentationDimensionIDs.add(getDimensionID());
    for (ScoringSegment scoringSegments : scoringSegments)
    {     
      allOfferObjectivesID.addAll(scoringSegments.getOfferObjectiveIDs());
    }
      result.put("segmentationdimensioneligibility", segmentationDimensionIDs);
      result.put("segmentationdimensionfileimport", segmentationDimensionIDs);
      result.put("segmentationdimensioneligibility", segmentationDimensionIDs);
      result.put("offerobjective", allOfferObjectivesID);
    
    return result;
  }
}
