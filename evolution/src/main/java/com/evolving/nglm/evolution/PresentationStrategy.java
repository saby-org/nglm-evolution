/*****************************************************************************
*
*  PresentationStrategy.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.SchemaUtilities;

import com.rii.utilities.JSONUtilities;
import com.rii.utilities.JSONUtilities.JSONUtilitiesException;
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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;

public class PresentationStrategy extends GUIManagedObject
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
    schemaBuilder.name("presentation_strategy");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("callingChannels", SchemaBuilder.array(Schema.STRING_SCHEMA));
    schemaBuilder.field("maximumPresentations", Schema.INT32_SCHEMA);
    schemaBuilder.field("maximumPresentationsPeriodDays", Schema.INT32_SCHEMA);
    schemaBuilder.field("targetGroupPositions", SchemaBuilder.array(PresentationPosition.schema()).schema());
    schemaBuilder.field("controlGroupPositions", SchemaBuilder.array(PresentationPosition.schema()).schema());
    schemaBuilder.field("scoringStrategies", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).name("presentation_strategy_scoringstrategies").schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<PresentationStrategy> serde = new ConnectSerde<PresentationStrategy>(schema, false, PresentationStrategy.class, PresentationStrategy::pack, PresentationStrategy::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<PresentationStrategy> serde() { return serde; }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private Set<CallingChannel> callingChannels;
  private Integer maximumPresentations;
  private Integer maximumPresentationsPeriodDays;
  private List<PresentationPosition> targetGroupPositions;
  private List<PresentationPosition> controlGroupPositions;
  private Map<OfferType,String> scoringStrategies;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  //
  //  public
  //

  public String getPresentationStrategyID() { return getGUIManagedObjectID(); }
  public Set<CallingChannel> getCallingChannels() { return callingChannels; }
  public Integer getMaximumPresentations() { return maximumPresentations; }
  public Integer getMaximumPresentationsPeriodDays() { return maximumPresentationsPeriodDays; }
  public List<PresentationPosition> getTargetGroupPositions() { return targetGroupPositions; }
  public List<PresentationPosition> getControlGroupPositions() { return controlGroupPositions; }
  public Map<OfferType,String> getScoringStrategies() { return scoringStrategies; }

  /*****************************************
  *
  *  getSummaryJSONRepresentation
  *
  *****************************************/

  @Override JSONObject getSummaryJSONRepresentation()
  {
    JSONObject jsonRepresentation = new JSONObject();
    jsonRepresentation.putAll(getJSONRepresentation());
    jsonRepresentation.remove("targetGroupPositions");
    jsonRepresentation.remove("controlGroupPositions");
    jsonRepresentation.remove("scoringStrategies");
    return jsonRepresentation;
  }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public PresentationStrategy(SchemaAndValue schemaAndValue, Set<CallingChannel> callingChannels, Integer maximumPresentations, Integer maximumPresentationsPeriodDays, List<PresentationPosition> targetGroupPositions, List<PresentationPosition> controlGroupPositions, Map<OfferType,String> scoringStrategies)
  {
    super(schemaAndValue);
    this.callingChannels = callingChannels;
    this.maximumPresentations = maximumPresentations;
    this.maximumPresentationsPeriodDays = maximumPresentationsPeriodDays;
    this.targetGroupPositions = targetGroupPositions;
    this.controlGroupPositions = controlGroupPositions;
    this.scoringStrategies = scoringStrategies;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    PresentationStrategy presentationStrategy = (PresentationStrategy) value;
    Struct struct = new Struct(schema);
    packCommon(struct, presentationStrategy);
    struct.put("callingChannels", packCallingChannels(presentationStrategy.getCallingChannels()));
    struct.put("maximumPresentations", presentationStrategy.getMaximumPresentations());
    struct.put("maximumPresentationsPeriodDays", presentationStrategy.getMaximumPresentationsPeriodDays());
    struct.put("targetGroupPositions", packPresentationPositions(presentationStrategy.getTargetGroupPositions()));
    struct.put("controlGroupPositions", packPresentationPositions(presentationStrategy.getControlGroupPositions()));
    struct.put("scoringStrategies", packScoringStrategies(presentationStrategy.getScoringStrategies()));
    return struct;
  }

  /****************************************
  *
  *  packCallingChannels
  *
  ****************************************/

  private static List<Object> packCallingChannels(Set<CallingChannel> callingChannels)
  {
    List<Object> result = new ArrayList<Object>();
    for (CallingChannel callingChannel : callingChannels)
      {
        result.add(callingChannel.getID());
      }
    return result;
  }

  /****************************************
  *
  *  packPresentationPositions
  *
  ****************************************/

  private static List<Object> packPresentationPositions(List<PresentationPosition> presentationPositions)
  {
    List<Object> result = new ArrayList<Object>();
    for (PresentationPosition presentationPosition : presentationPositions)
      {
        result.add(PresentationPosition.pack(presentationPosition));
      }
    return result;
  }

  /****************************************
  *
  *  packScoringStrategies
  *
  ****************************************/

  private static Map<String,String> packScoringStrategies(Map<OfferType,String> scoringStrategies)
  {
    Map<String,String> result = new LinkedHashMap<String,String>();
    for (OfferType offerType : scoringStrategies.keySet())
      {
        String scoringStrategy = scoringStrategies.get(offerType);
        result.put(offerType.getID(), scoringStrategy);
      }
    return result;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static PresentationStrategy unpack(SchemaAndValue schemaAndValue)
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
    Set<CallingChannel> callingChannels = unpackCallingChannels((List<String>) valueStruct.get("callingChannels"));
    Integer maximumPresentations = valueStruct.getInt32("maximumPresentations");
    Integer maximumPresentationsPeriodDays = valueStruct.getInt32("maximumPresentationsPeriodDays");
    List<PresentationPosition> targetGroupPositions = unpackPresentationPositions(schema.field("targetGroupPositions").schema(), valueStruct.get("targetGroupPositions"));
    List<PresentationPosition> controlGroupPositions = unpackPresentationPositions(schema.field("controlGroupPositions").schema(), valueStruct.get("controlGroupPositions"));
    Map<OfferType,String> scoringStrategies = unpackScoringStrategies((Map<String,String>) valueStruct.get("scoringStrategies"));
    
    //
    //  return
    //

    return new PresentationStrategy(schemaAndValue, callingChannels, maximumPresentations, maximumPresentationsPeriodDays, targetGroupPositions, controlGroupPositions, scoringStrategies);
  }
  
  /*****************************************
  *
  *  unpackCallingChannels
  *
  *****************************************/

  private static Set<CallingChannel> unpackCallingChannels(List<String> callingChannelNames)
  {
    Set<CallingChannel> callingChannels = new LinkedHashSet<CallingChannel>();
    for (String callingChannelName : callingChannelNames)
      {
        CallingChannel callingChannel = Deployment.getCallingChannels().get(callingChannelName);
        if (callingChannel == null) throw new SerializationException("unknown callingChannel: " + callingChannelName);
        callingChannels.add(callingChannel);
      }
    return callingChannels;
  }

  /*****************************************
  *
  *  unpackPresentationPositions
  *
  *****************************************/

  private static List<PresentationPosition> unpackPresentationPositions(Schema schema, Object value)
  {
    //
    //  get schema for PresentationPosition
    //

    Schema presentationPositionSchema = schema.valueSchema();
    
    //
    //  unpack
    //

    List<PresentationPosition> result = new ArrayList<PresentationPosition>();
    List<Object> valueArray = (List<Object>) value;
    for (Object presentationPosition : valueArray)
      {
        result.add(PresentationPosition.unpack(new SchemaAndValue(presentationPositionSchema, presentationPosition)));
      }

    //
    //  return
    //

    return result;
  }

  /*****************************************
  *
  *  unpackScoringStrategies
  *
  *****************************************/

  private static Map<OfferType,String> unpackScoringStrategies(Map<String,String> scoringStrategies)
  {
    Map<OfferType,String> result  = new LinkedHashMap<OfferType,String>();
    for (String offerTypeName : scoringStrategies.keySet())
      {
        OfferType offerType = Deployment.getOfferTypes().get(offerTypeName);
        if (offerType == null) throw new SerializationException("unknown offerType: " + offerTypeName);
        String scoringStrategy = scoringStrategies.get(offerType);
        result.put(offerType, scoringStrategy);
      }
    return result;
  }

  /*****************************************
  *
  *  constructor -- JSON
  *
  *****************************************/

  public PresentationStrategy(JSONObject jsonRoot, long epoch, GUIManagedObject existingPresentationStrategyUnchecked) throws GUIManagerException
  {
    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingPresentationStrategyUnchecked != null) ? existingPresentationStrategyUnchecked.getEpoch() : epoch);

    /*****************************************
    *
    *  existingPresentationStrategy
    *
    *****************************************/

    PresentationStrategy existingPresentationStrategy = (existingPresentationStrategyUnchecked != null && existingPresentationStrategyUnchecked instanceof PresentationStrategy) ? (PresentationStrategy) existingPresentationStrategyUnchecked : null;

    /*****************************************
    *
    *  attributes
    *
    *****************************************/

    this.callingChannels = decodeCallingChannels(JSONUtilities.decodeJSONArray(jsonRoot, "callingChannelIDs", true));
    this.maximumPresentations = JSONUtilities.decodeInteger(jsonRoot, "maximumPresentations", false);
    this.maximumPresentationsPeriodDays = JSONUtilities.decodeInteger(jsonRoot, "maximumPresentationsPeriodDays", false);
    this.targetGroupPositions = decodePresentationPositions(JSONUtilities.decodeJSONArray(jsonRoot, "targetGroupPositions", true));
    this.controlGroupPositions = decodePresentationPositions(JSONUtilities.decodeJSONArray(jsonRoot, "controlGroupPositions", true));
    this.scoringStrategies = decodeScoringStrategies(JSONUtilities.decodeJSONArray(jsonRoot, "scoringStrategies", true));

    /*****************************************
    *
    *  epoch
    *
    *****************************************/

    if (epochChanged(existingPresentationStrategy))
      {
        this.setEpoch(epoch);
      }
  }

  /*****************************************
  *
  *  decodeCallingChannels
  *
  *****************************************/

  private Set<CallingChannel> decodeCallingChannels(JSONArray jsonArray) throws GUIManagerException
  {
    Set<CallingChannel> callingChannels = new LinkedHashSet<CallingChannel>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        String callingChannelName = (String) jsonArray.get(i);
        CallingChannel callingChannel = Deployment.getCallingChannels().get(callingChannelName);
        if (callingChannel == null) throw new GUIManagerException("unknown callingChannel", callingChannelName);
        callingChannels.add(callingChannel);
      }
    return callingChannels;
  }

  /*****************************************
  *
  *  decodePresentationPositions
  *
  *****************************************/

  private List<PresentationPosition> decodePresentationPositions(JSONArray jsonArray) throws GUIManagerException
  {
    List<PresentationPosition> result = new ArrayList<PresentationPosition>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        result.add(new PresentationPosition((JSONObject) jsonArray.get(i)));
      }
    return result;
  }
  
  /*****************************************
  *
  *  decodeScoringStrategies
  *
  *****************************************/

  private Map<OfferType,String> decodeScoringStrategies(JSONArray jsonArray) throws GUIManagerException
  {
    Map<OfferType,String> result = new LinkedHashMap<OfferType,String>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        JSONObject jsonObject = (JSONObject) jsonArray.get(i);
        String offerTypeID = JSONUtilities.decodeString(jsonObject, "offerTypeID", true);
        OfferType offerType = Deployment.getOfferTypes().get(offerTypeID);
        if (offerType == null) throw new GUIManagerException("unknown offerType", offerTypeID);
        String scoringStrategyID = JSONUtilities.decodeString(jsonObject, "scoringStrategyID", true);
        result.put(offerType, scoringStrategyID);
      }
    return result;
  }

  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(PresentationStrategy existingPresentationStrategy)
  {
    if (existingPresentationStrategy != null && existingPresentationStrategy.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingPresentationStrategy.getGUIManagedObjectID());
        epochChanged = epochChanged || ! Objects.equals(callingChannels, existingPresentationStrategy.getCallingChannels());
        epochChanged = epochChanged || ! Objects.equals(maximumPresentations, existingPresentationStrategy.getMaximumPresentations());
        epochChanged = epochChanged || ! Objects.equals(maximumPresentationsPeriodDays, existingPresentationStrategy.getMaximumPresentationsPeriodDays());
        epochChanged = epochChanged || ! Objects.equals(targetGroupPositions, existingPresentationStrategy.getTargetGroupPositions());
        epochChanged = epochChanged || ! Objects.equals(controlGroupPositions, existingPresentationStrategy.getControlGroupPositions());
        epochChanged = epochChanged || ! Objects.equals(scoringStrategies, existingPresentationStrategy.getScoringStrategies());
        return epochChanged;
      }
    else
      {
        return true;
      }
  }

  /*****************************************
  *
  *  validateScoringStrategies
  *
  *****************************************/

  public void validateScoringStrategies(ScoringStrategyService scoringStrategyService, Date date) throws GUIManagerException
  {
    for (String scoringStrategyID : scoringStrategies.values())
      {
        if (scoringStrategyService.getActiveScoringStrategy(scoringStrategyID, date) == null) throw new GUIManagerException("unknown scoring strategy", scoringStrategyID);
      }
  }
}
