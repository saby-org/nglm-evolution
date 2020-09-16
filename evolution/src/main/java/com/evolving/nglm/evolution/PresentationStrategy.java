/*****************************************************************************
*
*  PresentationStrategy.java
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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;

@GUIDependencyDef(objectType = "presentationStrategy", serviceClass = PresentationStrategyService.class, dependencies = { "saleschannel","scoringStrategy" })
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
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),2));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("salesChannelIDs", SchemaBuilder.array(Schema.STRING_SCHEMA));
    schemaBuilder.field("maximumPresentations", Schema.INT32_SCHEMA);
    schemaBuilder.field("maximumPresentationsPeriodDays", Schema.INT32_SCHEMA);
    schemaBuilder.field("setA", PositionSet.schema());
    schemaBuilder.field("setB", PositionSet.schema());
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

  private Set<String> salesChannelIDs;
  private Integer maximumPresentations;
  private Integer maximumPresentationsPeriodDays;
  private PositionSet setA;
  private PositionSet setB;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  //
  //  public
  //

  public String getPresentationStrategyID() { return getGUIManagedObjectID(); }
  public Set<String> getSalesChannelIDs() { return salesChannelIDs; }
  public Integer getMaximumPresentations() { return maximumPresentations; }
  public Integer getMaximumPresentationsPeriodDays() { return maximumPresentationsPeriodDays; }
  public PositionSet getSetA() { return setA; }
  public PositionSet getSetB() { return setB; }

  /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public PresentationStrategy(SchemaAndValue schemaAndValue, Set<String> salesChannelIDs, Integer maximumPresentations, Integer maximumPresentationsPeriodDays, PositionSet setA, PositionSet setB)
  {
    super(schemaAndValue);
    this.salesChannelIDs = salesChannelIDs;
    this.maximumPresentations = maximumPresentations;
    this.maximumPresentationsPeriodDays = maximumPresentationsPeriodDays;
    this.setA = setA;
    this.setB = setB;
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
    struct.put("salesChannelIDs", packSalesChannelIDs(presentationStrategy.getSalesChannelIDs()));
    struct.put("maximumPresentations", presentationStrategy.getMaximumPresentations());
    struct.put("maximumPresentationsPeriodDays", presentationStrategy.getMaximumPresentationsPeriodDays());
    struct.put("setA", PositionSet.pack(presentationStrategy.getSetA()));
    struct.put("setB", PositionSet.pack(presentationStrategy.getSetB()));
    return struct;
  }

  /****************************************
  *
  *  packSalesChannelIDs
  *
  ****************************************/

  private static List<Object> packSalesChannelIDs(Set<String> salesChannelIDs)
  {
    List<Object> result = new ArrayList<Object>();
    for (String salesChannelID : salesChannelIDs)
      {
        result.add(salesChannelID);
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
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion1(schema.version()) : null;

    //
    //  unpack
    //

    Struct valueStruct = (Struct) value;
    Set<String> salesChannelIDs = unpackSalesChannels((List<String>) valueStruct.get("salesChannelIDs"));
    Integer maximumPresentations = valueStruct.getInt32("maximumPresentations");
    Integer maximumPresentationsPeriodDays = valueStruct.getInt32("maximumPresentationsPeriodDays");
    PositionSet setA = PositionSet.unpack(new SchemaAndValue(schema.field("setA").schema(), valueStruct.get("setA")));
    PositionSet setB = PositionSet.unpack(new SchemaAndValue(schema.field("setB").schema(), valueStruct.get("setB")));
  
    //
    //  return
    //

    return new PresentationStrategy(schemaAndValue, salesChannelIDs, maximumPresentations, maximumPresentationsPeriodDays, setA, setB);
  }
  
  /*****************************************
  *
  *  unpackSalesChannelIDs
  *
  *****************************************/

  private static Set<String> unpackSalesChannels(List<String> salesChannelIDs)
  {
    Set<String> result = new LinkedHashSet<String>();
    for (String salesChannelID : salesChannelIDs)
      {
        result.add(salesChannelID);
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

    this.salesChannelIDs = decodeSalesChannelIDs(JSONUtilities.decodeJSONArray(jsonRoot, "salesChannelIDs", true));
    this.maximumPresentations = JSONUtilities.decodeInteger(jsonRoot, "maximumPresentations", false);
    this.maximumPresentationsPeriodDays = JSONUtilities.decodeInteger(jsonRoot, "maximumPresentationsPeriodDays", false);
    this.setA = new PositionSet(JSONUtilities.decodeJSONObject(jsonRoot, "setA", true));
    this.setB = new PositionSet(JSONUtilities.decodeJSONObject(jsonRoot, "setB", true));

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
  *  decodeSalesChannelIDs
  *
  *****************************************/

  private Set<String> decodeSalesChannelIDs(JSONArray jsonArray) throws GUIManagerException
  {
    Set<String> salesChannelIDs = new LinkedHashSet<String>();
    for (int i=0; i<jsonArray.size(); i++)
      {
        salesChannelIDs.add((String) jsonArray.get(i));
      }
    return salesChannelIDs;
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
        epochChanged = epochChanged || ! Objects.equals(salesChannelIDs, existingPresentationStrategy.getSalesChannelIDs());
        epochChanged = epochChanged || ! Objects.equals(maximumPresentations, existingPresentationStrategy.getMaximumPresentations());
        epochChanged = epochChanged || ! Objects.equals(maximumPresentationsPeriodDays, existingPresentationStrategy.getMaximumPresentationsPeriodDays());
        epochChanged = epochChanged || ! Objects.equals(setA, existingPresentationStrategy.getSetA());
        epochChanged = epochChanged || ! Objects.equals(setB, existingPresentationStrategy.getSetB());
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

  public void validate(ScoringStrategyService scoringStrategyService, Date date) throws GUIManagerException
  {
    Set<PositionElement> pes = new HashSet<>();
    pes.addAll(setA.getPositions());
    pes.addAll(setB.getPositions());
    for (PositionElement pe : pes)
      {
        String scoringStrategyID = pe.getScoringStrategyID();
        if (scoringStrategyService.getActiveScoringStrategy(scoringStrategyID, date) == null) throw new GUIManagerException("unknown scoring strategy", scoringStrategyID);
      }
  }
  
  @Override public Map<String, List<String>> getGUIDependencies()
  {
    Map<String, List<String>> result = new HashMap<String, List<String>>();
    result.put("saleschannel", new ArrayList<>(getSalesChannelIDs()));
    List<String> scoringStrList=new ArrayList<>();
    Set<PositionElement> pes = new HashSet<>();
    pes.addAll(setA.getPositions());
    pes.addAll(setB.getPositions());
    for (PositionElement pe : pes)
      {
        String scoringStrategyID = pe.getScoringStrategyID();
        scoringStrList.add(scoringStrategyID);
      }
    result.put("scoringStrategy", scoringStrList);
    return result;
  }
}

