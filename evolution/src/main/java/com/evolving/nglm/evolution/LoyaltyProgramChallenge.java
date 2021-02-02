/*****************************************************************************
 *
 *  LoyaltyProgramChallenge.java
 *
 *****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.GUIManagedObject.GUIDependencyDef;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.LoyaltyProgramPoints.Tier;

@GUIDependencyDef(objectType = "loyaltyProgramChallenge", serviceClass = LoyaltyProgramService.class, dependencies = { "catalogcharacteristic" })
public class LoyaltyProgramChallenge extends LoyaltyProgram
{

  //
  // LoyaltyProgramLevelChange
  //

  public enum LoyaltyProgramLevelChange
  {
    Optin("opt-in"), Optout("opt-out"), Upgrade("upgrade"), Downgrade("downgrade"), NoChange("nochange"), Unknown("(unknown)");
    private String externalRepresentation;
    private LoyaltyProgramLevelChange(String externalRepresentation)
    {
      this.externalRepresentation = externalRepresentation;
    }

    public String getExternalRepresentation()
    {
      return externalRepresentation;
    }

    public static LoyaltyProgramLevelChange fromExternalRepresentation(String externalRepresentation)
    {
      for (LoyaltyProgramLevelChange enumeratedValue : LoyaltyProgramLevelChange.values())
        {
          if (enumeratedValue.getExternalRepresentation().equals(externalRepresentation))
            return enumeratedValue;
        }
      return Unknown;
    }
  }

  /*****************************************
   *
   * schema
   *
   *****************************************/

  //
  // schema
  //

  private static Schema schema = null;
  static
    {
      SchemaBuilder schemaBuilder = SchemaBuilder.struct();
      schemaBuilder.name("loyalty_program_challenge");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(LoyaltyProgram.commonSchema().version(), 1));
      for (Field field : LoyaltyProgram.commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
      schemaBuilder.field("createLeaderBoard", Schema.BOOLEAN_SCHEMA);
      schemaBuilder.field("recurrence", Schema.BOOLEAN_SCHEMA);
      schemaBuilder.field("recurrenceId", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("occurrenceNumber", Schema.OPTIONAL_INT32_SCHEMA);
      schemaBuilder.field("scheduler", JourneyScheduler.serde().optionalSchema());
      schemaBuilder.field("lastCreatedOccurrenceNumber", Schema.OPTIONAL_INT32_SCHEMA);
      schemaBuilder.field("tiers", SchemaBuilder.array(Tier.schema()).schema());
      schema = schemaBuilder.build();
    };

  //
  // serde
  //

  private static ConnectSerde<LoyaltyProgramChallenge> serde = new ConnectSerde<LoyaltyProgramChallenge>(schema, false, LoyaltyProgramChallenge.class, LoyaltyProgramChallenge::pack, LoyaltyProgramChallenge::unpack);

  //
  // accessor
  //

  public static Schema schema()
  {
    return schema;
  }

  public static ConnectSerde<LoyaltyProgramChallenge> serde()
  {
    return serde;
  }

  /*****************************************
   *
   * data
   *
   *****************************************/

  private boolean createLeaderBoard;
  private boolean recurrence;
  private String recurrenceId;
  private Integer occurrenceNumber;
  private JourneyScheduler journeyScheduler;
  private Integer lastCreatedOccurrenceNumber;
  private List<Tier> tiers = null;

  /*****************************************
   *
   * accessors
   *
   *****************************************/

  public boolean getCreateLeaderBoard()
  {
    return createLeaderBoard;
  }
  
  public boolean getRecurrence()
  {
    return recurrence;
  }

  public String getRecurrenceId()
  {
    return recurrenceId;
  }

  public Integer getOccurrenceNumber()
  {
    return occurrenceNumber;
  }

  public JourneyScheduler getJourneyScheduler()
  {
    return journeyScheduler;
  }

  public Integer getLastCreatedOccurrenceNumber()
  {
    return lastCreatedOccurrenceNumber;
  }

  public List<Tier> getTiers()
  {
    return tiers;
  }

  /*****************************************
   *
   * constructor -- JSON
   *
   *****************************************/

  public LoyaltyProgramChallenge(JSONObject jsonRoot, long epoch, GUIManagedObject existingLoyaltyProgramUnchecked, CatalogCharacteristicService catalogCharacteristicService) throws GUIManagerException
  {
    /*****************************************
     *
     * super
     *
     *****************************************/

    super(jsonRoot, epoch, existingLoyaltyProgramUnchecked, catalogCharacteristicService);

    /*****************************************
     *
     * existingLoyaltyProgramChallenge
     *
     *****************************************/

    LoyaltyProgramChallenge existingLoyaltyProgramChallenge = (existingLoyaltyProgramUnchecked != null && existingLoyaltyProgramUnchecked instanceof LoyaltyProgramChallenge) ? (LoyaltyProgramChallenge) existingLoyaltyProgramUnchecked : null;

    /*****************************************
     *
     * attributes
     *
     *****************************************/

    this.createLeaderBoard = JSONUtilities.decodeBoolean(jsonRoot, "createLeaderBoard", Boolean.FALSE);
    this.recurrence = JSONUtilities.decodeBoolean(jsonRoot, "recurrence", Boolean.FALSE);
    this.recurrenceId = JSONUtilities.decodeString(jsonRoot, "recurrenceId", recurrence);
    this.occurrenceNumber = JSONUtilities.decodeInteger(jsonRoot, "occurrenceNumber", recurrence);
    if (recurrence)
      this.journeyScheduler = new JourneyScheduler(JSONUtilities.decodeJSONObject(jsonRoot, "scheduler", recurrence));
    this.lastCreatedOccurrenceNumber = JSONUtilities.decodeInteger(jsonRoot, "lastCreatedOccurrenceNumber", recurrence);
    this.tiers = decodeLoyaltyProgramTiers(JSONUtilities.decodeJSONArray(jsonRoot, "tiers", true));

    /*****************************************
     *
     * epoch
     *
     *****************************************/

    if (epochChanged(existingLoyaltyProgramChallenge))
      {
        this.setEpoch(epoch);
      }
  }

  /*****************************************
   *
   * constructor -- unpack
   *
   *****************************************/

  public LoyaltyProgramChallenge(SchemaAndValue schemaAndValue, boolean createLeaderBoard, boolean recurrence, String recurrenceId, Integer occurrenceNumber, JourneyScheduler scheduler, Integer lastCreatedOccurrenceNumber, List<Tier> tiers)
  {
    super(schemaAndValue);
    this.createLeaderBoard = createLeaderBoard;
    this.recurrence = recurrence;
    this.recurrenceId = recurrenceId;
    this.occurrenceNumber = occurrenceNumber;
    this.journeyScheduler = scheduler;
    this.lastCreatedOccurrenceNumber = lastCreatedOccurrenceNumber;
    this.tiers = tiers;
  }

  /*****************************************
   *
   * pack
   *
   *****************************************/

  public static Object pack(Object value)
  {
    LoyaltyProgramChallenge loyaltyProgramChallenge = (LoyaltyProgramChallenge) value;
    Struct struct = new Struct(schema);
    LoyaltyProgram.packCommon(struct, loyaltyProgramChallenge);
    struct.put("createLeaderBoard", loyaltyProgramChallenge.getCreateLeaderBoard());
    struct.put("recurrence", loyaltyProgramChallenge.getRecurrence());
    struct.put("recurrenceId", loyaltyProgramChallenge.getRecurrenceId());
    struct.put("occurrenceNumber", loyaltyProgramChallenge.getOccurrenceNumber());
    struct.put("scheduler", JourneyScheduler.serde().packOptional(loyaltyProgramChallenge.getJourneyScheduler()));
    struct.put("lastCreatedOccurrenceNumber", loyaltyProgramChallenge.getLastCreatedOccurrenceNumber());
    struct.put("tiers", packLoyaltyProgramLevels(loyaltyProgramChallenge.getTiers()));
    return struct;
  }

  /****************************************
   *
   * packLoyaltyProgramLevels
   *
   ****************************************/

  private static List<Object> packLoyaltyProgramLevels(List<Tier> tiers)
  {
    List<Object> result = new ArrayList<Object>();
    for (Tier tier : tiers)
      {
        result.add(Tier.pack(tier));
      }
    return result;
  }

  /*****************************************
   *
   * unpack
   *
   *****************************************/

  public static LoyaltyProgramChallenge unpack(SchemaAndValue schemaAndValue)
  {
    //
    // data
    //

    Schema schema = schemaAndValue.schema();
    Object value = schemaAndValue.value();
    Integer schemaVersion = (schema != null) ? SchemaUtilities.unpackSchemaVersion2(schema.version()) : null;

    //
    // unpack
    //

    Struct valueStruct = (Struct) value;
    boolean createLeaderBoard = valueStruct.getBoolean("createLeaderBoard");
    boolean recurrence = valueStruct.getBoolean("recurrence");
    String recurrenceId = valueStruct.getString("recurrenceId");
    Integer occurrenceNumber = valueStruct.getInt32("occurrenceNumber");
    JourneyScheduler scheduler = JourneyScheduler.serde().unpackOptional(new SchemaAndValue(schema.field("scheduler").schema(), valueStruct.get("scheduler")));
    Integer lastCreatedOccurrenceNumber = valueStruct.getInt32("lastCreatedOccurrenceNumber");
    List<Tier> tiers = unpackLoyaltyProgramTiers(schema.field("tiers").schema(), valueStruct.get("tiers"));

    //
    // return
    //

    return new LoyaltyProgramChallenge(schemaAndValue, createLeaderBoard, recurrence, recurrenceId, occurrenceNumber, scheduler, lastCreatedOccurrenceNumber, tiers);
  }

  /*****************************************
   *
   * unpackLoyaltyProgramLevels
   *
   *****************************************/

  private static List<Tier> unpackLoyaltyProgramTiers(Schema schema, Object value)
  {
    //
    // get schema for LoyaltyProgramLevels
    //

    Schema propertySchema = schema.valueSchema();

    //
    // unpack
    //

    List<Tier> result = new ArrayList<Tier>();
    List<Object> valueArray = (List<Object>) value;
    for (Object property : valueArray)
      {
        result.add(Tier.unpack(new SchemaAndValue(propertySchema, property)));
      }

    //
    // return
    //

    return result;
  }

  /*****************************************
   *
   * decodeLoyaltyProgramLevels
   *
   *****************************************/

  private List<Tier> decodeLoyaltyProgramTiers(JSONArray jsonArray) throws GUIManagerException
  {
    List<Tier> result = new ArrayList<Tier>();
    if (jsonArray != null)
      {
        for (int i = 0; i < jsonArray.size(); i++)
          {
            result.add(new Tier((JSONObject) jsonArray.get(i)));
          }
      }
    return result;
  }

  /*****************************************
   *
   * epochChanged
   *
   *****************************************/

  private boolean epochChanged(LoyaltyProgramChallenge existingLoyaltyProgramChallenge)
  {
    if (existingLoyaltyProgramChallenge != null && existingLoyaltyProgramChallenge.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || !Objects.equals(getGUIManagedObjectID(), existingLoyaltyProgramChallenge.getGUIManagedObjectID());
        epochChanged = epochChanged || !Objects.equals(getGUIManagedObjectName(), existingLoyaltyProgramChallenge.getGUIManagedObjectName());
        epochChanged = epochChanged || !Objects.equals(getLoyaltyProgramType(), existingLoyaltyProgramChallenge.getLoyaltyProgramType());
        epochChanged = epochChanged || !Objects.equals(getTiers(), existingLoyaltyProgramChallenge.getTiers());
        epochChanged = epochChanged || !Objects.equals(getCharacteristics(), existingLoyaltyProgramChallenge.getCharacteristics());
        epochChanged = epochChanged || !Objects.equals(getCreateLeaderBoard(), existingLoyaltyProgramChallenge.getCreateLeaderBoard());
        epochChanged = epochChanged || !Objects.equals(getRecurrence(), existingLoyaltyProgramChallenge.getRecurrence());
        epochChanged = epochChanged || !Objects.equals(getRecurrenceId(), existingLoyaltyProgramChallenge.getRecurrenceId());
        epochChanged = epochChanged || !Objects.equals(getOccurrenceNumber(), existingLoyaltyProgramChallenge.getOccurrenceNumber());
        epochChanged = epochChanged || !Objects.equals(getJourneyScheduler(), existingLoyaltyProgramChallenge.getJourneyScheduler());
        return epochChanged;
      } else
      {
        return true;
      }
  }

  /*****************************************
   *
   * validate
   *
   *****************************************/

  @Override
  public boolean validate() throws GUIManagerException
  {
    return true;
  }
  
  /*******************************
   * 
   * getGUIDependencies
   * 
   *******************************/

  @Override
  public Map<String, List<String>> getGUIDependencies()
  {
    Map<String, List<String>> result = new HashMap<String, List<String>>();
    List<String> catalogcharacteristicIDs = new ArrayList<String>();
    if (getCharacteristics() != null)
      {
        for (CatalogCharacteristicInstance catalogCharacteristicInstance : getCharacteristics())
          {
            catalogcharacteristicIDs.add(catalogCharacteristicInstance.getCatalogCharacteristicID());
          }
      }
    result.put("catalogcharacteristic", catalogcharacteristicIDs);
    return result;
  }
}
