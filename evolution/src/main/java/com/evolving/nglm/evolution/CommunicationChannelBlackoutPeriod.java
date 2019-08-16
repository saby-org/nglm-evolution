package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;

public class CommunicationChannelBlackoutPeriod extends GUIManagedObject
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
    schemaBuilder.name("communication_channel_blackout");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("blackoutPeriods", SchemaBuilder.array(BlackoutPeriods.schema()).schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<CommunicationChannelBlackoutPeriod> serde = new ConnectSerde<CommunicationChannelBlackoutPeriod>(schema, false, CommunicationChannelBlackoutPeriod.class, CommunicationChannelBlackoutPeriod::pack, CommunicationChannelBlackoutPeriod::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<CommunicationChannelBlackoutPeriod> serde() { return serde; }

  /*****************************************
   *
   *  data
   *
   *****************************************/
  
  private Set<BlackoutPeriods> blackoutPeriods;

  /*****************************************
   *
   *  accessors
   *
   *****************************************/
  public Set<BlackoutPeriods> getBlackoutPeriodsList(){ return blackoutPeriods; }

  /*****************************************
   *
   *  constructor -- unpack
   *
   *****************************************/

  public CommunicationChannelBlackoutPeriod(SchemaAndValue schemaAndValue, Set<BlackoutPeriods> blackoutPeriods)
  {
    super(schemaAndValue);
    this.blackoutPeriods = blackoutPeriods;
  }

  /*****************************************
   *
   *  pack
   *
   *****************************************/

  public static Object pack(Object value)
  {
    CommunicationChannelBlackoutPeriod communicationChannelBlackout = (CommunicationChannelBlackoutPeriod) value;
    Struct struct = new Struct(schema);
    packCommon(struct, communicationChannelBlackout);
    struct.put("blackoutPeriods", packCommunicationChannelBlackout(communicationChannelBlackout.getBlackoutPeriodsList()));
    return struct;
  }
  
  /****************************************
  *
  *  packCommunicationChannelBlackout
  *
  ****************************************/

  private static List<Object> packCommunicationChannelBlackout(Set<BlackoutPeriods> blackoutPolicy)
  {
    List<Object> result = new ArrayList<Object>();
    for (BlackoutPeriods blackout : blackoutPolicy)
      {
        result.add(BlackoutPeriods.pack(blackout));
      }
    return result;
  }

  /*****************************************
   *
   *  unpack
   *
   *****************************************/

  public static CommunicationChannelBlackoutPeriod unpack(SchemaAndValue schemaAndValue)
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
    Set<BlackoutPeriods> blackoutList = unpackBlackoutPeriods(schema.field("blackoutPeriods").schema(), valueStruct.get("blackoutPeriods"));
   
    //
    //  return
    //

    return new CommunicationChannelBlackoutPeriod(schemaAndValue, blackoutList);
  }
  
  /*****************************************
  *
  *  unpackContactPolicyBlackout
  *
  *****************************************/

  private static Set<BlackoutPeriods> unpackBlackoutPeriods(Schema schema, Object value)
  {
    //
    //  get schema for BlackoutPeriods
    //

    Schema blackoutSchema = schema.valueSchema();

    //
    //  unpack
    //

    Set<BlackoutPeriods> result = new HashSet<BlackoutPeriods>();
    List<Object> valueArray = (List<Object>) value;
    for (Object blackout : valueArray)
      {
        result.add(BlackoutPeriods.unpack(new SchemaAndValue(blackoutSchema, blackout)));
      }

    //
    //  return
    //

    return result;
  }

  /*****************************************
   *
   *  constructor
   *
   *****************************************/

  public CommunicationChannelBlackoutPeriod(JSONObject jsonRoot, long epoch, GUIManagedObject existingBlackoutPeriodUnchecked) throws GUIManagerException
  {

    /*****************************************
    *
    *  super
    *
    *****************************************/

    super(jsonRoot, (existingBlackoutPeriodUnchecked != null) ? existingBlackoutPeriodUnchecked.getEpoch() : epoch);

    /*****************************************
    *
    *  existingCommunicationChannelBlackout
    *
    *****************************************/

    CommunicationChannelBlackoutPeriod existingContactPolicy = (existingBlackoutPeriodUnchecked != null && existingBlackoutPeriodUnchecked instanceof CommunicationChannelBlackoutPeriod) ? (CommunicationChannelBlackoutPeriod) existingBlackoutPeriodUnchecked : null;
    
    /*****************************************
    *
    *  attributes
    *
    *****************************************/
    
    this.blackoutPeriods = decodeBlackoutPeriods(JSONUtilities.decodeJSONArray(jsonRoot, "blackoutPeriods", false));
    
    /*****************************************
    *
    *  epoch
    *
    *****************************************/

    if (epochChanged(existingContactPolicy))
      {
        this.setEpoch(epoch);
      }
  }
  
  /*****************************************
  *
  *  decodeContactPolicyBlackout
  *
  *****************************************/

  private Set<BlackoutPeriods> decodeBlackoutPeriods(JSONArray jsonArray) throws GUIManagerException
  {
    Set<BlackoutPeriods> result = new HashSet<BlackoutPeriods>();
    if (jsonArray != null)
      {
        for (int i=0; i<jsonArray.size(); i++)
          {
            result.add(new BlackoutPeriods((JSONObject) jsonArray.get(i)));
          }
      }
    return result;
  }
  
  /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(CommunicationChannelBlackoutPeriod existingCommunicationChannelBlackout)
  {
    if (existingCommunicationChannelBlackout != null && existingCommunicationChannelBlackout.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingCommunicationChannelBlackout.getGUIManagedObjectID());
        return epochChanged;
      }
    else
      {
        return true;
      }
  }

  public static class BlackoutPeriods
  { 
    //
    //  logger
    //

    private static final Logger log = LoggerFactory.getLogger(BlackoutPeriods.class);

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
      schemaBuilder.name("blackout_periods");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
      schemaBuilder.field("from", Timestamp.builder().optional().schema());
      schemaBuilder.field("until", Timestamp.builder().optional().schema());
      schema = schemaBuilder.build();
    };

    //
    //  serde
    //

    private static ConnectSerde<BlackoutPeriods> serde = new ConnectSerde<BlackoutPeriods>(schema, false, BlackoutPeriods.class, BlackoutPeriods::pack, BlackoutPeriods::unpack);

    //
    //  accessor
    //

    public static Schema schema() { return schema; }
    public static ConnectSerde<BlackoutPeriods> serde() { return serde; }

    /*****************************************
     *
     *  data
     *
     *****************************************/

    private Date from;
    private Date until;

    /*****************************************
     *
     *  accessors
     *
     *****************************************/

    public Date getStartTime() { return from; }
    public Date getEndTime() { return until; }


    /*****************************************
     *
     *  constructor -- unpack
     *
     *****************************************/

    public BlackoutPeriods(Date from, Date until)
    {
      this.from = from;
      this.until = until;
    }

    /*****************************************
     *
     *  pack
     *
     *****************************************/

    public static Object pack(Object value)
    {
      BlackoutPeriods contactPolicyBlackout = (BlackoutPeriods) value;
      Struct struct = new Struct(schema);
      struct.put("from", contactPolicyBlackout.getStartTime());
      struct.put("until", contactPolicyBlackout.getEndTime());
      return struct;
    }

    /*****************************************
     *
     *  unpack
     *
     *****************************************/

    public static BlackoutPeriods unpack(SchemaAndValue schemaAndValue)
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

      Date from = (Date) valueStruct.get("from");
      Date until = (Date) valueStruct.get("until");
      
      //
      //  return
      //

      return new BlackoutPeriods(from, until);
    }

    /*****************************************
     *
     *  constructor -- JSON
     *
     *****************************************/

    public BlackoutPeriods(JSONObject jsonRoot) throws GUIManagerException
    {

      /*****************************************
       *
       *  attributes
       *
       *****************************************/
      
      //Use the date converter from the GUI as this is used in a GUIM
      this.from = GUIManagedObject.parseDateField(JSONUtilities.decodeString(jsonRoot, "from", false));
      this.until = GUIManagedObject.parseDateField(JSONUtilities.decodeString(jsonRoot, "until", false));
    }
  }
}
