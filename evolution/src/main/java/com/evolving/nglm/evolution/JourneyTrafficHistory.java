/*****************************************************************************
*
*  JourneyTrafficHistory.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.ReferenceDataValue;
import com.evolving.nglm.core.SchemaUtilities;

public class JourneyTrafficHistory implements ReferenceDataValue<String>
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
    schemaBuilder.name("JourneyTrafficHistory");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(0));
    schemaBuilder.field("journeyID", Schema.STRING_SCHEMA);
    schemaBuilder.field("lastUpdateDate", Timestamp.SCHEMA);
    schemaBuilder.field("lastArchivedDataDate", Timestamp.SCHEMA);
    schemaBuilder.field("archivePeriodInSeconds", Schema.INT32_SCHEMA);
    schemaBuilder.field("maxNumberOfPeriods", Schema.INT32_SCHEMA);
    schemaBuilder.field("currentData", JourneyTrafficSnapshot.schema());
    schemaBuilder.field("archivedData", SchemaBuilder.map(Schema.INT32_SCHEMA, JourneyTrafficSnapshot.schema()).name("archived_data_map").schema());
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<JourneyTrafficHistory> serde = new ConnectSerde<JourneyTrafficHistory>(schema, false, JourneyTrafficHistory.class, JourneyTrafficHistory::pack, JourneyTrafficHistory::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<JourneyTrafficHistory> serde() { return serde; }

  /****************************************
  *
  *  data
  *
  ****************************************/

  private String journeyID;
  private Date lastUpdateDate;
  private Date lastArchivedDataDate;
  private Integer archivePeriodInSeconds;
  private Integer maxNumberOfPeriods;
  private JourneyTrafficSnapshot currentData;
  private Map<Integer,JourneyTrafficSnapshot> archivedData;

  /****************************************
  *
  *  accessors
  *
  ****************************************/

  public String getJourneyID() { return journeyID; }
  public Date getLastUpdateDate() { return lastUpdateDate; }
  public Date getLastArchivedDataDate() { return lastArchivedDataDate; }
  public Integer getArchivePeriodInSeconds() { return archivePeriodInSeconds; }
  public Integer getMaxNumberOfPeriods() { return maxNumberOfPeriods; }
  public JourneyTrafficSnapshot getCurrentData() { return currentData; }
  public Map<Integer,JourneyTrafficSnapshot> getArchivedData() { return archivedData; }

  /****************************************
  *
  *  setter
  *
  ****************************************/

  public void setJourneyID(String journeyID) { this.journeyID = journeyID; }
  public void setLastUpdateDate(Date date) { lastUpdateDate = date; }
  public void setLastArchivedDataDate(Date date) { lastArchivedDataDate = date; }
  public void setArchivePeriodInSeconds(Integer integer) { archivePeriodInSeconds = integer; }
  public void setMaxNumberOfPeriods(Integer integer) { maxNumberOfPeriods = integer; }
  public void setCurrentData(JourneyTrafficSnapshot journeyTrafficByNode) { currentData = journeyTrafficByNode; }
  public void setArchivedData(Map<Integer,JourneyTrafficSnapshot> map) { archivedData = map; }

  /****************************************
  *
  *  ReferenceDataValue
  *
  ****************************************/
  
  @Override
  public String getKey()
  {
    return getJourneyID();
  }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public JourneyTrafficHistory(String journeyID, Date lastUpdateDate, Date lastArchivedDataDate, Integer archivePeriodInSeconds, Integer maxNumberOfPeriods, JourneyTrafficSnapshot currentData, Map<Integer,JourneyTrafficSnapshot> archivedData)
  {
    this.journeyID = journeyID;
    this.lastUpdateDate = lastUpdateDate;
    this.lastArchivedDataDate = lastArchivedDataDate;
    this.archivePeriodInSeconds = archivePeriodInSeconds;
    this.maxNumberOfPeriods = maxNumberOfPeriods;
    this.currentData = currentData;
    this.archivedData = archivedData;
  }
  
  /*****************************************
  *
  *  getJSONRepresentation
  *
  *****************************************/
    
  public JSONObject getJSONRepresentation()
  {
    HashMap<String,Object> json = new HashMap<String,Object>();
    json.put("journeyID", journeyID);
    json.put("lastUpdateDate", lastUpdateDate);
    json.put("lastArchivedDataDate", lastArchivedDataDate);
    json.put("archivePeriodInSeconds", archivePeriodInSeconds);
    json.put("maxNumberOfPeriods", maxNumberOfPeriods);
    json.put("currentData", currentData.getJSONRepresentation());
    json.put("archivedData", getJSONArchivedData(archivedData));
    return JSONUtilities.encodeObject(json);
  }

  /****************************************
  *
  *  getJSONArchivedData
  *
  ****************************************/

  public static JSONObject getJSONArchivedData(Map<Integer,JourneyTrafficSnapshot> javaObject)
  {
    Map<String,Object> result = new HashMap<String,Object>();
    for (Integer key : javaObject.keySet())
      {
        result.put(key.toString(), javaObject.get(key).getJSONRepresentation());
      }

    return JSONUtilities.encodeObject(result);
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    JourneyTrafficHistory obj = (JourneyTrafficHistory) value;
    Struct struct = new Struct(schema);
    struct.put("journeyID", obj.getJourneyID());
    struct.put("lastUpdateDate", obj.getLastUpdateDate());
    struct.put("lastArchivedDataDate", obj.getLastArchivedDataDate());
    struct.put("archivePeriodInSeconds", obj.getArchivePeriodInSeconds());
    struct.put("maxNumberOfPeriods", obj.getMaxNumberOfPeriods());
    struct.put("currentData", JourneyTrafficSnapshot.serde().pack(obj.getCurrentData()));
    struct.put("archivedData", packArchivedData(obj.getArchivedData()));
    return struct;
  }

  /****************************************
  *
  *  packArchivedData
  *
  ****************************************/

  private static Object packArchivedData(Map<Integer,JourneyTrafficSnapshot> javaObject)
  {
    Map<Integer,Object> result = new HashMap<Integer,Object>();
    for (Integer key : javaObject.keySet())
      {
        result.put(key, JourneyTrafficSnapshot.serde().pack(javaObject.get(key)));
      }

    return result;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static JourneyTrafficHistory unpack(SchemaAndValue schemaAndValue)
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
    Date lastUpdateDate = (Date) valueStruct.get("lastUpdateDate");
    Date lastArchivedDataDate = (Date) valueStruct.get("lastArchivedDataDate");
    Integer archivePeriodInSeconds = valueStruct.getInt32("archivePeriodInSeconds");
    Integer maxNumberOfPeriods = valueStruct.getInt32("maxNumberOfPeriods");
    JourneyTrafficSnapshot currentData = JourneyTrafficSnapshot.serde().unpack(new SchemaAndValue(schema.field("currentData").schema(), valueStruct.get("currentData")));
    Map<Integer,JourneyTrafficSnapshot> archivedData = unpackArchivedData(schema.field("archivedData").schema(), valueStruct.get("archivedData"));
    
    //
    //  return
    //

    return new JourneyTrafficHistory(journeyID, lastUpdateDate, lastArchivedDataDate, archivePeriodInSeconds, maxNumberOfPeriods, currentData, archivedData);
  }

  /****************************************
  *
  *  unpackArchivedData
  *
  ****************************************/

  private static Map<Integer,JourneyTrafficSnapshot> unpackArchivedData(Schema schema, Object value)
  {
    Schema mapSchema = schema.valueSchema();
    Map<Integer,JourneyTrafficSnapshot> result = new HashMap<Integer,JourneyTrafficSnapshot>();
    Map<Integer,Object> valueMap = (Map<Integer,Object>) value;
    for (Integer key : valueMap.keySet())
      {
        result.put(key, JourneyTrafficSnapshot.unpack(new SchemaAndValue(mapSchema, valueMap.get(key))));
      }

    return result;
  }

}