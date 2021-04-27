package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;

/*****************************************
*
*  JourneyScheduler
*
*****************************************/

public class JourneyScheduler
{
  /*****************************************
  *
  *  schema
  *
  *****************************************/
  
  private static Schema schema = null;
  static
  {
    SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    schemaBuilder.name("journey_scheduler");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(2));
    schemaBuilder.field("numberOfOccurrences", Schema.INT32_SCHEMA);
    schemaBuilder.field("runEveryDuration", Schema.INT32_SCHEMA);
    schemaBuilder.field("runEveryUnit", Schema.STRING_SCHEMA);
    schemaBuilder.field("runEveryWeekDay", SchemaBuilder.array(Schema.STRING_SCHEMA));
    schemaBuilder.field("runEveryMonthDay", SchemaBuilder.array(Schema.STRING_SCHEMA));
    schema = schemaBuilder.build();
  };
  
  //
  //  serde
  //

  private static ConnectSerde<JourneyScheduler> serde = new ConnectSerde<JourneyScheduler>(schema, false, JourneyScheduler.class, JourneyScheduler::pack, JourneyScheduler::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<JourneyScheduler> serde() { return serde; }
  
  /*****************************************
  *
  *  data
  *
  *****************************************/

  //
  //  basic
  //

  private Integer numberOfOccurrences;
  private Integer runEveryDuration;
  private String runEveryUnit;
  private List<String> runEveryWeekDay = new ArrayList<String>();
  private List<String> runEveryMonthDay = new ArrayList<String>();
  
  /*****************************************
  *
  *  accessors
  *
  *****************************************/
  
  public Integer getNumberOfOccurrences() { return numberOfOccurrences; }
  public Integer getRunEveryDuration() { return runEveryDuration; }
  public String getRunEveryUnit() { return runEveryUnit; }
  public List<String> getRunEveryWeekDay() { return runEveryWeekDay; }
  public List<String> getRunEveryMonthDay() { return runEveryMonthDay; }
  
  /*****************************************
  *
  *  constructor -- json
  *
  *****************************************/

  public JourneyScheduler(JSONObject jsonObject)
  {
    this.numberOfOccurrences = JSONUtilities.decodeInteger(jsonObject, "numberOfOccurrences", true);
    this.runEveryDuration = JSONUtilities.decodeInteger(jsonObject, "runEveryDuration", true);
    this.runEveryUnit = JSONUtilities.decodeString(jsonObject, "runEveryUnit", true);
    JSONArray weekDays = JSONUtilities.decodeJSONArray(jsonObject, "runEveryWeekDay", new JSONArray());
    for (int i=0; i<weekDays.size(); i++)
      {
        this.runEveryWeekDay.add((String) weekDays.get(i));
      }
    JSONArray monthDays = JSONUtilities.decodeJSONArray(jsonObject, "runEveryMonthDay", new JSONArray());
    for (int i=0; i<monthDays.size(); i++)
      {
        this.runEveryMonthDay.add((String) monthDays.get(i));
      }
  }
  
  /*****************************************
  *
  *  constructor -- standard
  *
  *****************************************/
  
  public JourneyScheduler(Integer numberOfOccurrences, Integer runEveryDuration, String runEveryUnit, List<String> runEveryWeekDay, List<String> runEveryMonthDay)
  {
    this.numberOfOccurrences = numberOfOccurrences;
    this.runEveryDuration = runEveryDuration;
    this.runEveryUnit = runEveryUnit;
    this.runEveryWeekDay = runEveryWeekDay;
    this.runEveryMonthDay = runEveryMonthDay;
  }
  
  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static JourneyScheduler unpack(SchemaAndValue schemaAndValue)
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
    Integer numberOfOccurrences = valueStruct.getInt32("numberOfOccurrences");
    Integer runEveryDuration = valueStruct.getInt32("runEveryDuration");
    String runEveryUnit = valueStruct.getString("runEveryUnit");
    List<String> runEveryWeekDay = (List<String>) valueStruct.get("runEveryWeekDay");
    List<String> runEveryMonthDay = (List<String>) valueStruct.get("runEveryMonthDay");
    
    return new JourneyScheduler(numberOfOccurrences, runEveryDuration, runEveryUnit, runEveryWeekDay, runEveryMonthDay);
  }
  
  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    JourneyScheduler journeyScheduler = (JourneyScheduler) value;
    Struct struct = new Struct(schema);
    struct.put("numberOfOccurrences", journeyScheduler.getNumberOfOccurrences());
    struct.put("runEveryDuration", journeyScheduler.getRunEveryDuration());
    struct.put("runEveryUnit", journeyScheduler.getRunEveryUnit());
    struct.put("runEveryWeekDay", journeyScheduler.getRunEveryWeekDay());
    struct.put("runEveryMonthDay", journeyScheduler.getRunEveryMonthDay());
    return struct;
  }
  
  @Override
  public boolean equals(Object obj)
  {
    boolean result = false;
    if (obj instanceof JourneyScheduler)
      {
        JourneyScheduler scheduler = (JourneyScheduler) obj;
        result = Objects.equals(getNumberOfOccurrences(), scheduler.getNumberOfOccurrences());
        result = result && Objects.equals(getRunEveryDuration(), scheduler.getRunEveryDuration());
        result = result && Objects.equals(getRunEveryUnit(), scheduler.getRunEveryUnit());
        result = result && Objects.equals(getRunEveryUnit(), scheduler.getRunEveryUnit());
        result = result && Objects.equals(getRunEveryMonthDay(), scheduler.getRunEveryMonthDay());
      }
    return result;
  }
}
