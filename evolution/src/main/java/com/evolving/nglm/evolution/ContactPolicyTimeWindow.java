/*****************************************************************************
*
*  ContactPolicyTimeWindow.java
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

import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Objects;

public class ContactPolicyTimeWindow
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
    schemaBuilder.name("contactpolicy_timewindow");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
    schemaBuilder.field("windowDays", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.BOOLEAN_SCHEMA).name("contactpolicy_timewindow_windowdays").schema());
    schemaBuilder.field("windowStartHour", Schema.INT32_SCHEMA);
    schemaBuilder.field("windowStartMinute", Schema.INT32_SCHEMA);
    schemaBuilder.field("windowEndHour", Schema.INT32_SCHEMA);
    schemaBuilder.field("windowEndMinute", Schema.INT32_SCHEMA);
    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<ContactPolicyTimeWindow> serde = new ConnectSerde<ContactPolicyTimeWindow>(schema, false, ContactPolicyTimeWindow.class, ContactPolicyTimeWindow::pack, ContactPolicyTimeWindow::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<ContactPolicyTimeWindow> serde() { return serde; }

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private Map<Integer, Boolean> windowDays;
  private int windowStartHour;
  private int windowStartMinute;
  private int windowEndHour;
  private int windowEndMinute;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public Map<Integer, Boolean> getWindowDays() { return windowDays; }
  public int getWindowStartHour() { return windowStartHour; }
  public int getWindowStartMinute() { return windowStartMinute; }
  public int getWindowEndHour() { return windowEndHour; }
  public int getWindowEndMinute() { return windowEndMinute; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public ContactPolicyTimeWindow(Map<Integer, Boolean> windowDays, int windowStartHour, int windowStartMinute, int windowEndHour, int windowEndMinute)
  {
    this.windowDays = windowDays;
    this.windowStartHour = windowStartHour;
    this.windowStartMinute = windowStartMinute;
    this.windowEndHour = windowEndHour;
    this.windowEndMinute = windowEndMinute;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    ContactPolicyTimeWindow contactPolicyTimeWindow = (ContactPolicyTimeWindow) value;
    Struct struct = new Struct(schema);
    struct.put("windowDays", packWindowDays(contactPolicyTimeWindow.getWindowDays()));
    struct.put("windowStartHour", contactPolicyTimeWindow.getWindowStartHour());
    struct.put("windowStartMinute", contactPolicyTimeWindow.getWindowStartMinute());
    struct.put("windowEndHour", contactPolicyTimeWindow.getWindowEndHour());
    struct.put("windowEndMinute", contactPolicyTimeWindow.getWindowEndMinute());
    return struct;
  }

  /*****************************************
  *
  *  packWindowDays
  *
  *****************************************/

  private static Map<String,Boolean> packWindowDays(Map<Integer,Boolean> windowDays)
  {
    Map<String,Boolean> result = new HashMap<String,Boolean>();
    for (Integer day : windowDays.keySet())
      {
        boolean dayValue = windowDays.get(day);
        String dayString = null;
        switch (day)
          {
            case Calendar.SUNDAY:
              result.put("sun", dayValue);
              break;
            case Calendar.MONDAY:
              result.put("mon", dayValue);
              break;
            case Calendar.TUESDAY:
              result.put("tue", dayValue);
              break;
            case Calendar.WEDNESDAY:
              result.put("wed", dayValue);
              break;
            case Calendar.THURSDAY:
              result.put("thu", dayValue);
              break;
            case Calendar.FRIDAY:
              result.put("fri", dayValue);
              break;
            case Calendar.SATURDAY:
              result.put("sat", dayValue);
              break;
          }
      }
    return result;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static ContactPolicyTimeWindow unpack(SchemaAndValue schemaAndValue)
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
    Map<Integer, Boolean> windowDays = unpackWindowDays((Map<String, Boolean>) valueStruct.get("windowDays"));
    int windowStartHour = valueStruct.getInt32("windowStartHour");
    int windowStartMinute = valueStruct.getInt32("windowStartMinute");
    int windowEndHour = valueStruct.getInt32("windowEndHour");
    int windowEndMinute = valueStruct.getInt32("windowEndMinute");

    //
    //  return
    //

    return new ContactPolicyTimeWindow(windowDays, windowStartHour, windowStartMinute, windowEndHour, windowEndMinute);
  }

  /*****************************************
  *
  *  unpackWindowDays
  *
  *****************************************/

  private static Map<Integer,Boolean> unpackWindowDays(Map<String,Boolean> windowDays)
  {
    Map<Integer,Boolean> result = new HashMap<Integer,Boolean>();
    for (String dayString : windowDays.keySet())
      {
        boolean dayValue = windowDays.get(dayString);
        Integer day = null;
        switch (dayString)
          {
            case "sun":
              result.put(Calendar.SUNDAY, dayValue);
              break;
            case "mon":
              result.put(Calendar.MONDAY, dayValue);
              break;
            case "tue":
              result.put(Calendar.TUESDAY, dayValue);
              break;
            case "wed":
              result.put(Calendar.WEDNESDAY, dayValue);
              break;
            case "thu":
              result.put(Calendar.THURSDAY, dayValue);
              break;
            case "fri":
              result.put(Calendar.FRIDAY, dayValue);
              break;
            case "sat":
              result.put(Calendar.SATURDAY, dayValue);
              break;
          }
      }
    return result;
  }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public ContactPolicyTimeWindow(JSONObject jsonRoot) throws GUIManagerException
  {
    this.windowDays = decodeWindowDays(JSONUtilities.decodeJSONObject(jsonRoot, "windowDays", true));
    this.windowStartHour = JSONUtilities.decodeInteger(jsonRoot, "windowStartHour", true);
    this.windowStartMinute = JSONUtilities.decodeInteger(jsonRoot, "windowStartMinute", true);
    this.windowEndHour = JSONUtilities.decodeInteger(jsonRoot, "windowEndHour", true);
    this.windowEndMinute = JSONUtilities.decodeInteger(jsonRoot, "windowEndMinute", true);
  }

  /*****************************************
  *
  *  decodeWindowDays
  *
  *****************************************/

  private Map<Integer,Boolean> decodeWindowDays(JSONObject jsonWindowDays) throws GUIManagerException
  {
    Map<Integer,Boolean> windowDays = new HashMap<Integer,Boolean>();
    windowDays.put(Calendar.SUNDAY,    JSONUtilities.decodeBoolean(jsonWindowDays, "sun", Boolean.FALSE));
    windowDays.put(Calendar.MONDAY,    JSONUtilities.decodeBoolean(jsonWindowDays, "mon", Boolean.FALSE));
    windowDays.put(Calendar.TUESDAY,   JSONUtilities.decodeBoolean(jsonWindowDays, "tue", Boolean.FALSE));
    windowDays.put(Calendar.WEDNESDAY, JSONUtilities.decodeBoolean(jsonWindowDays, "wed", Boolean.FALSE));
    windowDays.put(Calendar.THURSDAY,  JSONUtilities.decodeBoolean(jsonWindowDays, "thu", Boolean.FALSE));
    windowDays.put(Calendar.FRIDAY,    JSONUtilities.decodeBoolean(jsonWindowDays, "fri", Boolean.FALSE));
    windowDays.put(Calendar.SATURDAY,  JSONUtilities.decodeBoolean(jsonWindowDays, "sat", Boolean.FALSE));
    return windowDays;
  }

  /*****************************************
  *
  *  equals
  *
  *****************************************/

  public boolean equals(Object obj)
  {
    boolean result = false;
    if (obj instanceof ContactPolicyTimeWindow)
      {
        ContactPolicyTimeWindow contactPolicyTimeWindow = (ContactPolicyTimeWindow) obj;
        result = true;
        result = result && Objects.equals(windowDays, contactPolicyTimeWindow.getWindowDays());
        result = result && windowStartHour == contactPolicyTimeWindow.getWindowStartHour();
        result = result && windowStartMinute == contactPolicyTimeWindow.getWindowStartMinute();
        result = result && windowEndHour == contactPolicyTimeWindow.getWindowEndHour();
        result = result && windowEndMinute == contactPolicyTimeWindow.getWindowEndMinute();
      }
    return result;
  }
}
