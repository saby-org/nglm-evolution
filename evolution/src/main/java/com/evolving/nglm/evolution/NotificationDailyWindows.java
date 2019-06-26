package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class NotificationDailyWindows
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
      schemaBuilder.name("notification_daily_windows");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
      schemaBuilder.field("monday", SchemaBuilder.array(DailyWindow.schema()).optional().schema());
      schemaBuilder.field("tuesday", SchemaBuilder.array(DailyWindow.schema()).optional().schema());
      schemaBuilder.field("wednesday", SchemaBuilder.array(DailyWindow.schema()).optional().schema());
      schemaBuilder.field("thursday", SchemaBuilder.array(DailyWindow.schema()).optional().schema());
      schemaBuilder.field("friday", SchemaBuilder.array(DailyWindow.schema()).optional().schema());
      schemaBuilder.field("saturday", SchemaBuilder.array(DailyWindow.schema()).optional().schema());
      schemaBuilder.field("sunday", SchemaBuilder.array(DailyWindow.schema()).optional().schema());
      schema = schemaBuilder.build();
    };

    //
    //  serde
    //

    private static ConnectSerde<NotificationDailyWindows> serde = new ConnectSerde<NotificationDailyWindows>(schema, false, NotificationDailyWindows.class, NotificationDailyWindows::pack, NotificationDailyWindows::unpack);

    //
    //  accessor
    //

    public static Schema schema() { return schema; }
    public static ConnectSerde<NotificationDailyWindows> serde() { return serde; }

    /*****************************************
    *
    *  data
    *
    *****************************************/

    private List<DailyWindow> monday = null;
    private List<DailyWindow> tuesday = null;
    private List<DailyWindow> wednesday = null;
    private List<DailyWindow> thursday = null;
    private List<DailyWindow> friday = null;
    private List<DailyWindow> saturday = null;
    private List<DailyWindow> sunday = null;
    
    /*****************************************
    *
    *  accessors
    *
    *****************************************/
    
    public List<DailyWindow> getDailyWindowMonday() { return monday; }
    public List<DailyWindow> getDailyWindowTuesday() { return tuesday; }
    public List<DailyWindow> getDailyWindowWednesday() { return wednesday; }
    public List<DailyWindow> getDailyWindowThursday() { return thursday; }
    public List<DailyWindow> getDailyWindowFriday() { return friday; }
    public List<DailyWindow> getDailyWindowSaturday() { return saturday; }
    public List<DailyWindow> getDailyWindowSunday() { return sunday; }

    /*****************************************
    *
    *  constructor -- unpack
    *
    *****************************************/

    public NotificationDailyWindows(List<DailyWindow> monday, List<DailyWindow> tuesday, List<DailyWindow> wednesday, List<DailyWindow> thursday, List<DailyWindow> friday, List<DailyWindow> saturday, List<DailyWindow> sunday)
    {
      this.monday = monday;
      this.tuesday = tuesday;
      this.wednesday = wednesday;
      this.thursday = thursday;
      this.friday = friday;
      this.saturday = saturday;
      this.sunday = sunday;
    }
    
    /*****************************************
    *
    *  pack
    *
    *****************************************/

    public static Object pack(Object value)
    {
      NotificationDailyWindows timeWindow = (NotificationDailyWindows) value;
      Struct struct = new Struct(schema);
      if(timeWindow.getDailyWindowMonday() != null) struct.put("monday", packDailyWindows(timeWindow.getDailyWindowMonday()));
      if(timeWindow.getDailyWindowTuesday() != null) struct.put("tuesday", packDailyWindows(timeWindow.getDailyWindowTuesday()));
      if(timeWindow.getDailyWindowWednesday() != null) struct.put("wednesday", packDailyWindows(timeWindow.getDailyWindowWednesday()));
      if(timeWindow.getDailyWindowThursday() != null) struct.put("thursday", packDailyWindows(timeWindow.getDailyWindowThursday()));
      if(timeWindow.getDailyWindowFriday() != null) struct.put("friday", packDailyWindows(timeWindow.getDailyWindowFriday()));
      if(timeWindow.getDailyWindowSaturday() != null) struct.put("saturday", packDailyWindows(timeWindow.getDailyWindowSaturday()));
      if(timeWindow.getDailyWindowSunday() != null) struct.put("sunday", packDailyWindows(timeWindow.getDailyWindowSunday()));
      return struct;
    }
    
    /*****************************************
    *
    *  packDailyWindows
    *
    *****************************************/

    private static List<Object> packDailyWindows(List<DailyWindow> dailyWindows)
    {
      List<Object> result = new ArrayList<Object>();
      for (DailyWindow dailyWindow : dailyWindows)
        {
          result.add(DailyWindow.pack(dailyWindow));
        }
      return result;
    }
    
    /*****************************************
    *
    *  unpack
    *
    *****************************************/

    public static NotificationDailyWindows unpack(SchemaAndValue schemaAndValue)
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
      List<DailyWindow> dailyWindowsMonday = null;
      if(valueStruct.get("monday") != null) dailyWindowsMonday = unpackDailyWindows(schema.field("monday").schema(), valueStruct.get("monday"));
     
      List<DailyWindow> dailyWindowsTuesday = null;
      if(valueStruct.get("tuesday") != null) dailyWindowsTuesday = unpackDailyWindows(schema.field("tuesday").schema(), valueStruct.get("tuesday"));
     
      List<DailyWindow> dailyWindowsWednesday = null;
      if(valueStruct.get("wednesday") != null) dailyWindowsWednesday =  unpackDailyWindows(schema.field("wednesday").schema(), valueStruct.get("wednesday"));
     
      List<DailyWindow> dailyWindowsThursday = null;
      if(valueStruct.get("thursday") != null) dailyWindowsThursday = unpackDailyWindows(schema.field("thursday").schema(), valueStruct.get("thursday"));
     
      List<DailyWindow> dailyWindowsFriday = null;
      if(valueStruct.get("friday") != null) dailyWindowsFriday = unpackDailyWindows(schema.field("friday").schema(), valueStruct.get("friday"));
     
      List<DailyWindow> dailyWindowsSaturday = null;
      if(valueStruct.get("saturday") != null) dailyWindowsSaturday = unpackDailyWindows(schema.field("saturday").schema(), valueStruct.get("saturday"));
     
      List<DailyWindow> dailyWindowsSunday = null;
      if(valueStruct.get("sunday") != null) dailyWindowsSunday = unpackDailyWindows(schema.field("sunday").schema(), valueStruct.get("sunday"));
     
      //
      //  return
      //

      return new NotificationDailyWindows(dailyWindowsMonday, dailyWindowsTuesday, dailyWindowsWednesday, dailyWindowsThursday, dailyWindowsFriday, dailyWindowsSaturday, dailyWindowsSunday);
    }
    
    /*****************************************
    *
    *  unpackDailyWindows
    *
    *****************************************/

    private static List<DailyWindow> unpackDailyWindows(Schema schema, Object value)
    {
      //
      //  get schema for DailyWindows
      //

      Schema dailyWindowSchema = schema.valueSchema();

      //
      //  unpack
      //

      List<DailyWindow> result = new ArrayList<DailyWindow>();
      List<Object> valueArray = (List<Object>) value;
      for (Object dailyWindow : valueArray)
        {
          result.add(DailyWindow.unpack(new SchemaAndValue(dailyWindowSchema, dailyWindow)));
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

    public NotificationDailyWindows(JSONObject jsonRoot) throws GUIManagerException
    {
      /*****************************************
       *
       *  attributes
       *
       *****************************************/
      this.monday = decodeDailyWindows(JSONUtilities.decodeJSONArray(jsonRoot, "monday", false));
      this.tuesday = decodeDailyWindows(JSONUtilities.decodeJSONArray(jsonRoot, "tuesday", false));
      this.wednesday = decodeDailyWindows(JSONUtilities.decodeJSONArray(jsonRoot, "wednesday", false));
      this.thursday = decodeDailyWindows(JSONUtilities.decodeJSONArray(jsonRoot, "thursday", false));
      this.friday = decodeDailyWindows(JSONUtilities.decodeJSONArray(jsonRoot, "friday", false));
      this.saturday = decodeDailyWindows(JSONUtilities.decodeJSONArray(jsonRoot, "saturday", false));
      this.sunday = decodeDailyWindows(JSONUtilities.decodeJSONArray(jsonRoot, "sunday", false));
    }
    
    /*****************************************
    *
    *  decodeDailyWindow
    *
    *****************************************/

    private List<DailyWindow> decodeDailyWindows(JSONArray jsonArray) throws GUIManagerException
    {
      List<DailyWindow> dailyWindows = null;
      if(jsonArray != null) {
        dailyWindows = new ArrayList<DailyWindow>();
        for (int i=0; i<jsonArray.size(); i++)
          {
            dailyWindows.add(new DailyWindow((JSONObject) jsonArray.get(i)));
          }
      }
      return dailyWindows;
    }
    
    public boolean useDefault() {
      if(monday.isEmpty() && tuesday.isEmpty() && wednesday.isEmpty() && thursday.isEmpty() && friday.isEmpty() && saturday.isEmpty() && sunday.isEmpty()) {
        return true;
      }else {
        return false;
      }
    }
    
    /*****************************************
    *
    *  to JSONObject
    *
    *****************************************/
    
    public JSONObject getJSONRepresentation()
    {
      HashMap<String,Object> json = new HashMap<String,Object>();
      
      json.put("monday", translateDailyWindows(getDailyWindowMonday()));
      json.put("tuesday", translateDailyWindows(getDailyWindowTuesday()));
      json.put("wednesday", translateDailyWindows(getDailyWindowWednesday()));
      json.put("thursday", translateDailyWindows(getDailyWindowThursday()));
      json.put("friday", translateDailyWindows(getDailyWindowFriday()));
      json.put("saturday", translateDailyWindows(getDailyWindowSaturday()));
      json.put("sunday", translateDailyWindows(getDailyWindowSunday()));
      return JSONUtilities.encodeObject(json);
    }
    
    /*****************************************
    *
    *  Translate DailyWindows
    *
    *****************************************/

    private static List<Object> translateDailyWindows(List<DailyWindow> dailyWindows)
    {
      List<Object> result = new ArrayList<Object>();
      if(dailyWindows != null) {
        for (DailyWindow dailyWindow : dailyWindows)
          {
            result.add(dailyWindow.getJSONRepresentation());
          }
      }
      return result;
    }

    public static class DailyWindow
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
        schemaBuilder.name("daily_window");
        schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
        schemaBuilder.field("from", Schema.STRING_SCHEMA);
        schemaBuilder.field("until", Schema.STRING_SCHEMA);
        schema = schemaBuilder.build();
      };

      //
      //  serde
      //

      private static ConnectSerde<DailyWindow> serde = new ConnectSerde<DailyWindow>(schema, false, DailyWindow.class, DailyWindow::pack, DailyWindow::unpack);

      //
      //  accessor
      //

      public static Schema schema() { return schema; }
      public static ConnectSerde<DailyWindow> serde() { return serde; }

      /*****************************************
      *
      *  data
      *
      *****************************************/
      
      private String from;
      private String until;
      
      /*****************************************
      *
      *  accessors
      *
      *****************************************/

      public String getStartTime() { return from; }
      public String getEndTime() { return until; }
      

      /*****************************************
      *
      *  constructor -- unpack
      *
      *****************************************/

      public DailyWindow(String from, String until)
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
        DailyWindow dailyWindow = (DailyWindow) value;
        Struct struct = new Struct(schema);
        struct.put("from", dailyWindow.getStartTime());
        struct.put("until", dailyWindow.getEndTime());
        return struct;
      }
      
      /*****************************************
      *
      *  unpack
      *
      *****************************************/

      public static DailyWindow unpack(SchemaAndValue schemaAndValue)
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
        String stTime = valueStruct.getString("from");
        String edTime = valueStruct.getString("until");

        //
        //  return
        //

        return new DailyWindow(stTime, edTime);
      }
      
      /*****************************************
      *
      *  constructor -- JSON
      *
      *****************************************/

      public DailyWindow(JSONObject jsonRoot) throws GUIManagerException
      {
        
        /*****************************************
        *
        *  attributes
        *
        *****************************************/
        this.from = JSONUtilities.decodeString(jsonRoot, "from", false);
        this.until = JSONUtilities.decodeString(jsonRoot, "until", false);
      }
      
      /*****************************************
      *
      *  to JSONObject
      *
      *****************************************/
      
      public JSONObject getJSONRepresentation()
      {
        HashMap<String,Object> json = new HashMap<String,Object>();
        json.put("from", getStartTime());
        json.put("until", getEndTime());
        return JSONUtilities.encodeObject(json);
      }
    }
    
    public Date convertToDate(String time)
      {
        String[] splitTime = time.split(":");
        Calendar cal = SystemTime.getCalendar();
        if(splitTime.length == 2) {
          cal.set(Calendar.HOUR_OF_DAY, Integer.valueOf(splitTime[0]));
          cal.set(Calendar.MINUTE, Integer.valueOf(splitTime[1]));
        }
        return cal.getTime();
    }
}
