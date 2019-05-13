package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
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

public class TimeWindow
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
      schemaBuilder.name("time_windows");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(1));
      schemaBuilder.field("monday", SchemaBuilder.array(DailyWindow.schema()).schema());
      schemaBuilder.field("tuesday", SchemaBuilder.array(DailyWindow.schema()).schema());
      schemaBuilder.field("wednesday", SchemaBuilder.array(DailyWindow.schema()).schema());
      schemaBuilder.field("thursday", SchemaBuilder.array(DailyWindow.schema()).schema());
      schemaBuilder.field("friday", SchemaBuilder.array(DailyWindow.schema()).schema());
      schemaBuilder.field("saturday", SchemaBuilder.array(DailyWindow.schema()).schema());
      schemaBuilder.field("sunday", SchemaBuilder.array(DailyWindow.schema()).schema());
      schema = schemaBuilder.build();
    };

    //
    //  serde
    //

    private static ConnectSerde<TimeWindow> serde = new ConnectSerde<TimeWindow>(schema, false, TimeWindow.class, TimeWindow::pack, TimeWindow::unpack);

    //
    //  accessor
    //

    public static Schema schema() { return schema; }
    public static ConnectSerde<TimeWindow> serde() { return serde; }

    /*****************************************
    *
    *  data
    *
    *****************************************/

    private List<DailyWindow> monday;
    private List<DailyWindow> tuesday;
    private List<DailyWindow> wednesday;
    private List<DailyWindow> thursday;
    private List<DailyWindow> friday;
    private List<DailyWindow> saturday;
    private List<DailyWindow> sunday;
    
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

    public TimeWindow(List<DailyWindow> monday, List<DailyWindow> tuesday, List<DailyWindow> wednesday, List<DailyWindow> thursday, List<DailyWindow> friday, List<DailyWindow> saturday, List<DailyWindow> sunday)
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
      TimeWindow timeWindow = (TimeWindow) value;
      Struct struct = new Struct(schema);
      struct.put("monday", packDailyWindows(timeWindow.getDailyWindowMonday()));
      struct.put("tuesday", packDailyWindows(timeWindow.getDailyWindowMonday()));
      struct.put("wednesday", packDailyWindows(timeWindow.getDailyWindowMonday()));
      struct.put("thursday", packDailyWindows(timeWindow.getDailyWindowMonday()));
      struct.put("friday", packDailyWindows(timeWindow.getDailyWindowMonday()));
      struct.put("saturday", packDailyWindows(timeWindow.getDailyWindowMonday()));
      struct.put("sunday", packDailyWindows(timeWindow.getDailyWindowMonday()));
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

    public static TimeWindow unpack(SchemaAndValue schemaAndValue)
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
      
      List<DailyWindow> dailyWindowsMonday = unpackDailyWindows(schema.field("monday").schema(), valueStruct.get("monday"));
      List<DailyWindow> dailyWindowsTuesday = unpackDailyWindows(schema.field("tuesday").schema(), valueStruct.get("tuesday"));
      List<DailyWindow> dailyWindowsWednesday = unpackDailyWindows(schema.field("wednesday").schema(), valueStruct.get("wednesday"));
      List<DailyWindow> dailyWindowsThursday = unpackDailyWindows(schema.field("thursday").schema(), valueStruct.get("thursday"));
      List<DailyWindow> dailyWindowsFriday = unpackDailyWindows(schema.field("friday").schema(), valueStruct.get("friday"));
      List<DailyWindow> dailyWindowsSaturday = unpackDailyWindows(schema.field("saturday").schema(), valueStruct.get("saturday"));
      List<DailyWindow> dailyWindowsSunday = unpackDailyWindows(schema.field("sunday").schema(), valueStruct.get("sunday"));

      //
      //  return
      //

      return new TimeWindow(dailyWindowsMonday, dailyWindowsTuesday, dailyWindowsWednesday, dailyWindowsThursday, dailyWindowsFriday, dailyWindowsSaturday, dailyWindowsSunday);
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

    public TimeWindow(JSONObject jsonRoot) throws GUIManagerException
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
      List<DailyWindow> dailyWindows = new ArrayList<DailyWindow>();
      for (int i=0; i<jsonArray.size(); i++)
        {
          dailyWindows.add(new DailyWindow((JSONObject) jsonArray.get(i)));
        }
      return dailyWindows;
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
        schemaBuilder.field("from", Schema.INT64_SCHEMA);
        schemaBuilder.field("until", Schema.INT64_SCHEMA);
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

      public DailyWindow(Date from, Date until)
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
        struct.put("from", dailyWindow.getStartTime().getTime());
        struct.put("until", dailyWindow.getEndTime().getTime());
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
        long stTime = valueStruct.getInt64("from");
        long edTime = valueStruct.getInt64("until");
        
        Date from = new Date(stTime);
        Date until = new Date(edTime);

        //
        //  return
        //

        return new DailyWindow(from, until);
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
        String stTime = JSONUtilities.decodeString(jsonRoot, "from", true);
        String edTime = JSONUtilities.decodeString(jsonRoot, "until", true);
        Calendar cal = SystemTime.getCalendar();
        String[] splitStart = stTime.split(":");
        String[] splitEnd = edTime.split(":");
        if(splitStart.length == 2) {
          cal.set(Calendar.HOUR, Integer.valueOf(splitStart[0]));
          cal.set(Calendar.MINUTE, Integer.valueOf(splitStart[1]));
        }
        this.from = cal.getTime();
        if(splitEnd.length == 2) {
          cal.set(Calendar.HOUR, Integer.valueOf(splitEnd[0]));
          cal.set(Calendar.MINUTE, Integer.valueOf(splitEnd[1]));
        }
        this.until = cal.getTime();
      }
    }
    
}
