package com.evolving.nglm.evolution;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.core.UniqueKeyServer;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class CommunicationChannelTimeWindow extends GUIManagedObject
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
      schemaBuilder.name("channel_time_windows");
      schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),2));
      for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
     
      schemaBuilder.field("monday", SchemaBuilder.array(DailyWindow.schema()).optional().schema());
      schemaBuilder.field("tuesday", SchemaBuilder.array(DailyWindow.schema()).optional().schema());
      schemaBuilder.field("wednesday", SchemaBuilder.array(DailyWindow.schema()).optional().schema());
      schemaBuilder.field("thursday", SchemaBuilder.array(DailyWindow.schema()).optional().schema());
      schemaBuilder.field("friday", SchemaBuilder.array(DailyWindow.schema()).optional().schema());
      schemaBuilder.field("saturday", SchemaBuilder.array(DailyWindow.schema()).optional().schema());
      schemaBuilder.field("sunday", SchemaBuilder.array(DailyWindow.schema()).optional().schema());
      schemaBuilder.field("communicationChannelID", Schema.STRING_SCHEMA);
      
      schema = schemaBuilder.build();
    };

    //
    //  serde
    //

    private static ConnectSerde<CommunicationChannelTimeWindow> serde = new ConnectSerde<CommunicationChannelTimeWindow>(schema, false, CommunicationChannelTimeWindow.class, CommunicationChannelTimeWindow::pack, CommunicationChannelTimeWindow::unpack);

    //
    //  accessor
    //

    public static Schema schema() { return schema; }
    public static ConnectSerde<CommunicationChannelTimeWindow> serde() { return serde; }

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
    private String communicationChannelID = null;
    
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
    public String getCommunicationChannelID() { return communicationChannelID; };

    /*****************************************
    *
    *  constructor -- unpack
    *
    *****************************************/

    public CommunicationChannelTimeWindow(SchemaAndValue schemaAndValue, List<DailyWindow> monday, List<DailyWindow> tuesday, List<DailyWindow> wednesday, List<DailyWindow> thursday, List<DailyWindow> friday, List<DailyWindow> saturday, List<DailyWindow> sunday, String communicationChannelID)
    {
      super(schemaAndValue);
      this.monday = monday;
      this.tuesday = tuesday;
      this.wednesday = wednesday;
      this.thursday = thursday;
      this.friday = friday;
      this.saturday = saturday;
      this.sunday = sunday;
      this.communicationChannelID = communicationChannelID;
    }
    
    /*****************************************
    *
    *  pack
    *
    *****************************************/

    public static Object pack(Object value)
    {
      CommunicationChannelTimeWindow timeWindow = (CommunicationChannelTimeWindow) value;
      Struct struct = new Struct(schema);
      
      packCommon(struct, timeWindow);
      
      if(timeWindow.getDailyWindowMonday() != null) struct.put("monday", packDailyWindows(timeWindow.getDailyWindowMonday()));
      if(timeWindow.getDailyWindowTuesday() != null) struct.put("tuesday", packDailyWindows(timeWindow.getDailyWindowTuesday()));
      if(timeWindow.getDailyWindowWednesday() != null) struct.put("wednesday", packDailyWindows(timeWindow.getDailyWindowWednesday()));
      if(timeWindow.getDailyWindowThursday() != null) struct.put("thursday", packDailyWindows(timeWindow.getDailyWindowThursday()));
      if(timeWindow.getDailyWindowFriday() != null) struct.put("friday", packDailyWindows(timeWindow.getDailyWindowFriday()));
      if(timeWindow.getDailyWindowSaturday() != null) struct.put("saturday", packDailyWindows(timeWindow.getDailyWindowSaturday()));
      if(timeWindow.getDailyWindowSunday() != null) struct.put("sunday", packDailyWindows(timeWindow.getDailyWindowSunday()));
      struct.put("communicationChannelID", timeWindow.getCommunicationChannelID());      
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

    public static CommunicationChannelTimeWindow unpack(SchemaAndValue schemaAndValue)
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
      
      String communicationChannelID = valueStruct.getString("communicationChannelID");
     
      //
      //  return
      //

      return new CommunicationChannelTimeWindow(schemaAndValue, dailyWindowsMonday, dailyWindowsTuesday, dailyWindowsWednesday, dailyWindowsThursday, dailyWindowsFriday, dailyWindowsSaturday, dailyWindowsSunday, communicationChannelID);
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

    public CommunicationChannelTimeWindow(JSONObject jsonRoot, long epoch, GUIManagedObject existingTimeWindowUnchecked, int tenantID) throws GUIManagerException
    {
      
      /*****************************************
      *
      *  super
      *
      *****************************************/

      super(jsonRoot, (existingTimeWindowUnchecked != null) ? existingTimeWindowUnchecked.getEpoch() : epoch, tenantID);
      
      
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
      this.communicationChannelID = JSONUtilities.decodeString(jsonRoot, "communicationChannelID", true);
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
      json.put("communicationChannelID", communicationChannelID);
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
      public Date getFromDate(Date day, int tenantID) { return convertToDate(day, from, tenantID); }
      public Date getUntilDate(Date day, int tenantID) { return RLMDateUtils.addMinutes(convertToDate(day, until, tenantID), 1); }

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
        //
        //  parse
        //

        this.from = JSONUtilities.decodeString(jsonRoot, "from", false);
        this.until = JSONUtilities.decodeString(jsonRoot, "until", false);

        //
        //  validate
        //

        if (from != null && from.split(":").length != 2) throw new GUIManagerException("invalid time in daily window", from);
        if (until != null && until.split(":").length != 2) throw new GUIManagerException("invalid time in daily window", until);
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
      
      /*****************************************
      *
      *  convertToDate
      *
      *****************************************/

      private Date convertToDate(Date day, String time, int tenantID)
      {
        Date result = day;
        String[] splitTime = time.split(":");
        if (splitTime.length != 2) throw new ServerRuntimeException("bad daily window time field");
        result = RLMDateUtils.setField(result, Calendar.HOUR_OF_DAY, Integer.valueOf(splitTime[0]), Deployment.getDeployment(tenantID).getTimeZone());
        result = RLMDateUtils.setField(result, Calendar.MINUTE, Integer.valueOf(splitTime[1]), Deployment.getDeployment(tenantID).getTimeZone());
        return result;
      }
    }
    
    
    
    /*****************************************
    *
    *  processGetTimeWindowPeriodsList
    *
    *****************************************/

    public static JSONObject processGetChannelTimeWindowList(String userID, JSONObject jsonRoot, boolean fullDetails, boolean includeArchived, CommunicationChannelTimeWindowService communicationChannelTimeWindowService, int tenantID)
    {
      /*****************************************
      *
      *  retrieve time window list
      *
      *****************************************/

      Date now = SystemTime.getCurrentTime();
      List<JSONObject> communicationChannelTimeWindowList = new ArrayList<JSONObject>();
      Collection <GUIManagedObject> communicationChannelTimeWindowObjects = new ArrayList<GUIManagedObject>();
      
      if (jsonRoot.containsKey("ids"))
        {
          JSONArray communicationChannelTimeWindowIDs = JSONUtilities.decodeJSONArray(jsonRoot, "ids");
          for (int i = 0; i < communicationChannelTimeWindowIDs.size(); i++)
            {
              String communicationChannelTimeWindowID = communicationChannelTimeWindowIDs.get(i).toString();
              GUIManagedObject communicationChannelTimeWindow = communicationChannelTimeWindowService.getStoredCommunicationChannelTimeWindow(communicationChannelTimeWindowID, includeArchived);
              if (communicationChannelTimeWindow != null)
                {
                  communicationChannelTimeWindowObjects.add(communicationChannelTimeWindow);
                }            
            }
        }
      else
        {
          communicationChannelTimeWindowObjects = communicationChannelTimeWindowService.getStoredCommunicationChannelTimeWindows(includeArchived, tenantID);
        }
      for (GUIManagedObject timeWindows : communicationChannelTimeWindowObjects)
        {
          JSONObject timeWindow = communicationChannelTimeWindowService.generateResponseJSON(timeWindows, fullDetails, now);
          timeWindow.put("communicationChannelID", timeWindows.getJSONRepresentation().get("communicationChannelID"));
          communicationChannelTimeWindowList.add(timeWindow);
        }

      /*****************************************
      *
      *  response
      *
      *****************************************/

      HashMap<String,Object> response = new HashMap<String,Object>();
      response.put("responseCode", "ok");
      response.put("timeWindows", JSONUtilities.encodeArray(communicationChannelTimeWindowList));
      return JSONUtilities.encodeObject(response);
    }

    /*****************************************
    *
    *  processGetTimeWindows
    *
    *****************************************/

    public static JSONObject processGetTimeWindows(String userID, JSONObject jsonRoot, boolean includeArchived, CommunicationChannelTimeWindowService communicationChannelTimeWindowService)
    {
      /****************************************
      *
      *  response
      *
      ****************************************/

      HashMap<String,Object> response = new HashMap<String,Object>();

      /****************************************
      *
      *  argument
      *
      ****************************************/

      String communicationChannelID = JSONUtilities.decodeString(jsonRoot, "communicationChannelID", true);

      /**************************************************************
      *
      *  retrieve and decorate communication channel blackout period
      *
      ***************************************************************/

      GUIManagedObject communicationChannelTimeWindowPeriod = communicationChannelTimeWindowService.getStoredCommunicationChannelTimeWindow(communicationChannelID, includeArchived);
      JSONObject communicationChannelTimeWindowPeriodJSON = communicationChannelTimeWindowService.generateResponseJSON(communicationChannelTimeWindowPeriod, true, SystemTime.getCurrentTime());

      /*****************************************
      *
      *  response
      *
      *****************************************/

      response.put("responseCode", (communicationChannelTimeWindowPeriod != null) ? "ok" : "communicationChannelTimeWindowPeriodNotFound");
      if (communicationChannelTimeWindowPeriod != null) response.put("timeWindows", communicationChannelTimeWindowPeriodJSON);
      return JSONUtilities.encodeObject(response);
    }

    /*****************************************
    *
    *  processPutTimeWindows
    *
    *****************************************/

    public static JSONObject processPutTimeWindows(String userID, JSONObject jsonRoot, CommunicationChannelTimeWindowService communicationChannelTimeWindowService, UniqueKeyServer epochServer, int tenantID) throws GUIManagerException
    {
      /****************************************
      *
      *  response
      *
      ****************************************/

      Date now = SystemTime.getCurrentTime();
      HashMap<String,Object> response = new HashMap<String,Object>();
      Boolean dryRun = false;
      

      /*****************************************
      *
      *  dryRun
      *
      *****************************************/
      if (jsonRoot.containsKey("dryRun")) {
        dryRun = JSONUtilities.decodeBoolean(jsonRoot, "dryRun", false);
      }

      /*****************************************
      *
      *  CommunicationChannelTimeWindowsID
      *
      *****************************************/

      String communicationChannelID = JSONUtilities.decodeString(jsonRoot, "communicationChannelID", false);
      if (communicationChannelID == null)
      {
        throw new GUIManagerException("No communication channel ID provided", "");
      }

     String timeWindowID=communicationChannelTimeWindowService.generateCommunicationChannelTimeWindowID(communicationChannelID);
    
     //inject id property
      jsonRoot.put("id", timeWindowID);
 
          
  
      /*****************************************
      *
      *  existing CommunicationChannelTimeWindows
      *
      *****************************************/

      GUIManagedObject existingCommunicationChannelTimeWindows = communicationChannelTimeWindowService.getStoredCommunicationChannelTimeWindow(communicationChannelID);

      /*****************************************
      *
      *  read-only
      *
      *****************************************/

      if (existingCommunicationChannelTimeWindows != null && existingCommunicationChannelTimeWindows.getReadOnly())
        {
          response.put("id", existingCommunicationChannelTimeWindows.getGUIManagedObjectID());
          response.put("accepted", existingCommunicationChannelTimeWindows.getAccepted());
          response.put("valid", existingCommunicationChannelTimeWindows.getAccepted());
          response.put("processing", communicationChannelTimeWindowService.isActiveCommunicationChannelTimeWindow(existingCommunicationChannelTimeWindows, now));
          response.put("responseCode", "failedReadOnly");
          return JSONUtilities.encodeObject(response);
        }

      /*****************************************
      *
      *  process CommunicationChannelTimeWindows
      *
      *****************************************/

      long epoch = epochServer.getKey();
      try
        {
          /****************************************
          *
          *  instantiate CommunicationChannelTimeWindows
          *
          ****************************************/

          CommunicationChannelTimeWindow communicationChannelTimeWindowPeriod = new CommunicationChannelTimeWindow(jsonRoot, epoch, existingCommunicationChannelTimeWindows, tenantID);

          /*****************************************
          *
          *  store
          *
          *****************************************/
          if (!dryRun)
            {

              communicationChannelTimeWindowService.putCommunicationChannelTimeWindow(communicationChannelTimeWindowPeriod,
                  (existingCommunicationChannelTimeWindows == null), userID);
            }
          /*****************************************
          *
          *  response
          *
          *****************************************/

          response.put("id", communicationChannelTimeWindowPeriod.getGUIManagedObjectID());
          response.put("accepted", communicationChannelTimeWindowPeriod.getAccepted());
          response.put("valid", communicationChannelTimeWindowPeriod.getAccepted());
          response.put("processing", communicationChannelTimeWindowService.isActiveCommunicationChannelTimeWindow(communicationChannelTimeWindowPeriod, now));
          response.put("responseCode", "ok");
          return JSONUtilities.encodeObject(response);
        }
      catch (JSONUtilitiesException|GUIManagerException e)
        {
          //
          //  incompleteObject
          //

          IncompleteObject incompleteObject = new IncompleteObject(jsonRoot, epoch, tenantID);

          //
          //  store
          //
          if (!dryRun)
            {

              communicationChannelTimeWindowService.putCommunicationChannelTimeWindow(incompleteObject,
                  (existingCommunicationChannelTimeWindows == null), userID);
            }
          //
          //  log
          //

          StringWriter stackTraceWriter = new StringWriter();
          e.printStackTrace(new PrintWriter(stackTraceWriter, true));
          log.warn("Exception processing REST api: {}", stackTraceWriter.toString());

          //
          //  response
          //

          response.put("id", incompleteObject.getGUIManagedObjectID());
          response.put("responseCode", "communicationChannelTimeWindowPeriodNotValid");
          response.put("responseMessage", e.getMessage());
          response.put("responseParameter", (e instanceof GUIManagerException) ? ((GUIManagerException) e).getResponseParameter() : null);
          return JSONUtilities.encodeObject(response);
        }
    }

    /*****************************************
    *
    *  processRemoveTimeWindows
    *
    *****************************************/

    public static JSONObject processRemoveTimeWindows(String userID, JSONObject jsonRoot, CommunicationChannelTimeWindowService communicationChannelTimeWindowService, int tenantID)
    {
      /****************************************
      *
      *  response
      *
      ****************************************/

      HashMap<String,Object> response = new HashMap<String,Object>();

      String responseCode = "";
      String singleIDresponseCode = "";
      List<CommunicationChannelTimeWindow> timeWindows = new ArrayList<>();
      List<String> validIDs = new ArrayList<>();
        JSONArray communicationChannelIDS = new JSONArray();

      /****************************************
      *
      *  argument
      *
      ****************************************/

      boolean force = JSONUtilities.decodeBoolean(jsonRoot, "force", Boolean.FALSE);
      //
      //remove single communicationChannel
      //

    
      if (jsonRoot.containsKey("communicationChannelID"))
        {
          String communicationChannelID = JSONUtilities.decodeString(jsonRoot, "communicationChannelID", false);
          communicationChannelIDS.add(communicationChannelID);
          
          CommunicationChannelTimeWindow timeWindow =(CommunicationChannelTimeWindow) communicationChannelTimeWindowService.getStoredCommunicationChannelTimeWindow(communicationChannelID);

          if (timeWindow != null && (force || !timeWindow.getReadOnly()))
            singleIDresponseCode = "ok";
          else if (timeWindow != null)
            singleIDresponseCode = "failedReadOnly";
          else

            singleIDresponseCode = "existingTimeWindowPeriodNotFound";
        }

      //
      // multiple deletion
      //
      
      if (jsonRoot.containsKey("communicationChannelIDs"))
        {
          communicationChannelIDS = JSONUtilities.decodeJSONArray(jsonRoot, "communicationChannelIDs", false);
        }
      
      for (int i = 0; i < communicationChannelIDS.size(); i++)
        {
          String communicationChannelID = communicationChannelIDS.get(i).toString();
          CommunicationChannelTimeWindow timeWindow = (CommunicationChannelTimeWindow) communicationChannelTimeWindowService.getStoredCommunicationChannelTimeWindow(communicationChannelID);
          if (timeWindow != null && (force || !timeWindow.getReadOnly()))
            {
              timeWindows.add(timeWindow);
              validIDs.add(communicationChannelID);
            }
        }
          
    

      /*****************************************
      *
      *  remove
      *
      *****************************************/
      for (int i = 0; i < timeWindows.size(); i++)
        {

          CommunicationChannelTimeWindow existingTimeWindowPeriod = timeWindows.get(i);

          communicationChannelTimeWindowService
              .removeCommunicationChannelTimeWindow(existingTimeWindowPeriod.getCommunicationChannelID(), userID, tenantID);

        }

      /*****************************************
       *
       * response
       *
       *****************************************/
      response.put("removedExistingTimeWindowPeriodIDS", JSONUtilities.encodeArray(validIDs));

      /*****************************************
       *
       * responseCode
       *
       *****************************************/

      if (jsonRoot.containsKey("id"))
        {
          response.put("responseCode", singleIDresponseCode);
          return JSONUtilities.encodeObject(response);
        }

      else
        {
          response.put("responseCode", "ok");
        }

      return JSONUtilities.encodeObject(response);
    }

    
    
}
