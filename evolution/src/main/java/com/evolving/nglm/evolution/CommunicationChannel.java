/*****************************************************************************
*
*  CommunicationChannel.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.evolution.EvolutionUtilities.TimeUnit;
import com.evolving.nglm.evolution.CommunicationChannelTimeWindow.DailyWindow;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class CommunicationChannel extends GUIManagedObject
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
    schemaBuilder.name("communication_channel");
    schemaBuilder.version(SchemaUtilities.packSchemaVersion(commonSchema().version(),1));
    for (Field field : commonSchema().fields()) schemaBuilder.field(field.name(), field.schema());
    schemaBuilder.field("profileAddressField", Schema.STRING_SCHEMA);
    schemaBuilder.field("deliveryType", Schema.STRING_SCHEMA);
    schemaBuilder.field("parameters", SchemaBuilder.map(Schema.STRING_SCHEMA, CriterionField.schema()).name("communication_channel_parameters").schema());
    schemaBuilder.field("toolboxParameters", SchemaBuilder.map(Schema.STRING_SCHEMA, CriterionField.schema()).name("communication_channel_toolboxParameters").schema());
    schemaBuilder.field("notificationPluginClass", Schema.STRING_SCHEMA);
    schemaBuilder.field("notificationPluginConfiguration", Schema.STRING_SCHEMA);
    schemaBuilder.field("icon", Schema.STRING_SCHEMA);
    schemaBuilder.field("toolboxHeight", Schema.INT32_SCHEMA);
    schemaBuilder.field("toolboxWidth", Schema.INT32_SCHEMA);
    schemaBuilder.field("allowGuiTemplate", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("allowInLineTemplate", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("isGeneric", Schema.BOOLEAN_SCHEMA);
    schemaBuilder.field("campaignGUINodeSectionID", Schema.STRING_SCHEMA);
    schemaBuilder.field("journeyGUINodeSectionID", Schema.STRING_SCHEMA);
    schemaBuilder.field("workflowGUINodeSectionID", Schema.STRING_SCHEMA);
    schemaBuilder.field("toolboxTimeout", Schema.INT32_SCHEMA);
    schemaBuilder.field("toolboxTimeoutUnit", Schema.STRING_SCHEMA);
    schemaBuilder.field("deliveryRatePerMinute", Schema.INT32_SCHEMA);

    schema = schemaBuilder.build();
  };

  //
  //  serde
  //

  private static ConnectSerde<CommunicationChannel> serde = new ConnectSerde<CommunicationChannel>(schema, false, CommunicationChannel.class, CommunicationChannel::pack, CommunicationChannel::unpack);

  //
  //  accessor
  //

  public static Schema schema() { return schema; }
  public static ConnectSerde<CommunicationChannel> serde() { return serde; }

    /*****************************************
    *
    *  data
    *
    *****************************************/

    String id;
    String name;
    String display;
    String profileAddressField;
    String deliveryType;

    // parameters that must be into the template
    Map<String,CriterionField> parameters = new HashMap<String, CriterionField>();
    
    // paramters that must not be into the template but present into the toolbox
    Map<String,CriterionField> toolboxParameters = new HashMap<String, CriterionField>();
    String notificationPluginClass;
    JSONObject notificationPluginConfiguration; 
    String icon;
    int toolboxHeight;
    int toolboxWidth;
    boolean allowGuiTemplate;
    boolean allowInLineTemplate;
    boolean isGeneric;
    String campaignGUINodeSectionID;
    String journeyGUINodeSectionID;
    String workflowGUINodeSectionID;
    int toolboxTimeout;
    String toolboxTimeoutUnit;
    int deliveryRatePerMinute;
    // not used for old channel
    private DeliveryManagerDeclaration deliveryManagerDeclaration;

    /*****************************************
    *
    *  accessors
    *
    *****************************************/

    public String getID() { return id; }
    public String getName() { return name; }
    public String getDisplay() { return display; }
    public String getProfileAddressField() { return profileAddressField; }
    public String getDeliveryType () { return deliveryType; } 
//    public CommunicationChannelTimeWindows getNotificationDailyWindows() { return notificationDailyWindows; }
    public Map<String,CriterionField> getParameters() { return parameters; }
    public Map<String,CriterionField> getToolboxParameters() { return toolboxParameters; }
    public String getNotificationPluginClass() { return notificationPluginClass; }
    public JSONObject getNotificationPluginConfiguration() { return notificationPluginConfiguration; }
    public String getIcon() { return icon; }
    public int getToolboxHeight() { return toolboxHeight; }
    public int getToolboxWidth() { return toolboxWidth; }
    public boolean allowGuiTemplate() { return allowGuiTemplate; }
    public boolean allowInLineTemplate() { return allowInLineTemplate; }  
    public boolean isGeneric() { return isGeneric; }
    public String getCampaignGUINodeSectionID() { return campaignGUINodeSectionID; }
    public String getJourneyGUINodeSectionID() { return journeyGUINodeSectionID; }
    public String getWorkflowGUINodeSectionID() { return workflowGUINodeSectionID; }
    public int getToolboxTimeout() { return toolboxTimeout; }
    public String getToolboxTimeoutUnit() { return toolboxTimeoutUnit; }
    public int getDeliveryRatePerMinute() { return deliveryRatePerMinute; }
    public DeliveryManagerDeclaration getDeliveryManagerDeclaration() { return deliveryManagerDeclaration; }
    
    /*****************************************
    *
    *  getEffectiveDeliveryTime
    *
    *****************************************/

    public Date getEffectiveDeliveryTime(CommunicationChannelBlackoutService communicationChannelBlackoutServiceBlackout, CommunicationChannelTimeWindowService communicationChannelTimeWindowService, Date now, int tenantID)
    {
      //
      //  retrieve delivery time configuration
      //

      CommunicationChannelBlackoutPeriod blackoutPeriod = communicationChannelBlackoutServiceBlackout.getActiveCommunicationChannelBlackout("blackoutPeriod", now);

      //
      //  iterate until a valid date is found (give up after 7 days and reschedule even if not legal)
      //

      Date maximumDeliveryDate = RLMDateUtils.addDays(now, 7, Deployment.getDeployment(tenantID).getTimeZone());
      Date deliveryDate = now;
      while (deliveryDate.before(maximumDeliveryDate))
        {
          Date nextDailyWindowDeliveryDate = this.getEffectiveDeliveryTime(this, deliveryDate, communicationChannelTimeWindowService, tenantID);
          Date nextBlackoutWindowDeliveryDate = (blackoutPeriod != null) ? communicationChannelBlackoutServiceBlackout.getEffectiveDeliveryTime(blackoutPeriod.getGUIManagedObjectID(), deliveryDate) : deliveryDate;
          Date nextDeliveryDate = nextBlackoutWindowDeliveryDate.after(nextDailyWindowDeliveryDate) ? nextBlackoutWindowDeliveryDate : nextDailyWindowDeliveryDate;
          if (nextDeliveryDate.after(deliveryDate))
            deliveryDate = nextDeliveryDate;
          else
            break;
        }

      //
      //  resolve
      //

      return deliveryDate;
    }

   /*****************************************
  *
  *  constructor -- unpack
  *
  *****************************************/

  public CommunicationChannel(SchemaAndValue schemaAndValue, String profileAddressField, String deliveryType, Map<String, CriterionField> parameters, Map<String, CriterionField> toolboxParameters, String notificationPluginClass, String notificationPluginConfiguration, String icon, int toolboxHeight, int toolboxWidth, boolean allowGuiTemplate, boolean allowInLineTemplate, boolean isGeneric, String campaignGUINodeSectionID, String journeyGUINodeSectionID, String workflowGUINodeSectionID, int toolboxTimeout, String toolboxTimeoutUnit, int deliveryRatePerMinute)
  {
    super(schemaAndValue);
    this.profileAddressField = profileAddressField;
    this.deliveryType = deliveryType;
    this.parameters = parameters;
    this.toolboxParameters = toolboxParameters;
    this.notificationPluginClass = notificationPluginClass;

    JSONParser parser = new JSONParser();
    try {
      this.notificationPluginConfiguration = (JSONObject) parser.parse(notificationPluginConfiguration);
    } catch (ParseException e) {
      throw new ServerRuntimeException("deployment", e);
    }

    this.icon = icon;
    this.toolboxHeight = toolboxHeight;
    this.toolboxWidth = toolboxWidth;
    this.allowGuiTemplate = allowGuiTemplate;
    this.allowInLineTemplate = allowInLineTemplate;
    this.isGeneric = isGeneric;
    this.campaignGUINodeSectionID = campaignGUINodeSectionID;
    this.journeyGUINodeSectionID = journeyGUINodeSectionID;
    this.workflowGUINodeSectionID = workflowGUINodeSectionID;
    this.toolboxTimeout = toolboxTimeout;
    this.toolboxTimeoutUnit = toolboxTimeoutUnit;
    this.deliveryRatePerMinute = deliveryRatePerMinute;
  }

  /*****************************************
  *
  *  pack
  *
  *****************************************/

  public static Object pack(Object value)
  {
    CommunicationChannel communicationChannel = (CommunicationChannel) value;
    Struct struct = new Struct(schema);
    packCommon(struct, communicationChannel);
    struct.put("profileAddressField", communicationChannel.getProfileAddressField());
    struct.put("deliveryType", communicationChannel.getDeliveryType());
    struct.put("parameters", packParameters(communicationChannel.getParameters()));
    struct.put("toolboxParameters", packParameters(communicationChannel.getToolboxParameters()));
    struct.put("notificationPluginClass", communicationChannel.getNotificationPluginClass());
    struct.put("notificationPluginConfiguration", communicationChannel.getNotificationPluginConfiguration().toJSONString());
    struct.put("icon", communicationChannel.getIcon());
    struct.put("toolboxHeight", communicationChannel.getToolboxHeight());
    struct.put("toolboxWidth", communicationChannel.getToolboxWidth());
    struct.put("allowGuiTemplate", communicationChannel.allowGuiTemplate());
    struct.put("allowInLineTemplate", communicationChannel.allowInLineTemplate());
    struct.put("isGeneric", communicationChannel.isGeneric());
    struct.put("campaignGUINodeSectionID", communicationChannel.getCampaignGUINodeSectionID());
    struct.put("journeyGUINodeSectionID", communicationChannel.getJourneyGUINodeSectionID());
    struct.put("workflowGUINodeSectionID", communicationChannel.getWorkflowGUINodeSectionID());
    struct.put("toolboxTimeout", communicationChannel.getToolboxTimeout());
    struct.put("toolboxTimeoutUnit", communicationChannel.getToolboxTimeoutUnit());
    struct.put("deliveryRatePerMinute", communicationChannel.getDeliveryRatePerMinute());
    return struct;
  }

  /****************************************
  *
  *  packParameters
  *
  ****************************************/

  private static Map<String, Object> packParameters(Map<String, CriterionField> parameters)
  {
    Map<String, Object> result = new HashMap<String, Object>();
    for (String key : parameters.keySet())
      {
        result.put(key, CriterionField.pack(parameters.get(key)));
      }
    return result;
  }

  /*****************************************
  *
  *  unpack
  *
  *****************************************/

  public static CommunicationChannel unpack(SchemaAndValue schemaAndValue)
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
    String profileAddressField = valueStruct.getString("profileAddressField");
    String deliveryType = valueStruct.getString("deliveryType");
    Map<String, CriterionField> parameters = unpackParameters(valueStruct.get("parameters"));
    Map<String, CriterionField> toolboxParameters = unpackParameters(valueStruct.get("toolboxParameters"));
    String notificationPluginClass = valueStruct.getString("notificationPluginClass");
    String notificationPluginConfiguration = valueStruct.getString("notificationPluginConfiguration");
    String icon = valueStruct.getString("icon");
    int toolboxHeight = valueStruct.getInt32("toolboxHeight");
    int toolboxWidth = valueStruct.getInt32("toolboxWidth");
    boolean allowGuiTemplate = valueStruct.getBoolean("allowGuiTemplate");
    boolean allowInLineTemplate = valueStruct.getBoolean("allowInLineTemplate");
    boolean isGeneric = valueStruct.getBoolean("isGeneric");
    String campaignGUINodeSectionID = valueStruct.getString("campaignGUINodeSectionID");
    String journeyGUINodeSectionID = valueStruct.getString("journeyGUINodeSectionID");
    String workflowGUINodeSectionID = valueStruct.getString("workflowGUINodeSectionID");
    int toolboxTimeout = valueStruct.getInt32("toolboxTimeout");
    String toolboxTimeoutUnit = valueStruct.getString("toolboxTimeoutUnit");
    int deliveryRatePerMinute = valueStruct.getInt32("deliveryRatePerMinute");

    //
    //  return
    //

    return new CommunicationChannel(schemaAndValue, profileAddressField, deliveryType, parameters, toolboxParameters, notificationPluginClass, notificationPluginConfiguration, icon, toolboxHeight, toolboxWidth, allowGuiTemplate, allowInLineTemplate, isGeneric, campaignGUINodeSectionID, journeyGUINodeSectionID, workflowGUINodeSectionID, toolboxTimeout, toolboxTimeoutUnit, deliveryRatePerMinute);
  }
  
  /*****************************************
  *
  *  unpackParameters
  *
  *****************************************/

  private static Map<String, CriterionField> unpackParameters(Object value)
  {
    //
    //  unpack
    //

    Map<String, CriterionField> result = new HashMap<String, CriterionField>();
    Map<String, Object> valueMap = (Map<String, Object>) value;
    for (String key : valueMap.keySet())
      {
        result.put(key, CriterionField.unpack(new SchemaAndValue(CriterionField.schema().valueSchema(), valueMap.get(key))));
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

    public CommunicationChannel(JSONObject jsonRoot, long epoch, GUIManagedObject existingCommunicationChannelUnchecked, int tenantID) throws GUIManagerException
    {
      /*****************************************
      *
      *  super
      *
      *****************************************/

      super(jsonRoot, (existingCommunicationChannelUnchecked != null) ? existingCommunicationChannelUnchecked.getEpoch() : epoch, tenantID);

      /*****************************************
      *
      *  existingCommunicationChannel
      *
      *****************************************/

      CommunicationChannel existingCommunicationChannel = (existingCommunicationChannelUnchecked != null && existingCommunicationChannelUnchecked instanceof CommunicationChannel) ? (CommunicationChannel) existingCommunicationChannelUnchecked : null;

      /*****************************************
      *
      *  attributes
      *
      *****************************************/
      this.id = JSONUtilities.decodeString(jsonRoot, "id", false);
      this.name = JSONUtilities.decodeString(jsonRoot, "name", false);
      this.display = JSONUtilities.decodeString(jsonRoot, "display", false);
      this.profileAddressField = JSONUtilities.decodeString(jsonRoot, "profileAddressField", false);
      this.deliveryType = JSONUtilities.decodeString(jsonRoot, "deliveryType", false);
      
      JSONArray templateParametersJSON = JSONUtilities.decodeJSONArray(jsonRoot, "parameters", false);
      if(templateParametersJSON != null) {
        for (int i=0; i<templateParametersJSON.size(); i++)
          {
            JSONObject parameterJSON = (JSONObject) templateParametersJSON.get(i);
            CriterionField parameter = new CriterionField(parameterJSON);
            this.parameters.put(parameter.getID(), parameter);
          }
      }
      
      JSONArray toolboxParametersJSON = JSONUtilities.decodeJSONArray(jsonRoot, "toolboxParameters", false);
      if(toolboxParametersJSON != null) {
        for (int i=0; i<toolboxParametersJSON.size(); i++)
          {
            JSONObject parameterJSON = (JSONObject) toolboxParametersJSON.get(i);
            CriterionField parameter = new CriterionField(parameterJSON);
            this.toolboxParameters.put(parameter.getID(), parameter);
          }
      }
      
      this.toolboxTimeout = JSONUtilities.decodeInteger(jsonRoot, "toolboxTimeout", 1);
      TimeUnit toolboxTimeoutUnit = TimeUnit.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "toolboxTimeoutUnit", "day"));
      if(toolboxTimeoutUnit.equals(TimeUnit.Unknown))
        {
          log.warn("CommunicationChannel: Can't interpret toolboxTimeoutUnit " + JSONUtilities.decodeString(jsonRoot, "toolboxTimeoutUnit") + " for channel " + this.getID() + " use default \"day\"");
          toolboxTimeoutUnit = TimeUnit.Day;
        }
      this.toolboxTimeoutUnit = toolboxTimeoutUnit.getExternalRepresentation();
      this.isGeneric =  JSONUtilities.decodeBoolean(jsonRoot, "isGeneric", Boolean.FALSE);
      
      this.notificationPluginConfiguration = (JSONObject)jsonRoot.get("notificationPluginConfiguration");
      this.notificationPluginClass = JSONUtilities.decodeString(jsonRoot, "notificationPluginClass", isGeneric /* Mandatory only if this is a generic channel*/);
      this.icon = JSONUtilities.decodeString(jsonRoot, "icon", false);
      this.toolboxHeight = JSONUtilities.decodeInteger(jsonRoot, "toolboxHeight", 70);
      this.toolboxWidth = JSONUtilities.decodeInteger(jsonRoot, "toolboxWidth", 70);
      
      this.allowGuiTemplate =  JSONUtilities.decodeBoolean(jsonRoot, "allowGuiTemplate", Boolean.TRUE);
      this.allowInLineTemplate =  JSONUtilities.decodeBoolean(jsonRoot, "allowInLineTemplate", Boolean.FALSE);
      this.isGeneric =  JSONUtilities.decodeBoolean(jsonRoot, "isGeneric", Boolean.FALSE);
      this.deliveryRatePerMinute = JSONUtilities.decodeInteger(jsonRoot, "deliveryRatePerMinute", Integer.MAX_VALUE);
      JSONArray subscriberProfileFields = JSONUtilities.decodeJSONArray(jsonRoot, "subscriberProfileFields", false);
      
      // deliveryManagerDeclaration derived
      if(isGeneric())
        {
          String deliveryType = "notification_"+getName();
          
          //TODO remove later, forcing conf cleaning
          if(getDeliveryType()!=null && !getDeliveryType().equals(deliveryType)) // EVPRO-99 compare to deliveryType because of multiple time this is initialized
            {
              log.error("deliveryType configuration for generic channel ("+getName()+") is not possible anymore");
              throw new ServerRuntimeException("old conf for generic channel");
            }
          // delivery type should be derived
          this.deliveryType = deliveryType;
          jsonRoot.put("deliveryType",deliveryType);
          jsonRoot.put("requestClass", NotificationManager.NotificationManagerRequest.class.getName());
          jsonRoot.put("deliveryRatePerMinute", deliveryRatePerMinute);
          jsonRoot.put("subscriberProfileFields", subscriberProfileFields);
          
          try
            {
              this.deliveryManagerDeclaration = new DeliveryManagerDeclaration(jsonRoot);
            }
          catch (NoSuchMethodException|IllegalAccessException e) {
              throw new ServerRuntimeException("deployment", e);
            }
        }
      this.journeyGUINodeSectionID = JSONUtilities.decodeString(jsonRoot, "journeyGUINodeSectionID", false);
      this.workflowGUINodeSectionID = JSONUtilities.decodeString(jsonRoot, "workflowGUINodeSectionID", false);
      this.campaignGUINodeSectionID = JSONUtilities.decodeString(jsonRoot, "campaignGUINodeSectionID", false);
      
      /*****************************************
      *
      *  epoch
      *
      *****************************************/

      if (epochChanged(existingCommunicationChannel))
      {
        this.setEpoch(epoch);
      }
    }

    public CommunicationChannel(JSONObject jsonRoot, int tenantID) throws GUIManagerException
    {
      this(jsonRoot, 0, null, tenantID);
    }
    
    public String getToolboxID() {
      return "NotifChannel-" + this.getID();
    }

    /*****************************************
  *
  *  epochChanged
  *
  *****************************************/

  private boolean epochChanged(CommunicationChannel existingCommunicationChannel)
  {
    if (existingCommunicationChannel != null && existingCommunicationChannel.getAccepted())
      {
        boolean epochChanged = false;
        epochChanged = epochChanged || ! Objects.equals(getGUIManagedObjectID(), existingCommunicationChannel.getGUIManagedObjectID());
        epochChanged = epochChanged || ! (profileAddressField == existingCommunicationChannel.getProfileAddressField());
        epochChanged = epochChanged || ! (deliveryType == existingCommunicationChannel.getDeliveryType());
        epochChanged = epochChanged || ! Objects.equals(parameters, existingCommunicationChannel.getParameters());
        epochChanged = epochChanged || ! Objects.equals(toolboxParameters, existingCommunicationChannel.getToolboxParameters());
        epochChanged = epochChanged || ! (notificationPluginClass == existingCommunicationChannel.getDeliveryType());
        epochChanged = epochChanged || ! (notificationPluginConfiguration == existingCommunicationChannel.getNotificationPluginConfiguration());
        epochChanged = epochChanged || ! (icon == existingCommunicationChannel.getIcon());
        epochChanged = epochChanged || ! (toolboxHeight == existingCommunicationChannel.getToolboxHeight());
        epochChanged = epochChanged || ! (toolboxWidth == existingCommunicationChannel.getToolboxWidth());
        epochChanged = epochChanged || ! (allowGuiTemplate == existingCommunicationChannel.allowGuiTemplate());
        epochChanged = epochChanged || ! (allowInLineTemplate == existingCommunicationChannel.allowInLineTemplate());
        epochChanged = epochChanged || ! (isGeneric == existingCommunicationChannel.isGeneric());
        epochChanged = epochChanged || ! (campaignGUINodeSectionID == existingCommunicationChannel.getCampaignGUINodeSectionID());
        epochChanged = epochChanged || ! (journeyGUINodeSectionID == existingCommunicationChannel.getJourneyGUINodeSectionID());
        epochChanged = epochChanged || ! (workflowGUINodeSectionID == existingCommunicationChannel.getWorkflowGUINodeSectionID());
        epochChanged = epochChanged || ! (toolboxTimeout == existingCommunicationChannel.getToolboxTimeout());
        epochChanged = epochChanged || ! (toolboxTimeoutUnit == existingCommunicationChannel.getToolboxTimeoutUnit());
        epochChanged = epochChanged || ! (deliveryRatePerMinute == existingCommunicationChannel.getDeliveryRatePerMinute());

        return epochChanged;
      }
    else
      {
        return true;
      }
  }

    /*****************************************
    *
    *  schedule
    *
    *****************************************/

    public Date schedule(String touchPointID, Date now)
    {
      return now;
    }

   
    public List<DailyWindow> getTodaysDailyWindows(Date now, CommunicationChannelTimeWindowService communicationChannelTimeWindowService, CommunicationChannelTimeWindow timeWindow, int tenantID)
    {
      List<DailyWindow> result = null;
      int today = RLMDateUtils.getField(now, Calendar.DAY_OF_WEEK, Deployment.getDeployment(tenantID).getTimeZone());
      if (timeWindow != null)
        {
          switch(today)
            {
              case Calendar.SUNDAY:
                result = timeWindow.getDailyWindowSunday();
                break;
              case Calendar.MONDAY:
                result = timeWindow.getDailyWindowMonday();
                break;
              case Calendar.TUESDAY:
                result = timeWindow.getDailyWindowTuesday();
                break;
              case Calendar.WEDNESDAY:
                result = timeWindow.getDailyWindowWednesday();
                break;
              case Calendar.THURSDAY:
                result = timeWindow.getDailyWindowThursday();
                break;
              case Calendar.FRIDAY:
                result = timeWindow.getDailyWindowFriday();
                break;
              case Calendar.SATURDAY:
                result = timeWindow.getDailyWindowSaturday();
                break;
            }
        }
      return (result != null) ? result : Collections.<DailyWindow>emptyList();
    }
    
    
    /*****************************************
    *
    *  getEffectiveDeliveryTime
    *
    *****************************************/
    
    private Date getEffectiveDeliveryTime(CommunicationChannel communicationChannel, Date now, CommunicationChannelTimeWindowService communicationChannelTimeWindowService, int tenantID)
    {
      Date effectiveDeliveryDate = now;
      CommunicationChannelTimeWindow timeWindow = communicationChannelTimeWindowService.getActiveCommunicationChannelTimeWindow(communicationChannel.getID(), now);
      if(timeWindow == null) { timeWindow = Deployment.getDeployment(tenantID).getDefaultNotificationDailyWindows(); }
        
      if (communicationChannel != null && timeWindow != null)
        {
          effectiveDeliveryDate = NGLMRuntime.END_OF_TIME;
          Date today = RLMDateUtils.truncate(now, Calendar.DATE, Deployment.getDeployment(tenantID).getTimeZone());
          for (int i=0; i<8; i++)
            {
              //
              //  check the i-th day
              //

              Date windowDay = RLMDateUtils.addDays(today, i, Deployment.getDeployment(tenantID).getTimeZone());
              Date nextDay = RLMDateUtils.addDays(today, i+1, Deployment.getDeployment(tenantID).getTimeZone());
              for (DailyWindow dailyWindow : communicationChannel.getTodaysDailyWindows(windowDay, communicationChannelTimeWindowService, timeWindow, tenantID))
                {
                  Date windowStartDate = dailyWindow.getFromDate(windowDay, tenantID);
                  Date windowEndDate = dailyWindow.getUntilDate(windowDay, tenantID);
                  if (EvolutionUtilities.isDateBetween(now, windowStartDate, windowEndDate))
                    {
                      effectiveDeliveryDate = now;
                    }
                  else if (now.compareTo(windowStartDate) < 0)
                    {
                      effectiveDeliveryDate = windowStartDate.compareTo(effectiveDeliveryDate) < 0 ? windowStartDate : effectiveDeliveryDate;
                    }
                }

              //
              //  effectiveDeliveryDate found?
              //

              if (effectiveDeliveryDate.compareTo(nextDay) < 0)
                {
                  break;
                }
            }
        } 
      return effectiveDeliveryDate;
    }
    
    
    /*****************************************
    *
    *  generateResponseJSON For the GUI: This object is not a real GUIManagedObject...
    *
    *****************************************/

    public JSONObject generateResponseJSON(boolean fullDetails, Date date)
    {
      JSONObject responseJSON = new JSONObject();
      JSONObject jsonRepresentation = new JSONObject();
      jsonRepresentation.putAll(this.getJSONRepresentation());
          
      JSONObject summaryJSONRepresentation = new JSONObject();
      summaryJSONRepresentation.put("id", this.getJSONRepresentation().get("id"));
      summaryJSONRepresentation.put("name", this.getJSONRepresentation().get("name"));
      summaryJSONRepresentation.put("description", this.getJSONRepresentation().get("description"));
      summaryJSONRepresentation.put("display", this.getJSONRepresentation().get("display"));
      summaryJSONRepresentation.put("icon", this.getJSONRepresentation().get("icon"));
      summaryJSONRepresentation.put("effectiveStartDate", this.getJSONRepresentation().get("effectiveStartDate"));
      summaryJSONRepresentation.put("effectiveEndDate", this.getJSONRepresentation().get("effectiveEndDate"));
      summaryJSONRepresentation.put("userID", this.getJSONRepresentation().get("userID"));
      summaryJSONRepresentation.put("userName", this.getJSONRepresentation().get("userName"));
      summaryJSONRepresentation.put("groupID", this.getJSONRepresentation().get("groupID"));
      summaryJSONRepresentation.put("createdDate", this.getJSONRepresentation().get("createdDate"));
      summaryJSONRepresentation.put("updatedDate", this.getJSONRepresentation().get("updatedDate"));
      summaryJSONRepresentation.put("deleted", this.getJSONRepresentation().get("deleted") != null ? this.getJSONRepresentation().get("deleted") : false);
       
      responseJSON.putAll(fullDetails ? jsonRepresentation : summaryJSONRepresentation);
      responseJSON.put("accepted", this.getAccepted());
      responseJSON.put("active", this.getActive());
      responseJSON.put("valid", this.getAccepted());
      responseJSON.put("processing", true);
      responseJSON.put("readOnly", this.getReadOnly());
      return responseJSON;
    }
}
