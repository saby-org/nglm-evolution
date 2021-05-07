/*****************************************************************************
*
*  CommunicationChannel.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.evolution.EvolutionUtilities.TimeUnit;
import com.evolving.nglm.evolution.CommunicationChannelTimeWindow.DailyWindow;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CommunicationChannel extends GUIManagedObject
{
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
//    CommunicationChannelTimeWindows notificationDailyWindows;

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
    public DeliveryManagerDeclaration getDeliveryManagerDeclaration() { return deliveryManagerDeclaration; }
    
    /*****************************************
    *
    *  getEffectiveDeliveryTime
    *
    *****************************************/

    public Date getEffectiveDeliveryTime(CommunicationChannelBlackoutService communicationChannelBlackoutServiceBlackout, CommunicationChannelTimeWindowService communicationChannelTimeWindowService, Date now)
    {
      //
      //  retrieve delivery time configuration
      //

      CommunicationChannelBlackoutPeriod blackoutPeriod = communicationChannelBlackoutServiceBlackout.getActiveCommunicationChannelBlackout("blackoutPeriod", now);

      //
      //  iterate until a valid date is found (give up after 7 days and reschedule even if not legal)
      //

      Date maximumDeliveryDate = RLMDateUtils.addDays(now, 7, Deployment.getBaseTimeZone());
      Date deliveryDate = now;
      while (deliveryDate.before(maximumDeliveryDate))
        {
          Date nextDailyWindowDeliveryDate = this.getEffectiveDeliveryTime(this, deliveryDate, communicationChannelTimeWindowService);
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
    *  constructor
    *
    *****************************************/

    public CommunicationChannel(JSONObject jsonRoot) throws GUIManagerException
    {
      /*****************************************
      *
      *  super
      *
      *****************************************/

      super(jsonRoot, 0);

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
//      if(jsonRoot.get("notificationDailyWindows") != null) {
//        this.notificationDailyWindows = new CommunicationChannelTimeWindows(JSONUtilities.decodeJSONObject(jsonRoot, "notificationDailyWindows", false));
//      }else {
//        this.notificationDailyWindows = Deployment.getNotificationDailyWindows().get("0");
//      }
      
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
          //TODO remove later, forcing conf cleaning
          if(getDeliveryType()!=null)
            {
              log.error("deliveryType configuration for generic channel ("+getName()+") is not possible anymore");
              throw new ServerRuntimeException("old conf for generic channel");
            }
          // delivery type should be derived
          String deliveryType = "notification_"+getName();
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
      
    }
    
    public String getToolboxID() {
      return "NotifChannel-" + this.getID();
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

   
    public List<DailyWindow> getTodaysDailyWindows(Date now, CommunicationChannelTimeWindowService communicationChannelTimeWindowService, CommunicationChannelTimeWindow timeWindow)
    {
      List<DailyWindow> result = null;
      int today = RLMDateUtils.getField(now, Calendar.DAY_OF_WEEK, Deployment.getBaseTimeZone());
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
    
    private Date getEffectiveDeliveryTime(CommunicationChannel communicationChannel, Date now, CommunicationChannelTimeWindowService communicationChannelTimeWindowService)
    {
      Date effectiveDeliveryDate = now;
      CommunicationChannelTimeWindow timeWindow = communicationChannelTimeWindowService.getActiveCommunicationChannelTimeWindow(communicationChannel.getID(), now);
      if(timeWindow == null) { timeWindow = Deployment.getDefaultNotificationDailyWindows(); }
        
      if (communicationChannel != null && timeWindow != null)
        {
          effectiveDeliveryDate = NGLMRuntime.END_OF_TIME;
          Date today = RLMDateUtils.truncate(now, Calendar.DATE, Calendar.SUNDAY, Deployment.getBaseTimeZone());
          for (int i=0; i<8; i++)
            {
              //
              //  check the i-th day
              //

              Date windowDay = RLMDateUtils.addDays(today, i, Deployment.getBaseTimeZone());
              Date nextDay = RLMDateUtils.addDays(today, i+1, Deployment.getBaseTimeZone());
              for (DailyWindow dailyWindow : communicationChannel.getTodaysDailyWindows(windowDay, communicationChannelTimeWindowService, timeWindow))
                {
                  Date windowStartDate = dailyWindow.getFromDate(windowDay);
                  Date windowEndDate = dailyWindow.getUntilDate(windowDay);
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
