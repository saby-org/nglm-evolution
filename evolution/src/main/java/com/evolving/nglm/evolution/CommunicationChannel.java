/*****************************************************************************
*
*  CommunicationChannel.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.CommunicationChannelTimeWindows.DailyWindow;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

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
    *  data
    *
    *****************************************/

    String id;
    String name;
    String display;
    String profileAddressField;
    String deliveryType;
    CommunicationChannelTimeWindows notificationDailyWindows;

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
    public CommunicationChannelTimeWindows getNotificationDailyWindows() { return notificationDailyWindows; }
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
    
    /*****************************************
    *
    *  getEffectiveDeliveryTime
    *
    *****************************************/

    public Date getEffectiveDeliveryTime(CommunicationChannelBlackoutService communicationChannelBlackoutServiceBlackout, Date now)
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
          Date nextDailyWindowDeliveryDate = this.getEffectiveDeliveryTime(this, deliveryDate);
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
      if(jsonRoot.get("notificationDailyWindows") != null) {
        this.notificationDailyWindows = new CommunicationChannelTimeWindows(JSONUtilities.decodeJSONObject(jsonRoot, "notificationDailyWindows", false));
      }else {
        this.notificationDailyWindows = Deployment.getNotificationDailyWindows().get("0");
      }
      
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
      
      this.isGeneric =  JSONUtilities.decodeBoolean(jsonRoot, "isGeneric", Boolean.FALSE);
      
      this.notificationPluginConfiguration = (JSONObject)jsonRoot.get("notificationPluginConfiguration");
      this.notificationPluginClass = JSONUtilities.decodeString(jsonRoot, "notificationPluginClass", isGeneric /* Mandatory only if this is a generic channel*/);
      this.icon = JSONUtilities.decodeString(jsonRoot, "icon", false);
      this.toolboxHeight = JSONUtilities.decodeInteger(jsonRoot, "toolboxHeight", 70);
      this.toolboxWidth = JSONUtilities.decodeInteger(jsonRoot, "toolboxWidth", 70);
      
      this.allowGuiTemplate =  JSONUtilities.decodeBoolean(jsonRoot, "allowGuiTemplate", Boolean.TRUE);
      this.allowInLineTemplate =  JSONUtilities.decodeBoolean(jsonRoot, "allowInLineTemplate", Boolean.FALSE);
      this.isGeneric =  JSONUtilities.decodeBoolean(jsonRoot, "isGeneric", Boolean.FALSE);
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

   
    public List<DailyWindow> getTodaysDailyWindows(Date now)
    {
      List<DailyWindow> result = null;
      int today = RLMDateUtils.getField(now, Calendar.DAY_OF_WEEK, Deployment.getBaseTimeZone());
      if (getNotificationDailyWindows() != null)
        {
          switch(today)
            {
              case Calendar.SUNDAY:
                result = getNotificationDailyWindows().getDailyWindowSunday();
                break;
              case Calendar.MONDAY:
                result = getNotificationDailyWindows().getDailyWindowMonday();
                break;
              case Calendar.TUESDAY:
                result = getNotificationDailyWindows().getDailyWindowTuesday();
                break;
              case Calendar.WEDNESDAY:
                result = getNotificationDailyWindows().getDailyWindowWednesday();
                break;
              case Calendar.THURSDAY:
                result = getNotificationDailyWindows().getDailyWindowThursday();
                break;
              case Calendar.FRIDAY:
                result = getNotificationDailyWindows().getDailyWindowFriday();
                break;
              case Calendar.SATURDAY:
                result = getNotificationDailyWindows().getDailyWindowSaturday();
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
    
    private Date getEffectiveDeliveryTime(CommunicationChannel communicationChannel, Date now)
    {
      Date effectiveDeliveryDate = now;
      if (communicationChannel != null && communicationChannel.getNotificationDailyWindows() != null)
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
              for (DailyWindow dailyWindow : communicationChannel.getTodaysDailyWindows(windowDay))
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
