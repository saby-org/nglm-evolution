/*****************************************************************************
*
*  CommunicationChannel.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SchemaUtilities;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.Journey.JourneyStatus;
import com.evolving.nglm.evolution.NotificationDailyWindows.DailyWindow;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
      schemaBuilder.field("defaultSourceAddress", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("profileAddressField", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("deliveryType", Schema.OPTIONAL_STRING_SCHEMA);
      schemaBuilder.field("notificationDailyWindows", NotificationDailyWindows.serde().optionalSchema().schema());
      schemaBuilder.field("communicationChannelParameters", SchemaBuilder.map(Schema.STRING_SCHEMA, CriterionField.schema()).name("communication_channel_parameters").schema());
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

    String defaultSourceAddress;
    String profileAddressField;
    String deliveryType;
    NotificationDailyWindows notificationDailyWindows;
    Map<String,CriterionField> parameters = new HashMap<String, CriterionField>();
    

    /*****************************************
    *
    *  accessors
    *
    *****************************************/

    public String getDefaultSourceAddress() { return defaultSourceAddress; }
    public String getProfileAddressField() { return profileAddressField; }
    public String getDeliveryType () { return deliveryType; } 
    public NotificationDailyWindows getNotificationDailyWindows() { return notificationDailyWindows; }
    public Map<String,CriterionField> getParameters() { return parameters; }

    /*****************************************
    *
    *  constructor -- unpack
    *
    *****************************************/

    public CommunicationChannel(SchemaAndValue schemaAndValue, String defaultSourceAddress, String profileAddressField, String deliveryType, NotificationDailyWindows notificationDailyWindows, Map<String, CriterionField> communicationChannelParameters)
    {
      super(schemaAndValue);
      this.defaultSourceAddress = defaultSourceAddress;
      this.profileAddressField = profileAddressField;
      this.deliveryType = deliveryType;
      this.notificationDailyWindows = notificationDailyWindows; 
      this.parameters = communicationChannelParameters;
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
      struct.put("defaultSourceAddress", communicationChannel.getDefaultSourceAddress());
      struct.put("profileAddressField", communicationChannel.getProfileAddressField());
      struct.put("deliveryType", communicationChannel.getDeliveryType());
      struct.put("notificationDailyWindows", NotificationDailyWindows.serde().packOptional(communicationChannel.getNotificationDailyWindows()));
      struct.put("communicationChannelParameters", packCommunicationChannelParameters(communicationChannel.getParameters()));
      return struct;
    }
    
    /****************************************
    *
    *  packCommunicationChannelParameters
    *
    ****************************************/

    private static Map<String,Object> packCommunicationChannelParameters(Map<String,CriterionField> parameters)
    {
      Map<String,Object> result = new LinkedHashMap<String,Object>();
      for (String parameterName : parameters.keySet())
        {
          CriterionField communicationChannelParameter = parameters.get(parameterName);
          result.put(parameterName,CriterionField.pack(communicationChannelParameter));
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
      String defaultSourceAddress = valueStruct.getString("defaultSourceAddress");
      String profileAddressField = valueStruct.getString("profileAddressField");
      String deliveryType = valueStruct.getString("deliveryType");
      NotificationDailyWindows notificationTimeWindows = null;
      notificationTimeWindows = NotificationDailyWindows.serde().unpackOptional(new SchemaAndValue(schema.field("notificationDailyWindows").schema(), valueStruct.get("notificationDailyWindows")));
      Map<String,CriterionField> communicationChannelParameters = unpackCommunicationChannelParameters(schema.field("communicationChannelParameters").schema(), (Map<String,Object>) valueStruct.get("communicationChannelParameters"));
      
      //
      //  return
      //

      return new CommunicationChannel(schemaAndValue, defaultSourceAddress, profileAddressField, deliveryType, notificationTimeWindows, communicationChannelParameters);
    }
    
    /*****************************************
    *
    *  unpackCommunicationChannelParameters
    *
    *****************************************/

    private static Map<String,CriterionField> unpackCommunicationChannelParameters(Schema schema, Map<String,Object> parameters)
    {
      Map<String,CriterionField> result = new HashMap<String,CriterionField>();
      for (String parameterName : parameters.keySet())
        {
          CriterionField communicationChannelParameter = CriterionField.unpack(new SchemaAndValue(schema.valueSchema(), parameters.get(parameterName)));
          result.put(parameterName, communicationChannelParameter);
        }
      return result;
    }
    
    /*****************************************
    *
    *  getEffectiveDeliveryTime
    *
    *****************************************/

    public Date getEffectiveDeliveryTime(CommunicationChannelService communicationChannelService, CommunicationChannelBlackoutService communicationChannelBlackoutServiceBlackout, Date now)
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
          Date nextDailyWindowDeliveryDate = communicationChannelService.getEffectiveDeliveryTime(this.getGUIManagedObjectID(), deliveryDate);
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

    public CommunicationChannel(JSONObject jsonRoot, long epoch, GUIManagedObject existingCommunicationChannelUnchecked) throws GUIManagerException
    {
      /*****************************************
      *
      *  super
      *
      *****************************************/

      super(jsonRoot, (existingCommunicationChannelUnchecked != null) ? existingCommunicationChannelUnchecked.getEpoch() : epoch);

      /*****************************************
      *
      *  existingCommunicationChannel
      *
      *****************************************/

      CommunicationChannel existingContactPolicy = (existingCommunicationChannelUnchecked != null && existingCommunicationChannelUnchecked instanceof CommunicationChannel) ? (CommunicationChannel) existingCommunicationChannelUnchecked : null;
      
      /*****************************************
      *
      *  attributes
      *
      *****************************************/

      this.defaultSourceAddress = JSONUtilities.decodeString(jsonRoot, "defaultSourceAddress", false);
      this.profileAddressField = JSONUtilities.decodeString(jsonRoot, "profileAddressField", false);
      this.deliveryType = JSONUtilities.decodeString(jsonRoot, "deliveryType", false);
      if(jsonRoot.get("notificationDailyWindows") != null) {
        this.notificationDailyWindows = new NotificationDailyWindows(JSONUtilities.decodeJSONObject(jsonRoot, "notificationDailyWindows", false));
      }else {
        this.notificationDailyWindows = Deployment.getNotificationDailyWindows().get("0");
      }
      JSONArray parametersJSON = JSONUtilities.decodeJSONArray(jsonRoot, "parameters", false);
      for (int i=0; i<parametersJSON.size(); i++)
        {
          JSONObject parameterJSON = (JSONObject) parametersJSON.get(i);
          CriterionField parameter = new CriterionField(parameterJSON);
          this.parameters.put(parameter.getID(), parameter);
        }
      
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
    *  schedule
    *
    *****************************************/

    public Date schedule(String touchPointID, Date now)
    {
      return now;
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
          return epochChanged;
        }
      else
        {
          return true;
        }
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
}
