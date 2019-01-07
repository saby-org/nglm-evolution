/*****************************************************************************
*
*  SMSNotificationInterface.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import org.json.simple.JSONObject;

import com.evolving.nglm.evolution.SMSNotificationManager.SMSNotificationManagerRequest;

public interface SMSNotificationInterface
{
  public void init(SMSNotificationManager smsNotificationManager, JSONObject smsNotifSharedConfiguration, String smsNotifSpecificConfiguration, String pluginName);
  public void send(SMSNotificationManagerRequest smsNotificationRequest);
}
