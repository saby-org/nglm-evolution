/*****************************************************************************
*
*  MailNotificationInterface.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import org.json.simple.JSONObject;

import com.evolving.nglm.evolution.PushNotificationManager.PushNotificationManagerRequest;

public interface PushNotificationInterface
{
  public void init(PushNotificationManager pushNotificationManager, JSONObject pushNotifConfiguration, String pluginName);
  public void send(PushNotificationManagerRequest pushNotificationRequest);
}
