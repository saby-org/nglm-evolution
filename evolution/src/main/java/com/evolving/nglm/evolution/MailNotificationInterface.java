/*****************************************************************************
*
*  MailNotificationInterface.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import org.json.simple.JSONObject;

import com.evolving.nglm.evolution.MailNotificationManager.MailNotificationManagerRequest;

public interface MailNotificationInterface
{
  public void init(MailNotificationManager smsNotificationManager, JSONObject smsNotifSharedConfiguration, String smsNotifSpecificConfiguration, String pluginName);
  public void send(MailNotificationManagerRequest smsNotificationRequest);
}
