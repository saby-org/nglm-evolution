package com.evolving.nglm.evolution;

import org.json.simple.JSONObject;

import com.evolving.nglm.evolution.NotificationManager.NotificationManagerRequest;


public interface NotificationInterface
{
  public void init(NotificationManager pushNotificationManager, JSONObject pushNotifConfiguration, String pluginName);
  public void send(NotificationManagerRequest notificationRequest);
}
