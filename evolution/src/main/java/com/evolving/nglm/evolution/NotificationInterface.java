package com.evolving.nglm.evolution;

import org.json.simple.JSONObject;

import com.evolving.nglm.evolution.NotificationManager.NotificationManagerRequest;


public interface NotificationInterface
{
  public void init(JSONObject pluginConfiguration);
  public void send(NotificationManagerRequest notificationRequest);
}
