package com.evolving.nglm.evolution;

import java.util.Date;

public interface MONotificationEvent
{
  public void fillWithMOInfos(String subscriberID, Date originTimesTamp, String channelName, String sourceAdress, String destinationAddress, String messageText);
  default public String formatAddressForRedis (String address){
    return address;
  }
}
