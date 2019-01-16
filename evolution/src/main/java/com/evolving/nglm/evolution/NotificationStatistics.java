package com.evolving.nglm.evolution;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.NGLMMonitoringObject;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.evolution.DeliveryManager.DeliveryStatus;

public class NotificationStatistics implements NotificationStatisticsMBean, NGLMMonitoringObject
{
  
  //
  // Logger
  ///
  
  public static final Logger log = LoggerFactory.getLogger(NotificationStatistics.class);
  
  //
  //  JMXBaseObjectName
  //

  public static final String BaseJMXObjectName = "com.evolving.nglm.evolution:type=NotificationStatistics";

  //
  //  attributes
  //

  public String name;
  public String pluginName;
  public String objectNameForManagement;
  public int nbMessagesDelivered;
  public int nbMessagesFailed;
  public int nbMessagesFailedRetry;
  public int nbMessagesFailedTimeout;
  public int nbMessagesIndeterminate;
  public int nbMessagesPending;
  public int nbMessagesUnknown;

  //
  //  profile event counts, profile counts
  //

  public Map<String,NotificationStatistics> evolutionNotificationCounts;

  //
  // Interface: NGLMMonitoringObject
  //

  public String getObjectNameForManagement() { return objectNameForManagement; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public NotificationStatistics(String name, String pluginName)
  {
    //
    //  initialize
    //

    this.name = name;
    this.pluginName = pluginName;
    this.nbMessagesDelivered = 0;
    this.nbMessagesFailed = 0;
    this.nbMessagesFailedRetry = 0;
    this.nbMessagesFailedTimeout = 0;
    this.nbMessagesIndeterminate = 0;
    this.nbMessagesPending = 0;
    this.nbMessagesUnknown = 0;
    this.evolutionNotificationCounts = new HashMap<String,NotificationStatistics>();
    this.objectNameForManagement = BaseJMXObjectName + ",serviceName=" + name;

    log.info("NotificationStatistics: about to register statistics,  objectNameForManagement=" + objectNameForManagement);
    
    //
    //  register
    //

    NGLMRuntime.registerMonitoringObject(this, true);
    
    evolutionNotificationCounts.put(this.pluginName, this);
  }

  /*****************************************
  *
  *  updateMessageDeliveredCount
  *
  *****************************************/
  
  public synchronized void updateMessageCount(String channel, int amount, DeliveryStatus status){
    log.info("NotificationStatistics.updateMessageCount: updating stats,  channel=" + channel+ " amount="+amount+" status="+status);
    if(status != null && channel != null && !channel.isEmpty()){
      
      NotificationStatistics stats = evolutionNotificationCounts.get(channel);
      if (stats == null)
        {
          stats = new NotificationStatistics(this.name, this.pluginName);
          evolutionNotificationCounts.put(this.pluginName, stats);
        }
      switch (status) {
      case Delivered:
        stats.setNbMessagesDelivered(amount);
        break;

      case Pending:
        stats.setNbMessagesPending(amount);
        break;
        
      case Indeterminate:
        stats.setNbMessagesIndeterminate(amount);
        break;
        
      case Failed:
        stats.setNbMessagesFailed(amount);
        break;
      
      case FailedRetry:
        stats.setNbMessagesFailedRetry(amount);
        break;
        
      case FailedTimeout:
        stats.setNbMessagesFailedTimeout(amount);
        break;
      
      case Unknown:
        stats.setNbMessagesUnknown(amount);
        break;
      }
    }
  }
  
  @Override
  public int getNbMessagesDelivered()
  {
    return evolutionNotificationCounts.get(pluginName).getDeliveredMessages();
  }

  @Override
  public int getNbMessagesFailedRetry()
  {
    return evolutionNotificationCounts.get(pluginName).getFailedRetryMessages();
  }
  
  @Override
  public int getNbMessageFailed()
  {
    return evolutionNotificationCounts.get(pluginName).getFailedMessages();
  }

  @Override
  public int getNbMessagesPending()
  {
    return evolutionNotificationCounts.get(pluginName).getPendingMessages();
  }

  @Override
  public int getNbMessagesIndeterminate()
  {
    return evolutionNotificationCounts.get(pluginName).getIndeterminateMessages();
  }

  @Override
  public int getNbMessagesFailedTimeout()
  {
    return evolutionNotificationCounts.get(pluginName).getFailedTimeoutMessages();
  }

  @Override
  public int getNbMessagesUnknown()
  {
    return evolutionNotificationCounts.get(pluginName).getUnknownMessages();
  }
  
  public int getFailedMessages()
  {
    return nbMessagesFailed;
  }
  
  public int getFailedRetryMessages()
  {
    return nbMessagesFailedRetry;
  }
  
  public int getFailedTimeoutMessages()
  {
    return nbMessagesFailedTimeout;
  }
  
  public int getDeliveredMessages()
  {
    return nbMessagesDelivered;
  }
  
  public int getIndeterminateMessages()
  {
    return nbMessagesIndeterminate;
  }
  
  public int getPendingMessages()
  {
    return nbMessagesPending;
  }
  
  public int getUnknownMessages()
  {
    return nbMessagesUnknown;
  }

  /*****************************************
  *
  *  unregister
  *
  *****************************************/

  public void unregister()
  {
    for (NotificationStatistics stats : evolutionNotificationCounts.values())
      {
        NGLMRuntime.unregisterMonitoringObject(stats);
      }
  }

  public void setNbMessagesDelivered(int amount)
  {
    this.nbMessagesDelivered = nbMessagesDelivered + amount;
  }

  public void setNbMessagesFailed(int amount)
  {
    this.nbMessagesFailed = nbMessagesFailed + amount;
  }

  public void setNbMessagesFailedRetry(int amount)
  {
    this.nbMessagesFailedRetry = nbMessagesFailedRetry + amount;
  }

  public void setNbMessagesFailedTimeout(int amount)
  {
    this.nbMessagesFailedTimeout = nbMessagesFailedTimeout + amount;
  }

  public void setNbMessagesIndeterminate(int amount)
  {
    this.nbMessagesIndeterminate = nbMessagesIndeterminate + amount;
  }

  public void setNbMessagesPending(int amount)
  {
    this.nbMessagesPending = nbMessagesPending + amount;
  }

  public void setNbMessagesUnknown(int amount)
  {
    this.nbMessagesUnknown = nbMessagesUnknown + amount;
  }
}

