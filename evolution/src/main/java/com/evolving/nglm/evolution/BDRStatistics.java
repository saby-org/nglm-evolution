package com.evolving.nglm.evolution;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.NGLMMonitoringObject;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.evolution.DeliveryManager.DeliveryStatus;

public class BDRStatistics implements BDRStatisticsMBean, NGLMMonitoringObject
{
  
  //
  // Logger
  //
  
  public static final Logger log = LoggerFactory.getLogger(BDRStatistics.class);
  
  //
  //  JMXBaseObjectName
  //

  public static final String BaseJMXObjectName = "com.evolving.nglm.evolution:type=BDRStatistics";

  //
  //  attributes
  //

  public String name;
  public String objectNameForManagement;
  public int nbEventsDelivered;
  public int nbEventsFailed;
  public int nbEventsFailedRetry;
  public int nbEventsFailedTimeout;
  public int nbEventsIndeterminate;
  public int nbEventsPending;
  public int nbEventsUnknown;

  //
  //  profile event counts, profile counts
  //

  public Map<String,BDRStatistics> evolutionBDRCounts;

  //
  // Interface: NGLMMonitoringObject
  //

  public String getObjectNameForManagement() { return objectNameForManagement; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public BDRStatistics(String name)
  {
    //
    //  initialize
    //

    this.name = name;
    this.nbEventsDelivered = 0;
    this.nbEventsFailed = 0;
    this.nbEventsFailedRetry = 0;
    this.nbEventsFailedTimeout = 0;
    this.nbEventsIndeterminate = 0;
    this.nbEventsPending = 0;
    this.nbEventsUnknown = 0;
    this.evolutionBDRCounts = new HashMap<String,BDRStatistics>();
    this.objectNameForManagement = BaseJMXObjectName + ",serviceName=" + name;

    log.info("BDRStatistics: about to register statistics,  objectNameForManagement=" + objectNameForManagement);
    
    //
    //  register
    //

    NGLMRuntime.registerMonitoringObject(this, true);
    
    evolutionBDRCounts.put(this.name, this);
  }

  /*****************************************
  *
  *  updateMessageDeliveredCount
  *
  *****************************************/
  
  public synchronized void updateBDREventCount(int amount, DeliveryStatus status){
    log.info("BDRStatistics.updateBDREventCount: updating stats, amount="+amount+" status="+status);
    if(status != null){
      
      BDRStatistics stats = evolutionBDRCounts.get(name);
      if (stats == null)
        {
          stats = new BDRStatistics(this.name);
          evolutionBDRCounts.put(this.name, stats);
        }
      switch (status) {
      case Delivered:
        stats.setNbEventsDelivered(amount);
        break;

      case Pending:
        stats.setNbEventsPending(amount);
        break;
        
      case Indeterminate:
        stats.setNbEventsIndeterminate(amount);
        break;
        
      case Failed:
        stats.setNbEventsFailed(amount);
        break;
      
      case FailedRetry:
        stats.setNbEventsFailedRetry(amount);
        break;
        
      case FailedTimeout:
        stats.setNbEventsFailedTimeout(amount);
        break;
      
      case Unknown:
        stats.setNbEventsUnknown(amount);
        break;
      }
    }
  }
  
  @Override
  public int getNbEventsDelivered()
  {
    return evolutionBDRCounts.get(name).getDeliveredEvents();
  }

  @Override
  public int getNbEventsFailedRetry()
  {
    return evolutionBDRCounts.get(name).getFailedRetryEvents();
  }
  
  @Override
  public int getNbEventsFailed()
  {
    return evolutionBDRCounts.get(name).getFailedEvents();
  }

  @Override
  public int getNbEventsPending()
  {
    return evolutionBDRCounts.get(name).getPendingEvents();
  }

  @Override
  public int getNbEventsIndeterminate()
  {
    return evolutionBDRCounts.get(name).getIndeterminateEvents();
  }

  @Override
  public int getNbEventsFailedTimeout()
  {
    return evolutionBDRCounts.get(name).getFailedTimeoutEvents();
  }

  @Override
  public int getNbEventsUnknown()
  {
    return evolutionBDRCounts.get(name).getUnknownEvents();
  }
  
  public int getFailedEvents()
  {
    return nbEventsFailed;
  }
  
  public int getFailedRetryEvents()
  {
    return nbEventsFailedRetry;
  }
  
  public int getFailedTimeoutEvents()
  {
    return nbEventsFailedTimeout;
  }
  
  public int getDeliveredEvents()
  {
    return nbEventsDelivered;
  }
  
  public int getIndeterminateEvents()
  {
    return nbEventsIndeterminate;
  }
  
  public int getPendingEvents()
  {
    return nbEventsPending;
  }
  
  public int getUnknownEvents()
  {
    return nbEventsUnknown;
  }

  /*****************************************
  *
  *  unregister
  *
  *****************************************/

  public void unregister()
  {
    for (BDRStatistics stats : evolutionBDRCounts.values())
      {
        NGLMRuntime.unregisterMonitoringObject(stats);
      }
  }

  public void setNbEventsDelivered(int amount)
  {
    this.nbEventsDelivered = nbEventsDelivered + amount;
  }

  public void setNbEventsFailed(int amount)
  {
    this.nbEventsFailed = nbEventsFailed + amount;
  }

  public void setNbEventsFailedRetry(int amount)
  {
    this.nbEventsFailedRetry = nbEventsFailedRetry + amount;
  }

  public void setNbEventsFailedTimeout(int amount)
  {
    this.nbEventsFailedTimeout = nbEventsFailedTimeout + amount;
  }

  public void setNbEventsIndeterminate(int amount)
  {
    this.nbEventsIndeterminate = nbEventsIndeterminate + amount;
  }

  public void setNbEventsPending(int amount)
  {
    this.nbEventsPending = nbEventsPending + amount;
  }

  public void setNbEventsUnknown(int amount)
  {
    this.nbEventsUnknown = nbEventsUnknown + amount;
  }
}

