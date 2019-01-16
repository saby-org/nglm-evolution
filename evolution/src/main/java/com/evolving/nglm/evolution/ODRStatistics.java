package com.evolving.nglm.evolution;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.NGLMMonitoringObject;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.evolution.DeliveryManager.DeliveryStatus;

public class ODRStatistics implements ODRStatisticsMBean, NGLMMonitoringObject
{
  
  //
  // Logger
  //
  
  public static final Logger log = LoggerFactory.getLogger(ODRStatistics.class);
  
  //
  //  JMXBaseObjectName
  //

  public static final String BaseJMXObjectName = "com.evolving.nglm.evolution:type=ODRStatistics";

  //
  //  attributes
  //

  public String name;
  public String objectNameForManagement;
  public int nbPurchasesDelivered;
  public int nbPurchasesFailed;
  public int nbPurchasesFailedRetry;
  public int nbPurchasesFailedTimeout;
  public int nbPurchasesIndeterminate;
  public int nbPurchasesPending;
  public int nbPurchasesUnknown;

  //
  //  profile event counts, profile counts
  //

  public Map<String,ODRStatistics> evolutionODRCounts;

  //
  // Interface: NGLMMonitoringObject
  //

  public String getObjectNameForManagement() { return objectNameForManagement; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public ODRStatistics(String name)
  {
    //
    //  initialize
    //

    this.name = name;
    this.nbPurchasesDelivered = 0;
    this.nbPurchasesFailed = 0;
    this.nbPurchasesFailedRetry = 0;
    this.nbPurchasesFailedTimeout = 0;
    this.nbPurchasesIndeterminate = 0;
    this.nbPurchasesPending = 0;
    this.nbPurchasesUnknown = 0;
    this.evolutionODRCounts = new HashMap<String,ODRStatistics>();
    this.objectNameForManagement = BaseJMXObjectName + ",serviceName=" + name;

    log.info("ODRStatistics: about to register statistics,  objectNameForManagement=" + objectNameForManagement);
    
    //
    //  register
    //

    NGLMRuntime.registerMonitoringObject(this, true);
    
    evolutionODRCounts.put(this.name, this);
  }

  /*****************************************
  *
  *  updatePurchasesDeliveredCount
  *
  *****************************************/
  
  public synchronized void updatePurchasesCount(int amount, DeliveryStatus status){
    log.info("ODRStatistics.updatePurchasesCount: updating stats, amount="+amount+" status="+status);
    if(status != null){
      
      ODRStatistics stats = evolutionODRCounts.get(name);
      if (stats == null)
        {
          stats = new ODRStatistics(this.name);
          evolutionODRCounts.put(this.name, stats);
        }
      switch (status) {
      case Delivered:
        stats.setNbPurchasesDelivered(amount);
        break;

      case Pending:
        stats.setNbPurchasesPending(amount);
        break;
        
      case Indeterminate:
        stats.setNbPurchasesIndeterminate(amount);
        break;
        
      case Failed:
        stats. setNbPurchasesFailed(amount);
        break;
      
      case FailedRetry:
        stats.setNbPurchasesFailedRetry(amount);
        break;
        
      case FailedTimeout:
        stats.setNbPurchasesFailedTimeout(amount);
        break;
      
      case Unknown:
        stats.setNbPurchasesUnknown(amount);
        break;
      }
    }
  }
  
  @Override
  public int getNbPurchasesDelivered()
  {
    return evolutionODRCounts.get(name).getDeliveredPurchases();
  }

  @Override
  public int getNbPurchasesFailedRetry()
  {
    return evolutionODRCounts.get(name).getFailedRetryPurchases();
  }
  
  @Override
  public int getNbPurchasesFailed()
  {
    return evolutionODRCounts.get(name).getFailedPurchases();
  }

  @Override
  public int getNbPurchasesPending()
  {
    return evolutionODRCounts.get(name).getPendingPurchases();
  }

  @Override
  public int getNbPurchasesIndeterminate()
  {
    return evolutionODRCounts.get(name).getIndeterminatePurchases();
  }

  @Override
  public int getNbPurchasesFailedTimeout()
  {
    return evolutionODRCounts.get(name).getFailedTimeoutPurchases();
  }

  @Override
  public int getNbPurchasesUnknown()
  {
    return evolutionODRCounts.get(name).getUnknownPurchases();
  }
  
  public int getFailedPurchases()
  {
    return nbPurchasesFailed;
  }
  
  public int getFailedRetryPurchases()
  {
    return nbPurchasesFailedRetry;
  }
  
  public int getFailedTimeoutPurchases()
  {
    return nbPurchasesFailedTimeout;
  }
  
  public int getDeliveredPurchases()
  {
    return nbPurchasesDelivered;
  }
  
  public int getIndeterminatePurchases()
  {
    return nbPurchasesIndeterminate;
  }
  
  public int getPendingPurchases()
  {
    return nbPurchasesPending;
  }
  
  public int getUnknownPurchases()
  {
    return nbPurchasesUnknown;
  }

  /*****************************************
  *
  *  unregister
  *
  *****************************************/

  public void unregister()
  {
    for (ODRStatistics stats : evolutionODRCounts.values())
      {
        NGLMRuntime.unregisterMonitoringObject(stats);
      }
  }

  public void setNbPurchasesDelivered(int amount)
  {
    this.nbPurchasesDelivered = nbPurchasesDelivered + amount;
  }

  public void setNbPurchasesFailed(int amount)
  {
    this.nbPurchasesFailed = nbPurchasesFailed + amount;
  }

  public void setNbPurchasesFailedRetry(int amount)
  {
    this.nbPurchasesFailedRetry = nbPurchasesFailedRetry + amount;
  }

  public void setNbPurchasesFailedTimeout(int amount)
  {
    this.nbPurchasesFailedTimeout = nbPurchasesFailedTimeout + amount;
  }

  public void setNbPurchasesIndeterminate(int amount)
  {
    this.nbPurchasesIndeterminate = nbPurchasesIndeterminate + amount;
  }

  public void setNbPurchasesPending(int amount)
  {
    this.nbPurchasesPending = nbPurchasesPending + amount;
  }

  public void setNbPurchasesUnknown(int amount)
  {
    this.nbPurchasesUnknown = nbPurchasesUnknown + amount;
  }
}

