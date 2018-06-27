/****************************************************************************
*
*  GUIServiceStatistics.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.NGLMMonitoringObject;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.ServerException;

import com.rii.utilities.SystemTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class GUIServiceStatistics implements GUIServiceStatisticsMBean, NGLMMonitoringObject
{
  //
  //  Base JMX object name
  //

  public static String BaseJMXObjectName = "com.evolving.nglm.evolution:type=GUIService";

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(GUIServiceStatistics.class);
  

  //
  //  attributes
  //

  String serviceName;
  String objectNameForManagement;
  int activeCount;
  int objectCount;
  int putCount;
  int removeCount;
  String lastObjectIDPut;
  String lastObjectIDRemoved;
  Date lastPutTime;
  Date lastRemoveTime;

  //
  // Interface: GUIServiceStatisticsMBean
  //

  public String getServiceName() { return serviceName; }
  public int getActiveCount() { return activeCount; }
  public int getObjectCount() { return objectCount; }
  public int getPutCount() { return putCount; }
  public int getRemoveCount() { return removeCount; }
  public String getLastObjectIDPut() { return lastObjectIDPut; } 
  public String getLastObjectIDRemoved() { return lastObjectIDRemoved; } 
  public Date lastPutTime() { return lastPutTime; }
  public Date lastRemoveTime() { return lastRemoveTime; }

  //
  // Interface: NGLMMonitoringObject
  //

  public String getObjectNameForManagement() { return objectNameForManagement; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public GUIServiceStatistics(String serviceName) throws ServerException
  {
    this.serviceName = serviceName;
    this.activeCount = 0;
    this.objectCount = 0;
    this.putCount = 0;
    this.removeCount = 0;
    this.lastObjectIDPut = "";
    this.lastObjectIDRemoved = "";
    this.lastPutTime = NGLMRuntime.BEGINNING_OF_TIME;
    this.lastRemoveTime = NGLMRuntime.BEGINNING_OF_TIME;

    this.objectNameForManagement = BaseJMXObjectName + ",serviceName=" + serviceName;

    //
    // register
    //

    NGLMRuntime.registerMonitoringObject(this, true);
  }

  /*****************************************
  *
  *  setActiveCount
  *
  *****************************************/

  void setActiveCount(int activeCount)
  {
    this.activeCount = activeCount;
  }

  /*****************************************
  *
  *  setObjectCount
  *
  *****************************************/

  void setObjectCount(int objectCount)
  {
    this.objectCount = objectCount;
  }

  /*****************************************
  *
  *  updatePutCount
  *
  *****************************************/

  void updatePutCount(String objectID)
  {
    this.lastObjectIDPut = objectID;
    this.putCount += 1;
    this.lastPutTime = SystemTime.getCurrentTime();    
  }

  /*****************************************
  *
  *  updateRemoveCount
  *
  *****************************************/

  void updateRemoveCount(String objectID)
  {
    this.lastObjectIDRemoved = objectID;
    this.removeCount += 1;
    this.lastRemoveTime = SystemTime.getCurrentTime();
  }
      
  /*****************************************
  *
  *  unregister
  *
  *****************************************/

  public void unregister()
  {
    NGLMRuntime.unregisterMonitoringObject(this);
  }
}

