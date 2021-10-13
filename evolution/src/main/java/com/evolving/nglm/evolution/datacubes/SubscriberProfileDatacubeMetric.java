/*****************************************************************************
*
*  JourneyMetricDeclaration.java
*
*****************************************************************************/

package com.evolving.nglm.evolution.datacubes;

import com.evolving.nglm.core.DeploymentManagedObject;
import com.evolving.nglm.core.JSONUtilities;

import org.json.simple.JSONObject;

/* IF COPY/PASTE FROM ANOTHER DeploymentManagedObject OBJECT
 * DO NOT FORGET TO ADD IT TO THE DeploymentManagedObject FACTORY
 * SEE DeploymentManagedObject.create */
public class SubscriberProfileDatacubeMetric extends DeploymentManagedObject
{
  /*****************************************
  *
  *  data
  *
  *****************************************/
  
  private String yesterdayESField;
  private String todayESField;
  private boolean isMetricROI;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/
  
  public String getYesterdayESField() { return yesterdayESField; }
  public String getTodayESField() { return todayESField; }
  public boolean isMetricROI() {return isMetricROI; }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public SubscriberProfileDatacubeMetric(JSONObject jsonRoot)
  {
    super(jsonRoot);
    this.yesterdayESField = JSONUtilities.decodeString(jsonRoot, "yesterdayESField", true);
    this.todayESField = JSONUtilities.decodeString(jsonRoot, "todayESField", true);
    this.isMetricROI = JSONUtilities.decodeBoolean(jsonRoot, "metricROI", true);
  }
}
