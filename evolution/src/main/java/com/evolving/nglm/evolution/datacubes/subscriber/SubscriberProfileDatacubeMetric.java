/*****************************************************************************
*
*  JourneyMetricDeclaration.java
*
*****************************************************************************/

package com.evolving.nglm.evolution.datacubes.subscriber;

import com.evolving.nglm.core.DeploymentManagedObject;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import com.evolving.nglm.evolution.Deployment;
import com.evolving.nglm.evolution.MetricHistory;
import com.evolving.nglm.evolution.SubscriberProfile;
import com.evolving.nglm.core.ServerRuntimeException;

import org.json.simple.JSONObject;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Objects;

public class SubscriberProfileDatacubeMetric extends DeploymentManagedObject
{
  /*****************************************
  *
  *  data
  *
  *****************************************/
  
  private String yesterdayESField;
  private String todayESField;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/
  
  public String getYesterdayESField() { return yesterdayESField; }
  public String getTodayESField() { return todayESField; }

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
  }
}
