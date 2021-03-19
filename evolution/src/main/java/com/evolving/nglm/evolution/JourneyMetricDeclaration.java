/*****************************************************************************
*
*  JourneyMetricDeclaration.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.DeploymentManagedObject;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;
import com.evolving.nglm.core.ServerRuntimeException;

import org.json.simple.JSONObject;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Objects;

/* IF COPY/PASTE FROM ANOTHER DeploymentManagedObject OBJECT
 * DO NOT FORGET TO ADD IT TO THE DeploymentManagedObject FACTORY
 * SEE DeploymentManagedObject.create */
public class JourneyMetricDeclaration extends DeploymentManagedObject
{
  /*****************************************
  *
  *  data
  *
  *****************************************/

  private String subscriberProfileMetricHistoryAccessor;
  private int priorPeriodDays;
  private int postPeriodDays;
  private String esFieldPrior;
  private String esFieldDuring;
  private String esFieldPost;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getSubscriberProfileMetricHistoryAccessor() { return subscriberProfileMetricHistoryAccessor; }
  public int getPriorPeriodDays() { return priorPeriodDays; }
  public int getPostPeriodDays() { return postPeriodDays; }
  public String getESFieldPrior() { return esFieldPrior; }
  public String getESFieldDuring() { return esFieldDuring; }
  public String getESFieldPost() { return esFieldPost; }

  /*****************************************
  *
  *  getMetricHistory
  *
  *****************************************/

  public MetricHistory getMetricHistory(SubscriberProfile subscriberProfile)
  {
    try
      {
        Class<? extends SubscriberProfile> subscriberProfileClass = Deployment.getSubscriberProfileClass();
        Method metricHistoryAccessor = subscriberProfileClass.getMethod(subscriberProfileMetricHistoryAccessor);
        MetricHistory result = (MetricHistory) metricHistoryAccessor.invoke(subscriberProfile);
        return result;
      }
    catch (NoSuchMethodException|IllegalAccessException|InvocationTargetException e)
      {
        throw new ServerRuntimeException(e);
      }
  }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public JourneyMetricDeclaration(JSONObject jsonRoot)
  {
    //
    //  construct
    //

    super(jsonRoot);
    this.subscriberProfileMetricHistoryAccessor = JSONUtilities.decodeString(jsonRoot, "subscriberProfileMetricHistoryAccessor", true); 
    this.priorPeriodDays = JSONUtilities.decodeInteger(jsonRoot, "priorPeriodDays", true);
    this.postPeriodDays = JSONUtilities.decodeInteger(jsonRoot, "postPeriodDays", true);
    this.esFieldPrior = JSONUtilities.decodeString(jsonRoot, "esFieldPrior", true);
    this.esFieldDuring = JSONUtilities.decodeString(jsonRoot, "esFieldDuring", true);
    this.esFieldPost = JSONUtilities.decodeString(jsonRoot, "esFieldPost", true);
  }

  /*****************************************
  *
  *  validate
  *
  *****************************************/

  public void validate() throws NoSuchMethodException
  {
    Class<? extends SubscriberProfile> subscriberProfileClass = Deployment.getSubscriberProfileClass();
    subscriberProfileClass.getMethod(subscriberProfileMetricHistoryAccessor);
  }    
}
