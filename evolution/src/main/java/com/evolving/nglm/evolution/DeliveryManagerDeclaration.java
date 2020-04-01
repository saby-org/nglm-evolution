/*****************************************************************************
*
*  DeliveryManagerDeclaration.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.CommodityDeliveryManager.CommodityType;
import com.evolving.nglm.evolution.DeliveryManager.DeliveryGuarantee;
import com.evolving.nglm.evolution.DeliveryRequest.DeliveryPriority;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.DeploymentManagedObject;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.JSONUtilities.JSONUtilitiesException;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class DeliveryManagerDeclaration
{
  /****************************************
  *
  *  data
  *
  ****************************************/

  private JSONObject jsonRepresentation;
  private String deliveryType;
  private String requestClassName;
  private List<String> requestTopics;
  private String responseTopic;
  private String internalTopic;
  private String routingTopic;
  private int deliveryRatePerMinute;
  private DeliveryGuarantee deliveryGuarantee;
  private int retries;
  private int acknowledgementTimeoutSeconds;
  private int correlatorUpdateTimeoutSeconds;
  private String providerID;
  private String providerName;
  private String profileExternalSubscriberIDField;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public JSONObject getJSONRepresentation() { return jsonRepresentation; }
  public String getDeliveryType() { return deliveryType; }
  public String getRequestClassName() { return requestClassName; }
  public List<String> getRequestTopics() { return requestTopics; }
  public String getResponseTopic() { return responseTopic; }
  public String getInternalTopic() { return internalTopic; }
  public String getRoutingTopic() { return routingTopic; }
  public int getDeliveryRatePerMinute() { return deliveryRatePerMinute; }
  public DeliveryGuarantee getDeliveryGuarantee() { return deliveryGuarantee; }
  public int getRetries() { return retries; }
  public int getAcknowledgementTimeoutSeconds() { return acknowledgementTimeoutSeconds; }
  public int getCorrelatorUpdateTimeoutSeconds() { return correlatorUpdateTimeoutSeconds; }
  public String getProviderID() { return providerID; }
  public String getProviderName() { return providerName; }
  public String getProfileExternalSubscriberIDField() { return profileExternalSubscriberIDField; }

  //
  // derived
  //

  public String getDefaultRequestTopic() { return (requestTopics.size() > 0) ? requestTopics.get(0) : null; }
  public String getRequestTopic(DeliveryPriority deliveryPriority) { return requestTopics.get(Math.min(requestTopics.size()-1,deliveryPriority.getTopicIndex())); }
  public CommodityType getProviderType() { return CommodityType.fromExternalRepresentation(getRequestClassName()); }

  //
  //  getRequestSerde
  //

  public ConnectSerde<? extends DeliveryRequest> getRequestSerde()
  {
    try
      {
        Class<? extends DeliveryRequest> requestClass = (Class<? extends DeliveryRequest>) Class.forName(requestClassName);
        Method serdeMethod = requestClass.getMethod("serde");
        ConnectSerde<? extends DeliveryRequest> requestSerde = (ConnectSerde<? extends DeliveryRequest>) serdeMethod.invoke(null);
        return requestSerde;
      }
    catch (ClassNotFoundException|NoSuchMethodException|IllegalAccessException|InvocationTargetException e)
      {
        throw new RuntimeException(e);
      }
  }

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public DeliveryManagerDeclaration(JSONObject jsonRoot) throws NoSuchMethodException, IllegalAccessException
  {
    //
    //  data
    //

    this.jsonRepresentation = jsonRoot;
    this.deliveryType = JSONUtilities.decodeString(jsonRoot, "deliveryType", true);
    this.requestClassName = JSONUtilities.decodeString(jsonRoot, "requestClass", true);
    this.requestTopics = decodeRequestTopics(jsonRoot);
    this.responseTopic = JSONUtilities.decodeString(jsonRoot, "responseTopic", true);
    this.internalTopic = JSONUtilities.decodeString(jsonRoot, "internalTopic", false);
    this.routingTopic = JSONUtilities.decodeString(jsonRoot, "routingTopic", false);
    this.deliveryRatePerMinute = JSONUtilities.decodeInteger(jsonRoot, "deliveryRatePerMinute", Integer.MAX_VALUE);
    this.deliveryGuarantee = (JSONUtilities.decodeString(jsonRoot, "deliveryGuarantee", false) != null) ? DeliveryGuarantee.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "deliveryGuarantee", true)) : null;
    this.retries = JSONUtilities.decodeInteger(jsonRoot, "retries", 0);
    this.acknowledgementTimeoutSeconds = JSONUtilities.decodeInteger(jsonRoot, "acknowledgementTimeoutSeconds", 86400);
    this.correlatorUpdateTimeoutSeconds = JSONUtilities.decodeInteger(jsonRoot, "correlatorUpdateTimeoutSeconds", 86400);
    this.providerID = JSONUtilities.decodeString(jsonRoot, "providerID", false);
    this.providerName = JSONUtilities.decodeString(jsonRoot, "providerName", false);
    this.profileExternalSubscriberIDField = JSONUtilities.decodeString(jsonRoot, "profileExternalSubscriberIDField", false);
  }

  /*****************************************
  *
  *  decodeRequestTopics
  *
  *****************************************/

  private List<String> decodeRequestTopics(JSONObject jsonRoot) throws JSONUtilitiesException
  {
    List<String> requestTopics = new ArrayList<String>();
    if (JSONUtilities.decodeJSONArray(jsonRoot, "requestTopics", false) != null)
      {
        JSONArray jsonArray = JSONUtilities.decodeJSONArray(jsonRoot, "requestTopics", true);
        for (int i=0; i<jsonArray.size(); i++)
          {
            requestTopics.add((String) jsonArray.get(i));
          }
      }
    else
      {
        requestTopics.add(JSONUtilities.decodeString(jsonRoot, "requestTopic", true));
      }
    return requestTopics;
  }
}
