/*****************************************************************************
*
*  DeliveryManagerDeclaration.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.DeliveryManager.DeliveryGuarantee;

import com.evolving.nglm.core.ConnectSerde;
import com.evolving.nglm.core.DeploymentManagedObject;

import com.rii.utilities.JSONUtilities;
import com.rii.utilities.JSONUtilities.JSONUtilitiesException;

import org.json.simple.JSONObject;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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
  private String requestTopic;
  private String responseTopic;
  private String internalTopic;
  private String routingTopic;
  private int deliveryRatePerMinute;
  private DeliveryGuarantee deliveryGuarantee;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public JSONObject getJSONRepresentation() { return jsonRepresentation; }
  public String getDeliveryType() { return deliveryType; }
  public String getRequestClassName() { return requestClassName; }
  public String getRequestTopic() { return requestTopic; }
  public String getResponseTopic() { return responseTopic; }
  public String getInternalTopic() { return internalTopic; }
  public String getRoutingTopic() { return routingTopic; }
  public int getDeliveryRatePerMinute() { return deliveryRatePerMinute; }
  public DeliveryGuarantee getDeliveryGuarantee() { return deliveryGuarantee; }

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
    this.requestTopic = JSONUtilities.decodeString(jsonRoot, "requestTopic", true);
    this.responseTopic = JSONUtilities.decodeString(jsonRoot, "responseTopic", true);
    this.internalTopic = JSONUtilities.decodeString(jsonRoot, "internalTopic", true);
    this.routingTopic = JSONUtilities.decodeString(jsonRoot, "routingTopic", true);
    this.deliveryRatePerMinute = JSONUtilities.decodeInteger(jsonRoot, "deliveryRatePerMinute", true);
    this.deliveryGuarantee = (JSONUtilities.decodeString(jsonRoot, "deliveryGuarantee", false) != null) ? DeliveryGuarantee.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "deliveryGuarantee", true)) : null;
  }
}
