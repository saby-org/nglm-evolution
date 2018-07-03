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
  private String requestType;
  private Class<? extends DeliveryRequest> requestClass;
  private String requestTopic;
  private String responseTopic;
  private String internalTopic;
  private int deliveryRatePerMinute;
  private DeliveryGuarantee deliveryGuarantee;
  private ConnectSerde<? extends DeliveryRequest> requestSerde;

  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  protected JSONObject getJSONRepresentation() { return jsonRepresentation; }
  public String getRequestType() { return requestType; }
  public Class<? extends DeliveryRequest> getRequestClass() { return requestClass; }
  public String getRequestTopic() { return requestTopic; }
  public String getResponseTopic() { return responseTopic; }
  public String getInternalTopic() { return internalTopic; }
  public int getDeliveryRatePerMinute() { return deliveryRatePerMinute; }
  public DeliveryGuarantee getDeliveryGuarantee() { return deliveryGuarantee; }
  public ConnectSerde<? extends DeliveryRequest> getRequestSerde() { return requestSerde; }

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
    this.requestType = JSONUtilities.decodeString(jsonRoot, "requestType", true);
    this.requestTopic = JSONUtilities.decodeString(jsonRoot, "requestTopic", true);
    this.responseTopic = JSONUtilities.decodeString(jsonRoot, "responseTopic", true);
    this.internalTopic = JSONUtilities.decodeString(jsonRoot, "internalTopic", true);
    this.deliveryRatePerMinute = JSONUtilities.decodeInteger(jsonRoot, "deliveryRatePerMinute", true);
    this.deliveryGuarantee = (JSONUtilities.decodeString(jsonRoot, "deliveryGuarantee", false) != null) ? DeliveryGuarantee.fromExternalRepresentation(JSONUtilities.decodeString(jsonRoot, "deliveryGuarantee", true)) : null;

    //
    //  class
    //

    String requestClassName = JSONUtilities.decodeString(jsonRoot, "requestClassName", true);
    try
      {
        this.requestClass = (Class<? extends DeliveryRequest>) Class.forName(requestClassName);
        Method serdeMethod = this.requestClass.getMethod("serde");
        this.requestSerde = (ConnectSerde<? extends DeliveryRequest>) serdeMethod.invoke(null);
      }
    catch (ClassNotFoundException|NoSuchMethodException|IllegalAccessException|InvocationTargetException e)
      {
        throw new RuntimeException(e);
      }
  }
}
