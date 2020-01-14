package com.evolving.nglm.evolution.datacubes.mapping;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.PaymentMean;

public class PaymentMeansMap extends GUIManagedObjectList<PaymentMean>
{
  protected static final Logger log = LoggerFactory.getLogger(PaymentMeansMap.class);
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public PaymentMeansMap() {
    super();
  }
  
  /*****************************************
  *
  *  updateFromGUIManager
  *
  *****************************************/
  
  public void updateFromGUIManager(GUIManagerClient gmClient) {
    this.reset();
    
    for (JSONObject item : this.getGUIManagedObjectList(gmClient, "getPaymentMeansList", "paymentMeans"))
      {
        try
          {
            PaymentMean paymentMean = new PaymentMean(item);
            guiManagedObjects.put(paymentMean.getGUIManagedObjectID(), paymentMean);
          } 
        catch (GUIManagerException e)
          {
            log.warn("Unable to retrieve some offers: {}",e.getMessage());
          }
      }
  }
  
  /*****************************************
  *
  *  accessors
  *
  *****************************************/

  public String getProviderID(String id, String fieldName)
  {
    PaymentMean result = this.guiManagedObjects.get(id);
    if(result != null)
      {
        return result.getFulfillmentProviderID();
      }
    else
      {
        logWarningOnlyOnce("Unable to retrieve " + fieldName + ".providerID for " + fieldName + ".id: " + id);
        return id; // When missing, return the ID by default.
      }
  }
  
}