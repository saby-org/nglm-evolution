package com.evolving.nglm.evolution.datacubes.mapping;

import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.PaymentMean;
import com.evolving.nglm.evolution.PaymentMeanService;

public class PaymentMeansMap extends GUIManagedObjectMap<PaymentMean>
{
  protected static final Logger log = LoggerFactory.getLogger(PaymentMeansMap.class);

  /*****************************************
  *
  * Properties
  *
  *****************************************/
  private PaymentMeanService service;
  
  /*****************************************
  *
  * Constructor
  *
  *****************************************/
  public PaymentMeansMap(PaymentMeanService service) {
    super(PaymentMean.class);
    this.service = service;
  }
  
  /*****************************************
  *
  * GUIManagedObjectMap implementation
  *
  *****************************************/
  // TODO: for the moment, we also retrieve archived objects
  protected Collection<GUIManagedObject> getCollection(int tenantID) { return this.service.getStoredPaymentMeans(true, tenantID); }
  
  /*****************************************
  *
  * Getters
  *
  *****************************************/
  public String getProviderID(String display, String fieldName)
  {
    Collection<GUIManagedObject> paymentMeans = getCollection(0); // TODO EVPRO-99, which value for tenantID ? 
    String id = null;
    if (paymentMeans != null)
      {
        for (GUIManagedObject paymentMean : paymentMeans)
          {
            if (paymentMean.getGUIManagedObjectDisplay().equals(display))
              {
                id = paymentMean.getGUIManagedObjectID();
                break;
              }
          }
      }
    PaymentMean result = this.guiManagedObjects.get(id);
    if(result != null)
      {
        return result.getFulfillmentProviderID();
      }
    else
      {
        logWarningOnlyOnce("Unable to retrieve providerID for " + fieldName + " (PaymentMeanID: " + id + ").");
        return id; // When missing, return default.
      }
  }
  
}