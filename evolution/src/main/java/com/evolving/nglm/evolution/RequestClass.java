package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.INFulfillmentManager.INFulfillmentRequest;
import com.evolving.nglm.evolution.purchase.PurchaseFulfillmentManager.PurchaseFulfillmentRequest;

public enum RequestClass {
  IN(INFulfillmentRequest.class.getName()),
  PURCHASE(PurchaseFulfillmentRequest.class.getName());
  
  private String externalRepresentation;
  
  private RequestClass(String externalRepresentation) { this.externalRepresentation = externalRepresentation; }
  
  public String getExternalRepresentation() { return externalRepresentation; }
  
  public static RequestClass fromExternalRepresentation(String externalRepresentation) {
    for (RequestClass enumeratedValue : RequestClass.values()) {
      if (enumeratedValue.getExternalRepresentation().equals(externalRepresentation)) {
        return enumeratedValue;
      }
    }
    return null;
  }
}