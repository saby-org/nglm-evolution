package com.evolving.nglm.evolution.purchase;

import com.evolving.nglm.evolution.DeliveryRequest;

public interface IDRCallback
{
  public static final String originalRequest = "originalRequest";
  public void onDRResponse(DeliveryRequest response);
  public String getIdentifier();
}
