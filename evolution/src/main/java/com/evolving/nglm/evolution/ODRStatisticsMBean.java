package com.evolving.nglm.evolution;

public interface ODRStatisticsMBean
{
  public int getNbPurchasesDelivered();
  public int getNbPurchasesFailed();
  public int getNbPurchasesPending();
  public int getNbPurchasesIndeterminate();
  public int getNbPurchasesFailedTimeout();
  public int getNbPurchasesFailedRetry();
  public int getNbPurchasesUnknown();
}

