package com.evolving.nglm.evolution;

public interface BDRStatisticsMBean
{
  public int getNbEventsDelivered();
  public int getNbEventsFailed();
  public int getNbEventsPending();
  public int getNbEventsIndeterminate();
  public int getNbEventsFailedTimeout();
  public int getNbEventsFailedRetry();
  public int getNbEventsUnknown();
}
