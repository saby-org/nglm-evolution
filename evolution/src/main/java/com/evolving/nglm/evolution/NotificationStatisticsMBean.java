package com.evolving.nglm.evolution;

public interface NotificationStatisticsMBean
{
  public int getNbMessagesDelivered();
  public int getNbMessageFailed();
  public int getNbMessagesPending();
  public int getNbMessagesIndeterminate();
  public int getNbMessagesFailedTimeout();
  public int getNbMessagesFailedRetry();
  public int getNbMessagesUnknown();
}