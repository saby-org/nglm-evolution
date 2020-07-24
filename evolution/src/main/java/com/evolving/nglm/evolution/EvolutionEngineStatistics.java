/****************************************************************************
*
*  EvolutionEngineStatisticsMBean.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.NGLMMonitoringObject;
import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.ServerException;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.SubscriberStreamEvent;

import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class EvolutionEngineStatistics
{

  Histogram subscriberStateSize;
  Histogram subscriberHistorySize;
  Histogram extendedProfileSize;
  public Histogram getSubscriberStateSize() { return subscriberStateSize; }
  public Histogram getSubscriberHistorySize() { return subscriberHistorySize; }
  public Histogram getExtendedProfileSize() { return extendedProfileSize; }

  public EvolutionEngineStatistics(String name)
  {
    this.subscriberStateSize = new Histogram("subscriberStateSize", 10, 20, 1000, "KB");
    this.subscriberHistorySize = new Histogram("subscriberHistorySize", 10, 20, 1000, "KB");
    this.extendedProfileSize = new Histogram("extendedProfileSize", 10, 20, 1000, "KB");
  }

  synchronized void updateSubscriberStateSize(byte[] kafkaRepresentation)
  {
    subscriberStateSize.logData(kafkaRepresentation.length);
  }

  synchronized void updateSubscriberHistorySize(byte[] kafkaRepresentation)
  {
    subscriberHistorySize.logData(kafkaRepresentation.length);
  }

  synchronized void updateExtendedProfileSize(byte[] kafkaRepresentation)
  {
    extendedProfileSize.logData(kafkaRepresentation.length);
  }

}
