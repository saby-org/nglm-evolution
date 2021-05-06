/****************************************************************************
*
*  EvolutionEngineEvent.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import com.evolving.nglm.core.SubscriberStreamEvent;

public interface EvolutionEngineEvent extends SubscriberStreamEvent 
{
  public String getEventName();
  public default String generateEvolutionEngineEventID() { return UUID.randomUUID().toString(); }
  public default String getEvolutionEngineEventID() { return null; }
  public default Map<String, Object> getEDRDocumentMap()
  {
    return Collections.emptyMap();
  }
}