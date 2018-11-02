/****************************************************************************
*
*  EvolutionEngineEvent.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.SubscriberStreamEvent;

public interface EvolutionEngineEvent extends SubscriberStreamEvent 
{
  public String getEventName();
}
