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
}