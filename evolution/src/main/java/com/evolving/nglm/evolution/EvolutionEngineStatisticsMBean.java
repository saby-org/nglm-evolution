/****************************************************************************
*
*  EvolutionEngineStatisticsMBean.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

public interface EvolutionEngineStatisticsMBean
{
  public int getEventProcessedCount();
  public int getPresentationCount();
  public int getAcceptanceCount();
}
