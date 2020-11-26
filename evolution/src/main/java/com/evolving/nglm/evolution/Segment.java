/*****************************************************************************
*
*  Segment.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

public interface Segment
{
  public String getID();
  public String getName();
  public boolean getDependentOnExtendedSubscriberProfile();
  public boolean getSegmentConditionEqual(Segment segment);
}

