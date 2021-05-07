/****************************************************************************
*
*  AutoProvisionSubscriberStreamEvent.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.SubscriberStreamEvent;

public interface UpdateParentRelationshipEvent extends SubscriberStreamEvent
{
  public String getRelationshipDisplay();
  public String getNewParent();
  public void setNewParent(String newParentSubscriberID);
  public String getNewParentAlternateIDName();
  public boolean isDeletion();
}
