/*****************************************************************************
*
*  Point.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import org.json.simple.JSONObject;

import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.evolution.EvolutionUtilities.TimeUnit;
import com.evolving.nglm.evolution.GUIManagedObject.GUIDependencyDef;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

@GUIDependencyDef(objectType = "score", serviceClass = PointService.class, dependencies = {})
public class Score extends Point
{
  
  //
  //  Score constructor (same as point)
  //
  
  public Score(JSONObject jsonRoot, long epoch, GUIManagedObject existingPointUnchecked) throws GUIManagerException
  {
    super(jsonRoot, epoch, existingPointUnchecked);
    validate();
  }

  
  //
  // this property a score must have
  //
  
  private void validate() throws GUIManagerException
  {
    if (getEffectiveEndDate().before(NGLMRuntime.END_OF_TIME) || getEffectiveStartDate().after(NGLMRuntime.BEGINNING_OF_TIME)) throw new GUIManagerException("score should not have start/end date", "");
    if (!getCreditable() || !getDebitable()) throw new GUIManagerException("score should be both creditable and debitable", "");
    if (getValidity().getPeriodType() != TimeUnit.Year || getValidity().getPeriodQuantity() != 100) throw new GUIManagerException("score should have 100 years validity", "");
  }
  

}
