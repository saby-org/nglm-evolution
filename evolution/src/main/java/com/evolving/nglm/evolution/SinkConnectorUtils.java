package com.evolving.nglm.evolution;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.AlternateID;

public class SinkConnectorUtils
{
  private static final Logger log = LoggerFactory.getLogger(SinkConnectorUtils.class);

  // always populate all alternateIds in deploy order
  public static void putAlternateIDs(Map<String,String> alternateIDsMap, Map<String,Object> map)
  {
    for (AlternateID alternateID : Deployment.getAlternateIDs().values())
      {
        String alternateIDValue = alternateIDsMap.get(alternateID.getID());
        if (log.isTraceEnabled()) log.trace("adding " + alternateID.getID() + " with " + alternateIDValue);
        map.put(alternateID.getID(), alternateIDValue);
      }
  }

}
