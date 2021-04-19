package com.evolving.nglm.evolution.datacubes;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.DeploymentCommon;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.evolution.GUIManagedObject;
import com.evolving.nglm.evolution.RESTAPIGenericReturnCodes;
import com.evolving.nglm.evolution.JourneyService;
import com.evolving.nglm.evolution.datacubes.mapping.DeliverablesMap;
import com.evolving.nglm.evolution.datacubes.mapping.JourneysMap;
import com.evolving.nglm.evolution.datacubes.mapping.LoyaltyProgramsMap;
import com.evolving.nglm.evolution.datacubes.mapping.ModulesMap;
import com.evolving.nglm.evolution.datacubes.mapping.OffersMap;

public class DatacubeUtils
{
  private static final Logger log = LoggerFactory.getLogger(DatacubeUtils.class);

  public static void embelishReturnCode(Map<String, Object> filters)
  {
    String returnCode = (String) filters.remove("returnCode");
    Integer returnCodeInt = -1; // will translate to UNKNOWN(-1, "UNKNOWN", "UNKNOWN")
    try {
      returnCodeInt = Integer.parseInt(returnCode);
    } catch (NumberFormatException e) {
      log.info("Invalid value found for return code : " + returnCode);
    }
    filters.put("returnCode", RESTAPIGenericReturnCodes.fromGenericResponseCode(returnCodeInt).getGenericResponseMessage());
  }

  public static void embelishFeature(Map<String, Object> filters, String moduleID, ModulesMap modulesMap, LoyaltyProgramsMap loyaltyProgramsMap, DeliverablesMap deliverablesMap, OffersMap offersMap, JourneysMap journeysMap)
  {
    String featureID = (String) filters.remove("featureID");
    String featureDisplay = featureID; // default
    switch(modulesMap.getFeature(moduleID, "feature"))
      {
        case JourneyID:
          featureDisplay = journeysMap.getDisplay(featureID, "feature");
          break;
        case LoyaltyProgramID:
          featureDisplay = loyaltyProgramsMap.getDisplay(featureID, "feature");
          break;
        case OfferID:
          featureDisplay = offersMap.getDisplay(featureID, "feature");
          break;
        case DeliverableID:
          featureDisplay = deliverablesMap.getDisplay(featureID, "feature");
          break;
        default:
          // For None feature we let the featureID as a display (it is usually a string)
          break;
      }
    filters.put("feature", featureDisplay);
  }
  
  public static String retrieveJourneyEndWeek(String journeyID, JourneyService journeyService) {
    GUIManagedObject object = journeyService.getStoredJourney(journeyID, true);
    String timeZone = DeploymentCommon.getDeployment(object.getTenantID()).getTimeZone();
    return RLMDateUtils.formatDateISOWeek(object.getEffectiveEndDate(), timeZone);
  }

}
