package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.VoucherService.VoucherListener;
import com.evolving.nglm.evolution.elasticsearch.ElasticsearchClientAPI;

public class BadgeService extends GUIService
{
  
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(BadgeService.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private BadgeListener badgeListener = null;
  
  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public BadgeService(String bootstrapServers, String groupID, String badgeTopic, boolean masterService, BadgeListener badgeListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "BadgeService", groupID, badgeTopic, masterService, getSuperListener(badgeListener), "putBadge", "removeBadge", notifyOnSignificantChange);
  }
  
  //
  //  constructor
  //
  
  public BadgeService(String bootstrapServers, String groupID, String badgeTopic, boolean masterService, BadgeListener badgeListener)
  {
    this(bootstrapServers, groupID, badgeTopic, masterService, badgeListener, true);
  }

  //
  //  constructor
  //

  public BadgeService(String bootstrapServers, String groupID, String badgeTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, badgeTopic, masterService, (BadgeListener) null, true);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(BadgeListener badgeListener)
  {
    GUIManagedObjectListener superListener = null;
    if (badgeListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { badgeListener.badgeActivated((Badge) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID, int tenantID) { badgeListener.badgeDeactivated(guiManagedObjectID); }
        };
      }
    return superListener;
  }
  
  public interface BadgeListener
  {
    public void badgeActivated(Badge badge);
    public void badgeDeactivated(String guiManagedObjectID);
  }
  
  /*****************************************
  *
  *  getBadges
  *
  *****************************************/

  public String generateBadgeID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredBadge(String badgeID) { return getStoredGUIManagedObject(badgeID); }
  public GUIManagedObject getStoredBadge(String badgeID, boolean includeArchived) { return getStoredGUIManagedObject(badgeID, includeArchived); }
  public Collection<GUIManagedObject> getStoredBadges(int tenantID) { return getStoredGUIManagedObjects(tenantID); }
  public Collection<GUIManagedObject> getStoredBadges(boolean includeArchived, int tenantID) { return getStoredGUIManagedObjects(includeArchived, tenantID); }
  public boolean isActiveBadge(GUIManagedObject badgeUnchecked, Date date) { return isActiveGUIManagedObject(badgeUnchecked, date); }
  public Badge getActiveBadge(String badgeID, Date date) { return (Badge) getActiveGUIManagedObject(badgeID, date); }
  public Collection<Badge> getActiveBadges(Date date, int tenantID) { return (Collection<Badge>) getActiveGUIManagedObjects(date, tenantID); }
  
  /*****************************************
  *
  *  putBadge
  *
  *****************************************/

  public void putBadge(Badge badge, boolean newObject, String userID) throws GUIManagerException
  {
    
    //
    //  validate
    //

    badge.validate();

    //
    //  put
    //

    putGUIManagedObject(badge, SystemTime.getCurrentTime(), newObject, userID);
    
  }
  
  /*****************************************
  *
  *  putBadge
  *
  *****************************************/

  public void putBadge(IncompleteObject badge, boolean newObject, String userID)
  {
    putGUIManagedObject(badge, SystemTime.getCurrentTime(), newObject, userID);
  }
  
  /*****************************************
  *
  *  removeBadge
  *
  *****************************************/

  public void removeBadge(String badgeID, String userID, int tenantID) 
  { 
    removeGUIManagedObject(badgeID, SystemTime.getCurrentTime(), userID, tenantID);
  }
  
  public JSONObject getSummaryJSONRepresentation(GUIManagedObject guiManagedObject)
  {
    JSONObject result = super.getSummaryJSONRepresentation(guiManagedObject);
    JSONObject fullJSON = guiManagedObject.getJSONRepresentation();
    
    result.put("badgeType", JSONUtilities.decodeString(fullJSON, "badgeType", false));
    result.put("badgeObjectives", JSONUtilities.decodeJSONArray(fullJSON, "badgeObjectives", new JSONArray()));
    
    List<JSONObject> translations = new ArrayList<>();
    if (fullJSON.get("translations") != null)
      {
        JSONArray existingTranslation = (JSONArray) guiManagedObject.getJSONRepresentation().get("translations");
        if (existingTranslation != null && !(existingTranslation.isEmpty()))
          {
            for (int i = 0; i < existingTranslation.size(); i++)
              {
                JSONObject translationObject = (JSONObject) existingTranslation.get(i);
                if (translationObject != null && translationObject.get("language") != null)
                  {
                    String language = (String) translationObject.get("language");
                    JSONObject languageToBeUpdate = new JSONObject();
                    languageToBeUpdate.put("language", language);
                    translations.add(languageToBeUpdate);
                  }

              }
          }
      }
    result.put("translations", translations);
    return result;
  
    
  }

}
