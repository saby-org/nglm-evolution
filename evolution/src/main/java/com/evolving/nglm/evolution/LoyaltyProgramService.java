/****************************************************************************
*
*  LoyaltyProgramService.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.evolution.GUIManagedObject.IncompleteObject;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.LoyaltyProgram.LoyaltyProgramType;
import com.evolving.nglm.evolution.LoyaltyProgramPoints.Tier;
import com.evolving.nglm.evolution.elasticsearch.ElasticsearchClientAPI;
import com.evolving.nglm.evolution.elasticsearch.ElasticsearchClientException;
import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.SystemTime;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class LoyaltyProgramService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(LoyaltyProgramService.class);

  /*****************************************
  *
  *  constructor
  *
  *****************************************/
  
  public LoyaltyProgramService(String bootstrapServers, String groupID, String loyaltyProgramTopic, boolean masterService, LoyaltyProgramServiceListener loyaltyProgramListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "loyaltyProgramService", groupID, loyaltyProgramTopic, masterService, getSuperListener(loyaltyProgramListener), "putLoyaltyProgram", "removeLoyaltyProgram", notifyOnSignificantChange);
  }
  //
  //  constructor
  //
  
  public LoyaltyProgramService(String bootstrapServers, String groupID, String loyaltyProgramTopic, boolean masterService, LoyaltyProgramServiceListener loyaltyProgramListener)
  {
    this(bootstrapServers, groupID, loyaltyProgramTopic, masterService, loyaltyProgramListener, true);
  }

  //
  //  constructor
  //

  public LoyaltyProgramService(String bootstrapServers, String groupID, String loyaltyProgramTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, loyaltyProgramTopic, masterService, (LoyaltyProgramServiceListener) null, true);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(LoyaltyProgramServiceListener loyaltyProgramListener)
  {
    GUIManagedObjectListener superListener = null;
    if (loyaltyProgramListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { loyaltyProgramListener.loyaltyProgramActivated((LoyaltyProgram) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID, int tenantID) { loyaltyProgramListener.loyaltyProgramDeactivated(guiManagedObjectID); }
        };
      }
    return superListener;
  }

  /*****************************************
  *
  *  getLoyaltyPrograms
  *
  *****************************************/

  public String generateLoyaltyProgramID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredLoyaltyProgram(String loyaltyProgramID, int tenantID) { return getStoredGUIManagedObject(loyaltyProgramID, tenantID); }
  public GUIManagedObject getStoredLoyaltyProgram(String loyaltyProgramID, boolean includeArchived, int tenantID) { return getStoredGUIManagedObject(loyaltyProgramID, includeArchived, tenantID); }
  public Collection<GUIManagedObject> getStoredLoyaltyPrograms(int tenantID) { return getStoredGUIManagedObjects(tenantID); }
  public Collection<GUIManagedObject> getStoredLoyaltyPrograms(boolean includeArchived, int tenantID) { return getStoredGUIManagedObjects(includeArchived, tenantID); }
  public boolean isActiveLoyaltyProgram(GUIManagedObject loyaltyProgramUnchecked, Date date) { return isActiveGUIManagedObject(loyaltyProgramUnchecked, date); }
  public LoyaltyProgram getActiveLoyaltyProgram(String loyaltyProgramID, Date date, int tenantID) { return (LoyaltyProgram) getActiveGUIManagedObject(loyaltyProgramID, date, tenantID); }
  public Collection<LoyaltyProgram> getActiveLoyaltyPrograms(Date date, int tenantID) { return (Collection<LoyaltyProgram>) getActiveGUIManagedObjects(date, tenantID); }

  /*****************************************
  *
  *  putLoyaltyProgram
  *
  *****************************************/

  public void putLoyaltyProgram(LoyaltyProgram loyaltyProgram, boolean newObject, String userID, int tenantID) throws GUIManagerException
  {
    
    //
    //  validate
    //

    loyaltyProgram.validate();

    //
    //  put
    //

    putGUIManagedObject(loyaltyProgram, SystemTime.getCurrentTime(), newObject, userID, tenantID);
    
  }
  
  /*****************************************
  *
  *  putIncompleteLoyaltyProgram
  *
  *****************************************/

  public void putLoyaltyProgram(IncompleteObject loyaltyProgram, boolean newObject, String userID, int tenantID)
  {
    putGUIManagedObject(loyaltyProgram, SystemTime.getCurrentTime(), newObject, userID, tenantID);
  }
  
  /*****************************************
  *
  *  removeLoyaltyProgram
  *
  *****************************************/

  public void removeLoyaltyProgram(String loyaltyProgramID, String userID, int tenantID) 
  { 
    removeGUIManagedObject(loyaltyProgramID, SystemTime.getCurrentTime(), userID, tenantID);
    // LoyaltyProgram loyaltyProgram = (LoyaltyProgram) getStoredLoyaltyProgram(loyaltyProgramID);
  }
  
  public JSONObject generateResponseJSON(GUIManagedObject guiManagedObject, boolean fullDetails, Date date)
  {
    JSONObject responseJSON = super.generateResponseJSON(guiManagedObject, fullDetails, date);
    int tierCount = 0;
    if (guiManagedObject instanceof LoyaltyProgramPoints)
      {
        LoyaltyProgramPoints lp = (LoyaltyProgramPoints) guiManagedObject;
        List<Tier> tiers = lp.getTiers();
        if (tiers != null)
          {
            tierCount = tiers.size();
          }
      }
    responseJSON.put("tierCount", tierCount);
    return responseJSON;
  }

  /*****************************************
  *
  *  interface NotificationTimeWindowListener
  *
  *****************************************/

  public interface LoyaltyProgramServiceListener
  {
    public void loyaltyProgramActivated(LoyaltyProgram loyaltyProgram);
    public void loyaltyProgramDeactivated(String guiManagedObjectID);
  }

  /*****************************************
  *
  *  example main
  *
  *****************************************/

  public static void main(String[] args)
  {
    //
    //  loyaltyProgramListener
    //

    LoyaltyProgramServiceListener loyaltyProgramListener = new LoyaltyProgramServiceListener()
    {
      @Override public void loyaltyProgramActivated(LoyaltyProgram loyaltyProgram) { System.out.println("loyaltyProgram activated: " + loyaltyProgram.getGUIManagedObjectID()); }
      @Override public void loyaltyProgramDeactivated(String guiManagedObjectID) { System.out.println("loyaltyProgram deactivated: " + guiManagedObjectID); }
    };

    //
    //  loyaltyProgramService
    //

    LoyaltyProgramService loyaltyProgramService = new LoyaltyProgramService(Deployment.getBrokerServers(), "example-loyaltyProgramService-001", Deployment.getLoyaltyProgramTopic(), false, loyaltyProgramListener);
    loyaltyProgramService.start();

    //
    //  sleep forever
    //

    while (true)
      {
        try
          {
            Thread.sleep(Long.MAX_VALUE);
          }
        catch (InterruptedException e)
          {
            //
            //  ignore
            //
          }
      }
  }
}