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
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID) { loyaltyProgramListener.loyaltyProgramDeactivated(guiManagedObjectID); }
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
  public GUIManagedObject getStoredLoyaltyProgram(String loyaltyProgramID) { return getStoredGUIManagedObject(loyaltyProgramID); }
  public GUIManagedObject getStoredLoyaltyProgram(String loyaltyProgramID, boolean includeArchived) { return getStoredGUIManagedObject(loyaltyProgramID, includeArchived); }
  public Collection<GUIManagedObject> getStoredLoyaltyPrograms() { return getStoredGUIManagedObjects(); }
  public Collection<GUIManagedObject> getStoredLoyaltyPrograms(boolean includeArchived) { return getStoredGUIManagedObjects(includeArchived); }
  public boolean isActiveLoyaltyProgram(GUIManagedObject loyaltyProgramUnchecked, Date date) { return isActiveGUIManagedObject(loyaltyProgramUnchecked, date); }
  public LoyaltyProgram getActiveLoyaltyProgram(String loyaltyProgramID, Date date) { return (LoyaltyProgram) getActiveGUIManagedObject(loyaltyProgramID, date); }
  public Collection<LoyaltyProgram> getActiveLoyaltyPrograms(Date date) { return (Collection<LoyaltyProgram>) getActiveGUIManagedObjects(date); }

  /*****************************************
  *
  *  putLoyaltyProgram
  *
  *****************************************/

  public void putLoyaltyProgram(LoyaltyProgram loyaltyProgram, boolean newObject, String userID) throws GUIManagerException
  {
    
    //
    //  validate
    //

    loyaltyProgram.validate();

    //
    //  put
    //

    putGUIManagedObject(loyaltyProgram, SystemTime.getCurrentTime(), newObject, userID);
    
  }
  
  /*****************************************
  *
  *  putIncompleteLoyaltyProgram
  *
  *****************************************/

  public void putLoyaltyProgram(IncompleteObject loyaltyProgram, boolean newObject, String userID)
  {
    putGUIManagedObject(loyaltyProgram, SystemTime.getCurrentTime(), newObject, userID);
  }
  
  /*****************************************
  *
  *  removeLoyaltyProgram
  *
  *****************************************/

  public void removeLoyaltyProgram(String loyaltyProgramID, String userID) 
  { 
    removeGUIManagedObject(loyaltyProgramID, SystemTime.getCurrentTime(), userID);
    LoyaltyProgram loyaltyProgram = (LoyaltyProgram) getStoredLoyaltyProgram(loyaltyProgramID);
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