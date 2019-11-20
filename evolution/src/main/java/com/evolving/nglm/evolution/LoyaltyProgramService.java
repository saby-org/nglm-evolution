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
import com.evolving.nglm.core.SystemTime;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    
    //
    // Create associated criterion
    //
    
    // loyaltyprogram.${LPNAME}.tier
    {
      String lpName = loyaltyProgram.getLoyaltyProgramName();
      String id = computeCriterionId(loyaltyProgram.getLoyaltyProgramID());
      JSONObject criterionFieldJSON = new JSONObject();
      criterionFieldJSON.put("id", id);
      criterionFieldJSON.put("display", "Loyalty Program "+lpName+" Tier");
      criterionFieldJSON.put("dataType", "string");
      criterionFieldJSON.put("tagFormat", null);
      criterionFieldJSON.put("tagMaxLength", null);
      criterionFieldJSON.put("esField", null);
      criterionFieldJSON.put("retriever", "getLoyaltyProgramTier");
      criterionFieldJSON.put("minValue", null);
      criterionFieldJSON.put("maxValue", null);
      criterionFieldJSON.put("includedOperators", null);
      criterionFieldJSON.put("excludedOperators", null);
      criterionFieldJSON.put("includedComparableFields", null); 
      criterionFieldJSON.put("excludedComparableFields", new JSONArray());   
      JSONArray availableValuesField = new JSONArray();
      if (loyaltyProgram.getLoyaltyProgramType().equals(LoyaltyProgramType.POINTS))
        {
          for (Tier tier : ((LoyaltyProgramPoints) loyaltyProgram).getTiers())
            {
              availableValuesField.add(tier.getTierName());  
            }
        }
      criterionFieldJSON.put("availableValues", availableValuesField);
      CriterionField criterionField = new CriterionField(criterionFieldJSON);
      putLoyaltyCriterionFields(criterionField.getID(), criterionField);
    }
    
  }

  /*****************************************
  *
  *  computeCriterionId
  *
  *****************************************/

  private static String computeCriterionId(String loyaltyProgramID)
  {
    return "loyaltyprogram."+loyaltyProgramID+".tier";
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
    if (loyaltyProgram != null)
      {
        removeLoyaltyCriterionFields(computeCriterionId(loyaltyProgram.getLoyaltyProgramID()));
      }
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

  private static Map<String,CriterionField> loyaltyCriterionFields;
  
  static {
    loyaltyCriterionFields = new LinkedHashMap<String,CriterionField>();
    loyaltyCriterionFields.putAll(Deployment.getLoyaltyCriterionFields());
  }

  public static Map<String, CriterionField> getLoyaltyCriterionFields() {return loyaltyCriterionFields; }
  public static void putLoyaltyCriterionFields(String id, CriterionField criteria) { loyaltyCriterionFields.put(id, criteria); }
  public static void removeLoyaltyCriterionFields(String id) { loyaltyCriterionFields.remove(id); }
  
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