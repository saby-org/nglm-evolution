/*****************************************************************************
*
*  DynamicEventDeclarationsService.java
*
*****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.evolving.nglm.core.JSONUtilities;
import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionDataType;
import com.evolving.nglm.evolution.EvolutionEngineEventDeclaration.EventRule;

import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.LoyaltyProgramChallenge.ChallengeLevel;
import com.evolving.nglm.evolution.LoyaltyProgramChallenge.LoyaltyProgramLevelChange;
import com.evolving.nglm.evolution.LoyaltyProgramPoints.LoyaltyProgramTierChange;
import com.evolving.nglm.evolution.LoyaltyProgramPoints.Tier;
import com.google.gson.JsonObject;

public class DynamicEventDeclarationsService extends GUIService
{
  /*****************************************
  *
  *  configuration
  *
  *****************************************/

  //
  //  logger
  //

  private static final Logger log = LoggerFactory.getLogger(DynamicEventDeclarationsService.class);

  /*****************************************
  *
  *  data
  *
  *****************************************/

  private DynamicEventDeclarationsListener dynamicEventDeclarationsListener = null;
  private KafkaProducer<byte[], byte[]> kafkaProducer = null;

  /*****************************************
  *
  *  constructor
  *
  *****************************************/

  public DynamicEventDeclarationsService(String bootstrapServers, String groupID, String dynamicEventDeclarationsTopic, boolean masterService, DynamicEventDeclarationsListener dynamicEventDeclarationsListener, boolean notifyOnSignificantChange)
  {
    super(bootstrapServers, "DynamicEventDeclarationsService", groupID, dynamicEventDeclarationsTopic, masterService, getSuperListener(dynamicEventDeclarationsListener), "putDynamicEventDeclarations", "removeDynamicEventDeclarations", notifyOnSignificantChange);
  }

  //
  //  constructor
  //

  public DynamicEventDeclarationsService(String bootstrapServers, String groupID, String dynamicEventDeclarationsTopic, boolean masterService, DynamicEventDeclarationsListener dynamicEventDeclarationsListener)
  {
    this(bootstrapServers, groupID, dynamicEventDeclarationsTopic, masterService, dynamicEventDeclarationsListener, true);
  }

  //
  //  constructor
  //

  public DynamicEventDeclarationsService(String bootstrapServers, String groupID, String dynamicEventDeclarationsTopic, boolean masterService)
  {
    this(bootstrapServers, groupID, dynamicEventDeclarationsTopic, masterService, (DynamicEventDeclarationsListener) null, true);
  }

  //
  //  getSuperListener
  //

  private static GUIManagedObjectListener getSuperListener(DynamicEventDeclarationsListener dynamicEventDeclarationsListener)
  {
    GUIManagedObjectListener superListener = null;
    if (dynamicEventDeclarationsListener != null)
      {
        superListener = new GUIManagedObjectListener()
        {
          @Override public void guiManagedObjectActivated(GUIManagedObject guiManagedObject) { dynamicEventDeclarationsListener.dynamicEventDeclarationsActivated((DynamicEventDeclarations) guiManagedObject); }
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID) { dynamicEventDeclarationsListener.dynamicEventDeclarationsDeactivated(guiManagedObjectID); }
        };
      }
    return superListener;
  }

  /*****************************************
  *
  *  getDynamicEventDeclarationss
  *
  *****************************************/

  public String generateDynamicEventDeclarationsID() { return generateGUIManagedObjectID(); }
  public GUIManagedObject getStoredDynamicEventDeclarations(String dynamicEventDeclarationsID) { return getStoredGUIManagedObject(dynamicEventDeclarationsID); }
  public Collection<GUIManagedObject> getStoredDynamicEventDeclarationss() { return getStoredGUIManagedObjects(); }
  public boolean isActiveDynamicEventDeclarations(GUIManagedObject dynamicEventDeclarationsUnchecked, Date date) { return isActiveGUIManagedObject(dynamicEventDeclarationsUnchecked, date); }
  public DynamicEventDeclarations getActiveDynamicEventDeclarations(String dynamicEventDeclarationsID, Date date) { return (DynamicEventDeclarations) getActiveGUIManagedObject(dynamicEventDeclarationsID, date); }
  
  public DynamicEventDeclarations getSingletonDynamicEventDeclarations() { return getActiveDynamicEventDeclarations(DynamicEventDeclarations.singletonID, SystemTime.getCurrentTime()); }
  public Map<String, EvolutionEngineEventDeclaration> getStaticAndDynamicEvolutionEventDeclarations()
  {
    Map<String, EvolutionEngineEventDeclaration> result = new HashMap<>();
    result.putAll(Deployment.getEvolutionEngineEvents());
    DynamicEventDeclarations singletonDynamicEventDeclarations = getSingletonDynamicEventDeclarations();
    if(singletonDynamicEventDeclarations != null)
      {
        for(String singletonDynamicEventDeclarationName : singletonDynamicEventDeclarations.getDynamicEventDeclarations().keySet()) 
          {
            result.put(singletonDynamicEventDeclarationName, singletonDynamicEventDeclarations.getDynamicEventDeclarations().get(singletonDynamicEventDeclarationName).getDynamicEventDeclaration());
          }
      }
    return result;
  }
  
  /*****************************************
  *
  *  refreshLoyaltyProgramChangeEvent
  *
  *****************************************/

  public void refreshLoyaltyProgramChangeEvent(LoyaltyProgramService loyaltyProgramService)
  {
    DynamicEventDeclaration loyaltyProgramPointChangeEventDeclaration;
    try
      {
        loyaltyProgramPointChangeEventDeclaration = new DynamicEventDeclaration("tier update in loyalty program", ProfileLoyaltyProgramChangeEvent.class.getName(), Deployment.getProfileLoyaltyProgramChangeEventTopic(), EventRule.Standard, getProfileLoyaltyProgramChangeCriterionFields(loyaltyProgramService));
      }
    catch (GUIManagerException e)
      {
        throw new ServerRuntimeException("dynamicEventDeclaration point program change ", e);
      }

    DynamicEventDeclarations dynamicEventDeclarations = getSingletonDynamicEventDeclarations();
    boolean newObject;
    Map<String, DynamicEventDeclaration> dynamicEventDeclarationsMap;
    if (dynamicEventDeclarations == null)
      {
        newObject = true;
        dynamicEventDeclarationsMap = new HashMap<>();
      }
    else
      {
        newObject = false;
        dynamicEventDeclarationsMap = dynamicEventDeclarations.getDynamicEventDeclarations();
      }

    dynamicEventDeclarationsMap.put(loyaltyProgramPointChangeEventDeclaration.getDynamicEventDeclaration().getName(), loyaltyProgramPointChangeEventDeclaration);
    JSONObject guiManagedObjectJson = new JSONObject();
    guiManagedObjectJson.put("id", DynamicEventDeclarations.singletonID);
    guiManagedObjectJson.put("active", Boolean.TRUE);
    dynamicEventDeclarations = new DynamicEventDeclarations(guiManagedObjectJson, dynamicEventDeclarationsMap);

    //
    // put
    //

    putGUIManagedObject(dynamicEventDeclarations, SystemTime.getCurrentTime(), newObject, null);
  }

  /*****************************************
  *
  *  refreshSegmentationChangeEvent
  *
  *****************************************/

  public void refreshSegmentationChangeEvent(SegmentationDimensionService segmentationDimensionService)
  {
    if (!Deployment.getEnableProfileSegmentChange()) 
      {
        return;
      }
    
    DynamicEventDeclaration segmentChangeEventDeclaration;
    try
      {
        segmentChangeEventDeclaration = new DynamicEventDeclaration("segment update", ProfileSegmentChangeEvent.class.getName(), Deployment.getProfileSegmentChangeEventTopic(), EventRule.Standard, getProfileSegmentChangeCriterionFields(segmentationDimensionService));
      }
    catch (GUIManagerException e)
      {
        throw new ServerRuntimeException("dynamicEventDeclaration", e);
      }

    DynamicEventDeclarations dynamicEventDeclarations = getSingletonDynamicEventDeclarations();
    boolean newObject;
    Map<String, DynamicEventDeclaration> dynamicEventDeclarationsMap;
    if (dynamicEventDeclarations == null)
      {
        newObject = true;
        dynamicEventDeclarationsMap = new HashMap<>();
      }
    else
      {
        newObject = false;
        dynamicEventDeclarationsMap = dynamicEventDeclarations.getDynamicEventDeclarations();
      }

    dynamicEventDeclarationsMap.put(segmentChangeEventDeclaration.getDynamicEventDeclaration().getName(), segmentChangeEventDeclaration);
    JSONObject guiManagedObjectJson = new JSONObject();
    guiManagedObjectJson.put("id", DynamicEventDeclarations.singletonID);
    guiManagedObjectJson.put("active", Boolean.TRUE);
    dynamicEventDeclarations = new DynamicEventDeclarations(guiManagedObjectJson, dynamicEventDeclarationsMap);

    //
    // put
    //

    putGUIManagedObject(dynamicEventDeclarations, SystemTime.getCurrentTime(), newObject, null);
  }
  
  /*****************************************
  *
  *  getProfileLoyaltyProgramChangeCriterionFields
  *
  *****************************************/
  private Map<String, CriterionField> getProfileLoyaltyProgramChangeCriterionFields(LoyaltyProgramService loyaltyProgramService) throws GUIManagerException
  {

    Map<String, CriterionField> result = new HashMap<>();
    for (LoyaltyProgram loyaltyProgram : loyaltyProgramService.getActiveLoyaltyPrograms(SystemTime.getCurrentTime()))
      {
        switch (loyaltyProgram.getLoyaltyProgramType())
          {
          case POINTS:
            // for each loyalty program of type point, generate Old Tier, New Tier and isTierUpdated criterion
            LoyaltyProgramPoints loyaltyProgramPoints = (LoyaltyProgramPoints)loyaltyProgram;
            
            //
            // OLD Criterion
            //
            
            JSONObject criterionFieldOLDJSON = new JSONObject();
            JSONArray availableValues = new JSONArray();
            for (Tier tier : loyaltyProgramPoints.getTiers())
              {
                JSONObject av = new JSONObject();
                av.put("id", tier.getTierName());
                av.put("display", tier.getTierName());
                availableValues.add(av);
              }
            JSONObject v = new JSONObject();
            v.put("id", LoyaltyProgramPoints.LoyaltyProgramPointsEventInfos.ENTERING.name());
            v.put("display", LoyaltyProgramPoints.LoyaltyProgramPointsEventInfos.ENTERING.name());
            availableValues.add(v);
            
            criterionFieldOLDJSON.put("id", LoyaltyProgramPoints.CRITERION_FIELD_NAME_OLD_PREFIX + loyaltyProgramPoints.getLoyaltyProgramID());
            criterionFieldOLDJSON.put("display", "Old " + loyaltyProgramPoints.getGUIManagedObjectDisplay() + " tier");
            criterionFieldOLDJSON.put("dataType", "string");
            criterionFieldOLDJSON.put("retriever", "getProfilePointLoyaltyProgramChangeTierOldValue");
            
            criterionFieldOLDJSON.put("availableValues", availableValues);
            CriterionField criterionFieldOLD = new CriterionField(criterionFieldOLDJSON);

            //
            // NEW Criterion
            //           
            
            JSONObject criterionFieldNEWJSON = new JSONObject();
            availableValues = new JSONArray();
            for (Tier tier : loyaltyProgramPoints.getTiers())
              {
                JSONObject av = new JSONObject();
                av.put("id", tier.getTierName());
                av.put("display", tier.getTierName());
                availableValues.add(av);
              }
            v = new JSONObject();
            v.put("id", LoyaltyProgramPoints.LoyaltyProgramPointsEventInfos.LEAVING.name());
            v.put("display", LoyaltyProgramPoints.LoyaltyProgramPointsEventInfos.LEAVING.name());
            availableValues.add(v);
            
            criterionFieldNEWJSON.put("id", LoyaltyProgramPoints.CRITERION_FIELD_NAME_NEW_PREFIX + loyaltyProgramPoints.getLoyaltyProgramID());
            criterionFieldNEWJSON.put("display", "New " + loyaltyProgramPoints.getGUIManagedObjectDisplay() + " tier");
            criterionFieldNEWJSON.put("dataType", "string");
            criterionFieldNEWJSON.put("retriever", "getProfilePointLoyaltyProgramChangeTierNewValue");
            
            criterionFieldNEWJSON.put("availableValues", availableValues);
            CriterionField criterionFieldNEW = new CriterionField(criterionFieldNEWJSON);
            
            //
            // IsUpdated Criterion
            // 
            
            JSONObject criterionFielUpdatedJSON = new JSONObject();
            criterionFielUpdatedJSON.put("id", LoyaltyProgramPoints.CRITERION_FIELD_NAME_IS_UPDATED_PREFIX + loyaltyProgramPoints.getLoyaltyProgramID());
            criterionFielUpdatedJSON.put("display", "Is " + loyaltyProgramPoints.getGUIManagedObjectDisplay() + " updated");
            criterionFielUpdatedJSON.put("dataType", "boolean");
            criterionFielUpdatedJSON.put("retriever", "getProfilePointLoyaltyProgramUpdated");
            CriterionField criterionFieldUpdated = new CriterionField(criterionFielUpdatedJSON);

            result.put(criterionFieldOLD.getID(), criterionFieldOLD);
            result.put(criterionFieldNEW.getID(), criterionFieldNEW);
            result.put(criterionFieldUpdated.getID(), criterionFieldUpdated);
            
            //
            // Tier Update Type
            // 
            
            JSONObject criterionFieldTierUpdateTypeJSON = new JSONObject();
            criterionFieldTierUpdateTypeJSON.put("id", "loyaltyprogram" + "." + loyaltyProgram.getLoyaltyProgramID() + ".tierupdatetype");
            criterionFieldTierUpdateTypeJSON.put("display", "Loyalty Program " + loyaltyProgram.getGUIManagedObjectDisplay() + " tier update type");
            criterionFieldTierUpdateTypeJSON.put("dataType", CriterionDataType.StringCriterion.getExternalRepresentation());
            criterionFieldTierUpdateTypeJSON.put("retriever", "getProfilePointLoyaltyProgramTierUpdateType");
            criterionFieldTierUpdateTypeJSON.put("availableValues", generateAvailableValuesForTierUpdateType());
            CriterionField criterionFieldTierUpdateType = new CriterionField(criterionFieldTierUpdateTypeJSON);

            result.put(criterionFieldOLD.getID(), criterionFieldOLD);
            result.put(criterionFieldNEW.getID(), criterionFieldNEW);
            result.put(criterionFieldUpdated.getID(), criterionFieldUpdated);
            result.put(criterionFieldTierUpdateType.getID(),  criterionFieldTierUpdateType);
            break;
            
          case CHALLENGE:
            LoyaltyProgramChallenge loyaltyProgramChallenge = (LoyaltyProgramChallenge) loyaltyProgram;
            
            //
            // OLD Criterion
            //
            
            Map<String, Object> criterionFieldChallengeOLDMap = new HashMap<String, Object>();
            JSONArray levelAvailableValues = new JSONArray();
            for (ChallengeLevel level : loyaltyProgramChallenge.getLevels())
              {
                JSONObject av = new JSONObject();
                av.put("id", level.getLevelName());
                av.put("display", level.getLevelName());
                levelAvailableValues.add(av);
              }
            JSONObject val = new JSONObject();
            val.put("id", LoyaltyProgramChallenge.LoyaltyProgramChallengeEventInfos.ENTERING.name());
            val.put("display", LoyaltyProgramChallenge.LoyaltyProgramChallengeEventInfos.ENTERING.name());
            levelAvailableValues.add(val);
            
            criterionFieldChallengeOLDMap.put("id", LoyaltyProgramChallenge.CRITERION_FIELD_NAME_OLD_PREFIX + loyaltyProgramChallenge.getLoyaltyProgramID());
            criterionFieldChallengeOLDMap.put("display", "Old " + loyaltyProgramChallenge.getGUIManagedObjectDisplay() + " level");
            criterionFieldChallengeOLDMap.put("dataType", "string");
            criterionFieldChallengeOLDMap.put("retriever", "getProfileChallengeLoyaltyProgramChangeLevelOldValue");
            criterionFieldChallengeOLDMap.put("availableValues", levelAvailableValues);
            CriterionField criterionFieldChallengeOLD = new CriterionField(JSONUtilities.encodeObject(criterionFieldChallengeOLDMap));
            
            //
            // NEW Criterion
            //           
            
            Map<String, Object> criterionFieldChallengeNEWJMap = new HashMap<String, Object>();
            availableValues = new JSONArray();
            for (ChallengeLevel level : loyaltyProgramChallenge.getLevels())
              {
                JSONObject av = new JSONObject();
                av.put("id", level.getLevelName());
                av.put("display", level.getLevelName());
                availableValues.add(av);
              }
            v = new JSONObject();
            v.put("id", LoyaltyProgramChallenge.LoyaltyProgramChallengeEventInfos.LEAVING.name());
            v.put("display", LoyaltyProgramChallenge.LoyaltyProgramChallengeEventInfos.LEAVING.name());
            availableValues.add(v);
            
            criterionFieldChallengeNEWJMap.put("id", LoyaltyProgramChallenge.CRITERION_FIELD_NAME_NEW_PREFIX + loyaltyProgramChallenge.getLoyaltyProgramID());
            criterionFieldChallengeNEWJMap.put("display", "New " + loyaltyProgramChallenge.getGUIManagedObjectDisplay() + " level");
            criterionFieldChallengeNEWJMap.put("dataType", "string");
            criterionFieldChallengeNEWJMap.put("retriever", "getProfileChallengeLoyaltyProgramChangeLevelNewValue");
            criterionFieldChallengeNEWJMap.put("availableValues", availableValues);
            CriterionField criterionFieldChallengeNEW = new CriterionField(JSONUtilities.encodeObject(criterionFieldChallengeNEWJMap));
            
            //
            // IsUpdated Criterion
            // 
            
            Map<String, Object> criterionChallengeFielUpdatedMap = new HashMap<String, Object>();
            criterionChallengeFielUpdatedMap.put("id", LoyaltyProgramChallenge.CRITERION_FIELD_NAME_IS_UPDATED_PREFIX + loyaltyProgramChallenge.getLoyaltyProgramID());
            criterionChallengeFielUpdatedMap.put("display", "Is " + loyaltyProgramChallenge.getGUIManagedObjectDisplay() + " updated");
            criterionChallengeFielUpdatedMap.put("dataType", "boolean");
            criterionChallengeFielUpdatedMap.put("retriever", "getProfileChallengeLoyaltyProgramUpdated");
            CriterionField criterionFieldChallengeUpdated = new CriterionField(JSONUtilities.encodeObject(criterionChallengeFielUpdatedMap));
            
            //
            // level Update Type
            // 
            
            Map<String, Object> criterionFieldLevelUpdateTypeMap = new HashMap<String, Object>();
            criterionFieldLevelUpdateTypeMap.put("id", "loyaltyprogram" + "." + loyaltyProgram.getLoyaltyProgramID() + ".levelupdatetype");
            criterionFieldLevelUpdateTypeMap.put("display", "Loyalty Program " + loyaltyProgram.getGUIManagedObjectDisplay() + " level update type");
            criterionFieldLevelUpdateTypeMap.put("dataType", CriterionDataType.StringCriterion.getExternalRepresentation());
            criterionFieldLevelUpdateTypeMap.put("retriever", "getProfileChallengeLoyaltyProgramLevelUpdateType");
            criterionFieldLevelUpdateTypeMap.put("availableValues", generateAvailableValuesForLevelUpdateType());
            CriterionField criterionFieldLevelUpdateType = new CriterionField(JSONUtilities.encodeObject(criterionFieldLevelUpdateTypeMap));
            
            result.put(criterionFieldChallengeOLD.getID(), criterionFieldChallengeOLD);
            result.put(criterionFieldChallengeNEW.getID(), criterionFieldChallengeNEW);
            result.put(criterionFieldChallengeUpdated.getID(), criterionFieldChallengeUpdated);
            result.put(criterionFieldLevelUpdateType.getID(), criterionFieldLevelUpdateType);
            break;
          } 
      }
    return result;
  }  
  
  /*****************************************
  *
  *  generateAvailableValuesForTierDataType
  *
  *****************************************/

  private JSONArray generateAvailableValuesForTierUpdateType()
  {
    JSONArray availableValuesField = new JSONArray();
    availableValuesField.add(LoyaltyProgramTierChange.Optin.getExternalRepresentation());
    availableValuesField.add(LoyaltyProgramTierChange.Optout.getExternalRepresentation());
    availableValuesField.add(LoyaltyProgramTierChange.Upgrade.getExternalRepresentation());
    availableValuesField.add(LoyaltyProgramTierChange.Downgrade.getExternalRepresentation());
    return availableValuesField;
  }
  
  /*****************************************
  *
  *  generateAvailableValuesForLevelUpdateType
  *
  *****************************************/

  private JSONArray generateAvailableValuesForLevelUpdateType()
  {
    JSONArray availableValuesField = new JSONArray();
    availableValuesField.add(LoyaltyProgramLevelChange.Optin.getExternalRepresentation());
    availableValuesField.add(LoyaltyProgramLevelChange.Optout.getExternalRepresentation());
    availableValuesField.add(LoyaltyProgramLevelChange.Upgrade.getExternalRepresentation());
    availableValuesField.add(LoyaltyProgramLevelChange.Downgrade.getExternalRepresentation());
    return availableValuesField;
  }

  
  /*****************************************
  *
  *  getProfileSegmentChangeCriterionFields
  *
  *****************************************/
  private Map<String, CriterionField> getProfileSegmentChangeCriterionFields(SegmentationDimensionService segmentationDimensionService) throws GUIManagerException
  {

    Map<String, CriterionField> result = new HashMap<>();
    for (SegmentationDimension dimension : segmentationDimensionService.getActiveSegmentationDimensions(SystemTime.getCurrentTime()))
      {
        // for each dimension, generate Old, New and isUpdated dimension criterion
        JSONObject criterionFieldOLDJSON = new JSONObject();
        JSONArray availableValues = new JSONArray();
        for (Segment segment : dimension.getSegments())
          {
            JSONObject v = new JSONObject();
            v.put("id", segment.getID());
            v.put("display", segment.getName());
            availableValues.add(v);
          }
        JSONObject v = new JSONObject();
        v.put("id", ProfileSegmentChangeEvent.SEGMENT_ENTERING_LEAVING.ENTERING.name());
        v.put("display", ProfileSegmentChangeEvent.SEGMENT_ENTERING_LEAVING.ENTERING.name());
        availableValues.add(v);
        
        criterionFieldOLDJSON.put("id", ProfileSegmentChangeEvent.CRITERION_FIELD_NAME_OLD_PREFIX + dimension.getSegmentationDimensionName());
        criterionFieldOLDJSON.put("display", "Old " + dimension.getSegmentationDimensionName() + " segment");
        criterionFieldOLDJSON.put("dataType", "string");
        criterionFieldOLDJSON.put("retriever", "getProfileSegmentChangeDimensionOldValue");
        criterionFieldOLDJSON.put("availableValues", availableValues);
        CriterionField criterionFieldOLD = new CriterionField(criterionFieldOLDJSON);

        availableValues = new JSONArray();
        for (Segment segment : dimension.getSegments())
          {
            v = new JSONObject();
            v.put("id", segment.getID());
            v.put("display", segment.getName());
            availableValues.add(v);
          }
        v = new JSONObject();
        v.put("id", ProfileSegmentChangeEvent.SEGMENT_ENTERING_LEAVING.LEAVING.name());
        v.put("display", ProfileSegmentChangeEvent.SEGMENT_ENTERING_LEAVING.LEAVING.name());
        availableValues.add(v);
        
        JSONObject criterionFieldNewJSON = new JSONObject();
        criterionFieldNewJSON.put("id", ProfileSegmentChangeEvent.CRITERION_FIELD_NAME_NEW_PREFIX + dimension.getSegmentationDimensionName());
        criterionFieldNewJSON.put("display", "New " + dimension.getSegmentationDimensionName() + " segment");
        criterionFieldNewJSON.put("dataType", "string");
        criterionFieldNewJSON.put("retriever", "getProfileSegmentChangeDimensionNewValue");
        criterionFieldNewJSON.put("availableValues", availableValues);
        CriterionField criterionFieldNEW = new CriterionField(criterionFieldNewJSON);

        JSONObject criterionFielUpdatedJSON = new JSONObject();
        criterionFielUpdatedJSON.put("id", ProfileSegmentChangeEvent.CRITERION_FIELD_NAME_IS_UPDATED_PREFIX + dimension.getSegmentationDimensionName());
        criterionFielUpdatedJSON.put("display", "Is " + dimension.getSegmentationDimensionName() + " updated");
        criterionFielUpdatedJSON.put("dataType", "boolean");
        criterionFielUpdatedJSON.put("retriever", "getProfileSegmentChangeDimensionUpdated");
        CriterionField criterionFieldUpdated = new CriterionField(criterionFielUpdatedJSON);

        result.put(criterionFieldOLD.getID(), criterionFieldOLD);
        result.put(criterionFieldNEW.getID(), criterionFieldNEW);
        result.put(criterionFieldUpdated.getID(), criterionFieldUpdated);
      }
    return result;
  }
  
  

  /*****************************************
  *
  *  removeDynamicEventDeclarations
  *
  *****************************************/

  public void removeDynamicEventDeclarations(String dynamicEventDeclarationsID, String userID) { removeGUIManagedObject(dynamicEventDeclarationsID, SystemTime.getCurrentTime(), userID); }

  /*****************************************
  *
  *  interface DynamicEventDeclarationsListener
  *
  *****************************************/

  public interface DynamicEventDeclarationsListener
  {
    public void dynamicEventDeclarationsActivated(DynamicEventDeclarations dynamicEventDeclarations);
    public void dynamicEventDeclarationsDeactivated(String guiManagedObjectID);
  }

  /*****************************************
  *
  *  example main
  *
  *****************************************/

  public static void main(String[] args)
  {
    //
    //  targetListener
    //

    DynamicEventDeclarationsListener dynamicEventDeclarationsListener = new DynamicEventDeclarationsListener()
    {
      @Override public void dynamicEventDeclarationsActivated(DynamicEventDeclarations dynamicEventDeclarations) { System.out.println("DynamicEventDeclarations activated: " + dynamicEventDeclarations.getGUIManagedObjectID()); }
      @Override public void dynamicEventDeclarationsDeactivated(String guiManagedObjectID) { System.out.println("DynamicEventDeclarations deactivated: " + guiManagedObjectID); }
    };

    //
    //  DynamicEventDeclarationsService
    //

    DynamicEventDeclarationsService dynamicEventDeclarationsService = new DynamicEventDeclarationsService(Deployment.getBrokerServers(), "example-segmentchangeeventdeclarationservice-001", Deployment.getDynamicEventDeclarationsTopic(), false, dynamicEventDeclarationsListener);
    dynamicEventDeclarationsService.start();

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
