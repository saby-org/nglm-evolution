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

import com.evolving.nglm.core.ServerRuntimeException;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.EvaluationCriterion.CriterionDataType;
import com.evolving.nglm.evolution.EvolutionEngineEventDeclaration.EventRule;

import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
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
          @Override public void guiManagedObjectDeactivated(String guiManagedObjectID, int tenantID) { dynamicEventDeclarationsListener.dynamicEventDeclarationsDeactivated(guiManagedObjectID); }
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
  public GUIManagedObject getStoredDynamicEventDeclarations(String dynamicEventDeclarationsID, int tenantID) { return getStoredGUIManagedObject(dynamicEventDeclarationsID, tenantID); }
  public Collection<GUIManagedObject> getStoredDynamicEventDeclarationss(int tenantID) { return getStoredGUIManagedObjects(tenantID); }
  public boolean isActiveDynamicEventDeclarations(GUIManagedObject dynamicEventDeclarationsUnchecked, Date date) { return isActiveGUIManagedObject(dynamicEventDeclarationsUnchecked, date); }
  public DynamicEventDeclarations getActiveDynamicEventDeclarations(String dynamicEventDeclarationsID, Date date, int tenantID) { return (DynamicEventDeclarations) getActiveGUIManagedObject(dynamicEventDeclarationsID, date, tenantID); }
  
  public DynamicEventDeclarations getSingletonDynamicEventDeclarations(int tenantID) { return getActiveDynamicEventDeclarations(DynamicEventDeclarations.singletonID, SystemTime.getCurrentTime(), tenantID); }
  public Map<String, EvolutionEngineEventDeclaration> getStaticAndDynamicEvolutionEventDeclarations(int tenantID)
  {
    Map<String, EvolutionEngineEventDeclaration> result = new HashMap<>();
    result.putAll(Deployment.getEvolutionEngineEvents());
    DynamicEventDeclarations singletonDynamicEventDeclarations = getSingletonDynamicEventDeclarations(tenantID);
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

  public void refreshLoyaltyProgramChangeEvent(LoyaltyProgramService loyaltyProgramService, int tenantID)
  {
    DynamicEventDeclaration loyaltyProgramPointChangeEventDeclaration;
    try
      {
        loyaltyProgramPointChangeEventDeclaration = new DynamicEventDeclaration("tier update in loyalty program", ProfileLoyaltyProgramChangeEvent.class.getName(), Deployment.getProfileLoyaltyProgramChangeEventTopic(), EventRule.Standard, getProfileLoyaltyProgramChangeCriterionFields(loyaltyProgramService, tenantID));
      }
    catch (GUIManagerException e)
      {
        throw new ServerRuntimeException("dynamicEventDeclaration point program change ", e);
      }

    DynamicEventDeclarations dynamicEventDeclarations = getSingletonDynamicEventDeclarations(tenantID);
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
    dynamicEventDeclarations = new DynamicEventDeclarations(guiManagedObjectJson, dynamicEventDeclarationsMap, tenantID);

    //
    // put
    //

    putGUIManagedObject(dynamicEventDeclarations, SystemTime.getCurrentTime(), newObject, null, tenantID);
  }

  /*****************************************
  *
  *  refreshSegmentationChangeEvent
  *
  *****************************************/

  public void refreshSegmentationChangeEvent(SegmentationDimensionService segmentationDimensionService, int tenantID)
  {
    if (!Deployment.getEnableProfileSegmentChange()) 
      {
        return;
      }
    
    DynamicEventDeclaration segmentChangeEventDeclaration;
    try
      {
        segmentChangeEventDeclaration = new DynamicEventDeclaration("segment update", ProfileSegmentChangeEvent.class.getName(), Deployment.getProfileSegmentChangeEventTopic(), EventRule.Standard, getProfileSegmentChangeCriterionFields(segmentationDimensionService, tenantID));
      }
    catch (GUIManagerException e)
      {
        throw new ServerRuntimeException("dynamicEventDeclaration", e);
      }

    DynamicEventDeclarations dynamicEventDeclarations = getSingletonDynamicEventDeclarations(tenantID);
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
    dynamicEventDeclarations = new DynamicEventDeclarations(guiManagedObjectJson, dynamicEventDeclarationsMap, tenantID);

    //
    // put
    //

    putGUIManagedObject(dynamicEventDeclarations, SystemTime.getCurrentTime(), newObject, null, tenantID);
  }
  
  /*****************************************
  *
  *  getProfileLoyaltyProgramChangeCriterionFields
  *
  *****************************************/
  private Map<String, CriterionField> getProfileLoyaltyProgramChangeCriterionFields(LoyaltyProgramService loyaltyProgramService, int tenantID) throws GUIManagerException
  {

    Map<String, CriterionField> result = new HashMap<>();
    for (LoyaltyProgram loyaltyProgram : loyaltyProgramService.getActiveLoyaltyPrograms(SystemTime.getCurrentTime(), tenantID))
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
  *  getProfileSegmentChangeCriterionFields
  *
  *****************************************/
  private Map<String, CriterionField> getProfileSegmentChangeCriterionFields(SegmentationDimensionService segmentationDimensionService, int tenantID) throws GUIManagerException
  {

    Map<String, CriterionField> result = new HashMap<>();
    for (SegmentationDimension dimension : segmentationDimensionService.getActiveSegmentationDimensions(SystemTime.getCurrentTime(), tenantID))
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

  public void removeDynamicEventDeclarations(String dynamicEventDeclarationsID, String userID, int tenantID) { removeGUIManagedObject(dynamicEventDeclarationsID, SystemTime.getCurrentTime(), userID, tenantID); }

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
