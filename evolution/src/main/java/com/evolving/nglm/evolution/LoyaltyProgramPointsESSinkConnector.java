/****************************************************************************
*
*  OfferESSinkConnector.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ChangeLogESSinkTask;
import com.evolving.nglm.core.SimpleESSinkConnector;
import com.evolving.nglm.core.StreamESSinkTask;
import com.evolving.nglm.core.StringKey;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.LoyaltyProgramPoints.Tier;

public class LoyaltyProgramPointsESSinkConnector extends SimpleESSinkConnector
{
  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  @Override public Class<? extends Task> taskClass()
  {
    DynamicCriterionFieldService dynamicCriterionFieldService = new DynamicCriterionFieldService(Deployment.getBrokerServers(), "loyaltyprogramessinkconnector-dynamiccriterionfieldservice-001", Deployment.getDynamicCriterionFieldTopic(), false);
    dynamicCriterionFieldService.start();
    CriterionContext.initialize(dynamicCriterionFieldService);
    return LoyaltyProgramPointsESSinkTask.class;
  }

  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  public static class LoyaltyProgramPointsESSinkTask extends ChangeLogESSinkTask
  {
    private CatalogCharacteristicService catalogCharacteristicService = new CatalogCharacteristicService(Deployment.getBrokerServers(), "loyaltyprogramessinkconnector-catalogcharacteristicservice-" + getTaskNumber(), Deployment.getCatalogCharacteristicTopic(), true);
	
	@Override
	public String getDocumentID(SinkRecord sinkRecord) {
     /****************************************
      *  extract loyaltyProgramID
      ****************************************/
	      
      Object loyaltyProgramIDValue = sinkRecord.key();
      Schema loyaltyProgramIDValueSchema = sinkRecord.keySchema();
      StringKey loyaltyProgramID = StringKey.unpack(new SchemaAndValue(loyaltyProgramIDValueSchema, loyaltyProgramIDValue));
		      
	  /****************************************
	  *  use loyaltyProgramID
	  ****************************************/
		      
	  return "_" + loyaltyProgramID.hashCode();    
    }

    @Override public Map<String,Object> getDocumentMap(SinkRecord sinkRecord)
    {
      /****************************************
      *
      *  extract LoyaltyProgram
      *
      ****************************************/

      Object guiManagedObjectValue = sinkRecord.value();
      Schema guiManagedObjectValueSchema = sinkRecord.valueSchema();
      GUIManagedObject guiManagedObject = GUIManagedObject.commonSerde().unpack(new SchemaAndValue(guiManagedObjectValueSchema, guiManagedObjectValue));
        
      Map<String,Object> documentMap = new HashMap<String,Object>();

      /****************************************
      *
      *  documentMap
      *
      ****************************************/

      try {
    	LoyaltyProgramPoints loyaltyProgramPoints = new LoyaltyProgramPoints(guiManagedObject.getJSONRepresentation(), guiManagedObject.getEpoch(), guiManagedObject, catalogCharacteristicService);
        //
        //  flat fields
        //
      
        documentMap.put("loyaltyProgramID", loyaltyProgramPoints.getLoyaltyProgramID());
        documentMap.put("loyaltyProgramName", loyaltyProgramPoints.getLoyaltyProgramDisplay());
        documentMap.put("loyaltyProgramType", loyaltyProgramPoints.getLoyaltyProgramType().name());
        documentMap.put("rewardPointsID", loyaltyProgramPoints.getRewardPointsID());
        documentMap.put("statusPointsID", loyaltyProgramPoints.getStatusPointsID());
        JSONArray jsonTiers = new JSONArray();
        for(Tier tier : loyaltyProgramPoints.getTiers()){
    	  JSONObject jsonTier = new JSONObject();
    	  jsonTier.put("tierName", tier.getTierName());
    	  jsonTiers.add(jsonTier);
        }
        documentMap.put("tiers", jsonTiers);
	  } catch (GUIManagerException e) {
	  }
      
      //
      //  return
      //
      
      return documentMap;
    }    
  }
}
