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

import com.evolving.nglm.core.ChangeLogESSinkTask;
import com.evolving.nglm.core.SimpleESSinkConnector;
import com.evolving.nglm.core.StringKey;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class OfferESSinkConnector extends SimpleESSinkConnector
{
  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  @Override public Class<? extends Task> taskClass()
  {
    return OfferESSinkTask.class;
  }

  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  public static class OfferESSinkTask extends ChangeLogESSinkTask
  {
	private CatalogCharacteristicService catalogCharacteristicService = new CatalogCharacteristicService(Deployment.getBrokerServers(), "offeressinkconnector-catalogcharacteristicservice-" + getTaskNumber(), Deployment.getCatalogCharacteristicTopic(), false);
	private DynamicCriterionFieldService dynamicCriterionFieldService;

	@Override
	public String getDocumentID(SinkRecord sinkRecord) {
      /****************************************
      *  extract OfferID
      ****************************************/
	      
      Object offerIDValue = sinkRecord.key();
      Schema offerIDValueSchema = sinkRecord.keySchema();
      StringKey offerID = StringKey.unpack(new SchemaAndValue(offerIDValueSchema, offerIDValue));
	      
	  /****************************************
	  *  use offerID
	  ****************************************/
	      
	  return "_" + offerID.hashCode();    
    }
  
    @Override public Map<String,Object> getDocumentMap(SinkRecord sinkRecord)
    {
      /****************************************
      *
      *  extract Offer
      *
      ****************************************/

      //lazy instantiation quick fix (new Offer need the context init, even if there not really needed, prefer modifying here)
      if(dynamicCriterionFieldService==null){
        synchronized (this){
      	  if(dynamicCriterionFieldService==null){
            dynamicCriterionFieldService = new DynamicCriterionFieldService(Deployment.getBrokerServers(), "offeressinkconnector-dynamiccriterionfieldservice-" + getTaskNumber(), Deployment.getDynamicCriterionFieldTopic(), false);
            CriterionContext.initialize(dynamicCriterionFieldService);
      	  }
        }
      }

      Object guiManagedObjectValue = sinkRecord.value();
      Schema guiManagedObjectValueSchema = sinkRecord.valueSchema();
      GUIManagedObject guiManagedObject = GUIManagedObject.commonSerde().unpack(new SchemaAndValue(guiManagedObjectValueSchema, guiManagedObjectValue));
      
      Map<String,Object> documentMap = new HashMap<String,Object>();
      

      try {
		Offer offer = new Offer(guiManagedObject.getJSONRepresentation(), guiManagedObject.getEpoch(), guiManagedObject, catalogCharacteristicService);
		
	    //
	    //  flat fields
	    //
	      
	    documentMap.put("offerID", offer.getOfferID());
	    documentMap.put("offerName", offer.getDisplay());
	    documentMap.put("offerActive", offer.getActive());

	  } catch (GUIManagerException e) {
	  }

      //
      //  return
      //
      
      return documentMap;
    }
  }
}
