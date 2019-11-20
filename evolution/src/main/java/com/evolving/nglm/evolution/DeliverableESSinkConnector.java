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
import com.evolving.nglm.core.StreamESSinkTask;
import com.evolving.nglm.core.StringKey;
import com.evolving.nglm.core.UniqueKeyServer;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;

public class DeliverableESSinkConnector extends SimpleESSinkConnector
{
  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  @Override public Class<? extends Task> taskClass()
  {
    return DeliverableESSinkTask.class;
  }

  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  public static class DeliverableESSinkTask extends ChangeLogESSinkTask
  {
	@Override
	public String getDocumentID(SinkRecord sinkRecord) {
      /****************************************
      *  extract OfferID
      ****************************************/
	      
      Object deliverableIDValue = sinkRecord.key();
      Schema deliverableIDValueSchema = sinkRecord.keySchema();
      StringKey deliverableID = StringKey.unpack(new SchemaAndValue(deliverableIDValueSchema, deliverableIDValue));
	      
	  /****************************************
	  *  use offerID
	  ****************************************/
	      
	  return "_" + deliverableID.hashCode();    
    }
  
    @Override public Map<String,Object> getDocumentMap(SinkRecord sinkRecord)
    {
      /****************************************
      *
      *  extract Offer
      *
      ****************************************/

      Object guiManagedObjectValue = sinkRecord.value();
      Schema guiManagedObjectValueSchema = sinkRecord.valueSchema();
      GUIManagedObject guiManagedObject = GUIManagedObject.commonSerde().unpack(new SchemaAndValue(guiManagedObjectValueSchema, guiManagedObjectValue));
      
      Map<String,Object> documentMap = new HashMap<String,Object>();
      

      try {
    	  Deliverable deliverable = new Deliverable(guiManagedObject.getJSONRepresentation(), guiManagedObject.getEpoch(), guiManagedObject);
		
	    //
	    //  flat fields
	    //
	      
	    documentMap.put("deliverableID", deliverable.getDeliverableID());
	    documentMap.put("deliverableName", deliverable.getDeliverableName());
	    documentMap.put("deliverableActive", deliverable.getActive());
	    documentMap.put("deliverableProviderID", deliverable.getFulfillmentProviderID());
	  } catch (GUIManagerException e) {
	  }

      //
      //  return
      //
      
      return documentMap;
    }
  }
}
