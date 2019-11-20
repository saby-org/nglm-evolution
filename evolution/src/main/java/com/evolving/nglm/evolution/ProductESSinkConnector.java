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

public class ProductESSinkConnector extends SimpleESSinkConnector
{
  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  @Override public Class<? extends Task> taskClass()
  {
    return ProductESSinkTask.class;
  }

  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  public static class ProductESSinkTask extends ChangeLogESSinkTask
  {
	private CatalogCharacteristicService catalogCharacteristicService = new CatalogCharacteristicService(Deployment.getBrokerServers(), "productessinkconnector-catalogcharacteristicservice-" + getTaskNumber(), Deployment.getCatalogCharacteristicTopic(), true);
	private DeliverableService deliverableService = new DeliverableService(Deployment.getBrokerServers(), "productessinkconnector-deliverableservice-" + getTaskNumber(), Deployment.getDeliverableTopic(), true);

	@Override
	public String getDocumentID(SinkRecord sinkRecord) {
      /****************************************
      *  extract OfferID
      ****************************************/
	      
      Object productIDValue = sinkRecord.key();
      Schema productIDValueSchema = sinkRecord.keySchema();
      StringKey productID = StringKey.unpack(new SchemaAndValue(productIDValueSchema, productIDValue));
	      
	  /****************************************
	  *  use offerID
	  ****************************************/
	      
	  return "_" + productID.hashCode();    
    }
  
    @Override public Map<String,Object> getDocumentMap(SinkRecord sinkRecord)
    {
      /****************************************
      *
      *  extract Product
      *
      ****************************************/

      Object guiManagedObjectValue = sinkRecord.value();
      Schema guiManagedObjectValueSchema = sinkRecord.valueSchema();
      GUIManagedObject guiManagedObject = GUIManagedObject.commonSerde().unpack(new SchemaAndValue(guiManagedObjectValueSchema, guiManagedObjectValue));
      
      Map<String,Object> documentMap = new HashMap<String,Object>();
      

      try {
		Product product = new Product(guiManagedObject.getJSONRepresentation(), guiManagedObject.getEpoch(), guiManagedObject, deliverableService, catalogCharacteristicService);
		
	    //
	    //  flat fields
	    //
	      
	    documentMap.put("productID", product.getProductID());
	    documentMap.put("productName", product.getDisplay());
	    documentMap.put("productActive", product.getActive());

	  } catch (GUIManagerException e) {
	  }

      //
      //  return
      //
      
      return documentMap;
    }
  }
}
