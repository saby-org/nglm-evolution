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

public class PaymentMeanESSinkConnector extends SimpleESSinkConnector
{
  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  @Override public Class<? extends Task> taskClass()
  {
    return PaymentMeanESSinkTask.class;
  }

  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  public static class PaymentMeanESSinkTask extends ChangeLogESSinkTask
  {
	@Override
	public String getDocumentID(SinkRecord sinkRecord) {
      /****************************************
      *  extract OfferID
      ****************************************/
	      
      Object paymentMeanIDValue = sinkRecord.key();
      Schema paymentMeanIDValueSchema = sinkRecord.keySchema();
      StringKey paymentMeanID = StringKey.unpack(new SchemaAndValue(paymentMeanIDValueSchema, paymentMeanIDValue));
	      
	  /****************************************
	  *  use offerID
	  ****************************************/
	      
	  return "_" + paymentMeanID.hashCode();    
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
      	PaymentMean paymentMean = new PaymentMean(guiManagedObject.getJSONRepresentation(), guiManagedObject.getEpoch(), guiManagedObject);
		
	    //
	    //  flat fields
	    //
	      
	    documentMap.put("paymentMeanID", paymentMean.getPaymentMeanID());
	    documentMap.put("paymentMeanName", paymentMean.getPaymentMeanName());
	    documentMap.put("paymentMeanActive", paymentMean.getActive());
	    documentMap.put("paymentMeanProviderID", paymentMean.getFulfillmentProviderID());
	  } catch (GUIManagerException e) {
	  }

      //
      //  return
      //
      
      return documentMap;
    }
  }
}
