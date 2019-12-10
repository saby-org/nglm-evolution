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

public class SalesChannelESSinkConnector extends SimpleESSinkConnector
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
		@Override
		public String getDocumentID(SinkRecord sinkRecord) {
			/****************************************
			 *  extract OfferID
			 ****************************************/

			Object salesChannelIDValue = sinkRecord.key();
			Schema salesChannelIDValueSchema = sinkRecord.keySchema();
			StringKey salesChannelID = StringKey.unpack(new SchemaAndValue(salesChannelIDValueSchema, salesChannelIDValue));

			/****************************************
			 *  use offerID
			 ****************************************/

			return "_" + salesChannelID.hashCode();    
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
				SalesChannel salesChannel = new SalesChannel(guiManagedObject.getJSONRepresentation(), guiManagedObject.getEpoch(), guiManagedObject);

				//
				//  flat fields
				//

				documentMap.put("salesChannelID", salesChannel.getSalesChannelID());
				documentMap.put("salesChannelName", salesChannel.getGUIManagedObjectDisplay());
				documentMap.put("salesChannelActive", salesChannel.getActive());

			} catch (GUIManagerException e) {
			}

			//
			//  return
			//

			return documentMap;
		}
	}
}
