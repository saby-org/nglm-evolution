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

public class JourneyESSinkConnector extends SimpleESSinkConnector
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
		private CatalogCharacteristicService catalogCharacteristicService = new CatalogCharacteristicService(Deployment.getBrokerServers(), "journeyessinkconnector-catalogcharacteristicservice-" + getTaskNumber(), Deployment.getCatalogCharacteristicTopic(), true);
		private SubscriberMessageTemplateService subscriberMessageTemplateService = new SubscriberMessageTemplateService(Deployment.getBrokerServers(), "journeyessinkconnector-subscriberMessageTemplateService-" + getTaskNumber(), Deployment.getSubscriberMessageTemplateTopic(), true);
		private DynamicEventDeclarationsService dynamicEventDeclarationsService = new DynamicEventDeclarationsService(Deployment.getBrokerServers(), "journeyessinkconnector-dynamicEventDeclarationsService-" + getTaskNumber(), Deployment.getDynamicEventDeclarationsTopic(), true);
		private CommunicationChannelService communicationChannelService = new CommunicationChannelService(Deployment.getBrokerServers(), "journeyessinkconnector-communicationChannelService-" + getTaskNumber(), Deployment.getCommunicationChannelTopic(), true);

		@Override
		public String getDocumentID(SinkRecord sinkRecord) {
			/****************************************
			 *  extract OfferID
			 ****************************************/

			Object journeyIDValue = sinkRecord.key();
			Schema journeyIDValueSchema = sinkRecord.keySchema();
			StringKey journeyID = StringKey.unpack(new SchemaAndValue(journeyIDValueSchema, journeyIDValue));

			/****************************************
			 *  use offerID
			 ****************************************/

			return "_" + journeyID.hashCode();    
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
				Journey journey = new Journey(guiManagedObject.getJSONRepresentation(), guiManagedObject.getGUIManagedObjectType(), guiManagedObject.getEpoch(), guiManagedObject, catalogCharacteristicService, subscriberMessageTemplateService, dynamicEventDeclarationsService, communicationChannelService);

				//
				//  flat fields
				//

				documentMap.put("journeyID", journey.getJourneyID());
				documentMap.put("journeyName", journey.getGUIManagedObjectDisplay());
				documentMap.put("journeyActive", journey.getActive());

			} catch (GUIManagerException e) {
			}

			//
			//  return
			//

			return documentMap;
		}
	}
}
