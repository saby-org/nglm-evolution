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
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.GUIManager.GUIManagerException;
import com.evolving.nglm.evolution.datacubes.DatacubeGenerator;

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
    private JourneyService journeyService;
    private CatalogCharacteristicService catalogCharacteristicService;
    private SubscriberMessageTemplateService subscriberMessageTemplateService;
    private DynamicEventDeclarationsService dynamicEventDeclarationsService;
    private CommunicationChannelService communicationChannelService;
    private TargetService targetService;
    private JourneyObjectiveService journeyObjectiveService;

    public OfferESSinkTask()
    {
      journeyService = new JourneyService(Deployment.getBrokerServers(), "journeyessinkconnector-journeyservice-" + getTaskNumber(), Deployment.getJourneyTopic(), false);
      catalogCharacteristicService = new CatalogCharacteristicService(Deployment.getBrokerServers(), "journeyessinkconnector-catalogcharacteristicservice-" + getTaskNumber(), Deployment.getCatalogCharacteristicTopic(), false);
      subscriberMessageTemplateService = new SubscriberMessageTemplateService(Deployment.getBrokerServers(), "journeyessinkconnector-subscriberMessageTemplateService-" + getTaskNumber(), Deployment.getSubscriberMessageTemplateTopic(), false);
      dynamicEventDeclarationsService = new DynamicEventDeclarationsService(Deployment.getBrokerServers(), "journeyessinkconnector-dynamicEventDeclarationsService-" + getTaskNumber(), Deployment.getDynamicEventDeclarationsTopic(), false);
      communicationChannelService = new CommunicationChannelService(Deployment.getBrokerServers(), "journeyessinkconnector-communicationChannelService-" + getTaskNumber(), Deployment.getCommunicationChannelTopic(), false);
      targetService = new TargetService(Deployment.getBrokerServers(), "journeyessinkconnector-targetservice-" + getTaskNumber(), Deployment.getTargetTopic(), false);
      journeyObjectiveService = new JourneyObjectiveService(Deployment.getBrokerServers(), "journeyessinkconnector-journeyobjectiveservice-" + getTaskNumber(), Deployment.getJourneyObjectiveTopic(), false);
      
      journeyService.start();
      catalogCharacteristicService.start();
      subscriberMessageTemplateService.start();
      dynamicEventDeclarationsService.start();
      communicationChannelService.start();
      targetService.start();
      journeyObjectiveService.start();
    }

    @Override public String getDocumentID(SinkRecord sinkRecord)
    {
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


      try
        {
          Journey journey = new Journey(guiManagedObject.getJSONRepresentation(), guiManagedObject.getGUIManagedObjectType(), guiManagedObject.getEpoch(), guiManagedObject, journeyService, catalogCharacteristicService, subscriberMessageTemplateService, dynamicEventDeclarationsService, communicationChannelService);
          
          //
          // description: retrieved from JSON, not in the object
          //
          Object description = journey.getJSONRepresentation().get("description");
          
          //
          // targets
          //
          String targets = "";
          for(String targetID : journey.getTargetID()) {
            GUIManagedObject target = targetService.getStoredGUIManagedObject(targetID);
            if(target != null) {
              String targetDisplay = target.getGUIManagedObjectDisplay();
              if(targetDisplay == null) {
                targetDisplay = "Unknown(ID:" + targetID + ")";
              }
              
              if(targets.equals("")) {
                targets = targetDisplay;
              } else {
                targets += "/" + targetDisplay;
              }
            }
          }
          
          //
          // objectives
          //
          String objectives = "";
          for(JourneyObjectiveInstance objectiveInstance : journey.getJourneyObjectiveInstances()) {
            GUIManagedObject journeyObjective = journeyObjectiveService.getStoredGUIManagedObject(objectiveInstance.getJourneyObjectiveID());
            if(journeyObjective != null) {
              String journeyObjectiveDisplay = journeyObjective.getGUIManagedObjectDisplay();
              if(journeyObjectiveDisplay == null) {
                journeyObjectiveDisplay = "Unknown(ID:" + objectiveInstance.getJourneyObjectiveID() + ")";
              }
              
              if(objectives.equals("")) {
                objectives = journeyObjectiveDisplay;
              } else {
                objectives += "/" + journeyObjectiveDisplay;
              }
            }
          }
          
          //
          // targetCount 
          //
          long targetCount = 0;
          
          /** TODO: @rl does not have acces to RestHighLevelClient here
           * maybe do this in datacubemanager ?
           *
          if(journey.getTargetingType() == TargetingType.Target) {
            try {
              BoolQueryBuilder query = Journey.processEvaluateProfileCriteriaGetQuery(journey.getTargetingCriteria());
              targetCount = Journey.processEvaluateProfileCriteriaExecuteQuery(query, elasticsearch);
            } catch (CriterionException|IOException e) {
              StringWriter stackTraceWriter = new StringWriter();
              e.printStackTrace(new PrintWriter(stackTraceWriter, true));
              log.warn("Journey sink connector processEvaluateProfileCriteria exception {}", stackTraceWriter.toString());
            }
          }*/
          
          documentMap.put("journeyID", journey.getJourneyID());
          documentMap.put("display", journey.getGUIManagedObjectDisplay());
          documentMap.put("description", (description != null)? description: "");
          documentMap.put("type", journey.getGUIManagedObjectType().getExternalRepresentation());
          documentMap.put("user", journey.getUserName());
          documentMap.put("targets", targets);
          documentMap.put("targetCount", targetCount);
          documentMap.put("objectives", objectives);
          documentMap.put("startDate", DatacubeGenerator.TIMESTAMP_FORMAT.format(journey.getEffectiveStartDate()));
          documentMap.put("endDate", DatacubeGenerator.TIMESTAMP_FORMAT.format(journey.getEffectiveEndDate()));
          documentMap.put("active", journey.getActive());
          documentMap.put("timestamp", DatacubeGenerator.TIMESTAMP_FORMAT.format(SystemTime.getCurrentTime())); // @rl: TODO TIMESTAMP_FORMAT in more generic class ? Elasticsearch client ?

        }
      catch (GUIManagerException e)
        {
        }

      //
      //  return
      //

      return documentMap;
    }
  }
}
