package com.evolving.nglm.evolution;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.NGLMRuntime;
import com.evolving.nglm.core.SimpleESSinkConnector;
import com.evolving.nglm.core.StreamESSinkTask;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.EvolutionUtilities.RoundingSelection;
import com.evolving.nglm.evolution.EvolutionUtilities.TimeUnit;
import com.evolving.nglm.evolution.GUIManagedObject.GUIManagedObjectType;
import com.evolving.nglm.evolution.PurchaseFulfillmentManager.PurchaseFulfillmentRequest;
import com.evolving.nglm.evolution.datacubes.DatacubeGenerator;

public class JourneyObjectiveESSinkConnector extends SimpleESSinkConnector
{
  private static DynamicCriterionFieldService dynamicCriterionFieldService;
  private static ContactPolicyService contactPolicyService;
  private static JourneyObjectiveService journeyObjectiveService;
  
  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  @Override public Class<JourneyObjectiveESSinkConnectorTask> taskClass()
  {
    return JourneyObjectiveESSinkConnectorTask.class;
  }

  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  public static class JourneyObjectiveESSinkConnectorTask extends StreamESSinkTask<JourneyObjective>
  {
    private static String elasticSearchDateFormat = Deployment.getElasticSearchDateFormat();
    private DateFormat dateFormat = new SimpleDateFormat(elasticSearchDateFormat);

    /*****************************************
    *
    *  start
    *
    *****************************************/

    @Override public void start(Map<String, String> taskConfig)
    {
      //
      //  super
      //

      super.start(taskConfig);
    
      //
      //  services
      //
   
      dynamicCriterionFieldService = new DynamicCriterionFieldService(Deployment.getBrokerServers(), "odrsinkconnector-dynamiccriterionfieldservice-" + getTaskNumber(), Deployment.getDynamicCriterionFieldTopic(), false);
      CriterionContext.initialize(dynamicCriterionFieldService);
      dynamicCriterionFieldService.start();      
      
      contactPolicyService = new ContactPolicyService(Deployment.getBrokerServers(), "journeyobjectivesinkconnector-contactpolicyservice-" + Integer.toHexString((new Random()).nextInt(1000000000)), Deployment.getContactPolicyTopic(), false);
      contactPolicyService.start();
      journeyObjectiveService = new JourneyObjectiveService(Deployment.getBrokerServers(), "journeyobjectivesinkconnector-journeyObjectiveServiceservice-" + Integer.toHexString((new Random()).nextInt(1000000000)), Deployment.getJourneyObjectiveTopic(), false);
      journeyObjectiveService.start();
    }

    /*****************************************
    *
    *  stop
    *
    *****************************************/

    @Override public void stop()
    {
      //
      //  services
      //

      contactPolicyService.stop();
      journeyObjectiveService.stop();
      
      //
      //  super
      //

      super.stop();
    }

    /*****************************************
    *
    *  unpackRecord
    *
    *****************************************/
    
    @Override public JourneyObjective unpackRecord(SinkRecord sinkRecord) 
    {
      Object journeyObjectiveValue = sinkRecord.value();
      Schema journeyObjectiveValueSchema = sinkRecord.valueSchema();
      return JourneyObjective.unpack(new SchemaAndValue(JourneyObjective.schema(), ((Struct) journeyObjectiveValue).get("journey_objective"))); 
    }
    
    
    /*****************************************
    *
    *  getDocumentMap
    *
    *****************************************/
    
    @Override
    public Map<String, Object> getDocumentMap(JourneyObjective journeyObjective)
    {
      Date now = SystemTime.getCurrentTime();
      ContactPolicy contactPolicy = contactPolicyService.getActiveContactPolicy(journeyObjective.getContactPolicyID(), now);
      
      Map<String,Object> documentMap = new HashMap<String,Object>();
      documentMap.put("id", journeyObjective.getJourneyObjectiveID());
      documentMap.put("display", journeyObjective.getGUIManagedObjectDisplay());
      documentMap.put("contactPolicy", (contactPolicy == null)? "" : contactPolicy.getGUIManagedObjectDisplay());
      documentMap.put("timestamp", DatacubeGenerator.TIMESTAMP_FORMAT.format(SystemTime.getCurrentTime()));
      
      JSONObject jr = journeyObjective.getJSONRepresentation();
      documentMap.put("jr", jr);
      
      documentMap.put("display from JR",jr.get("display"));
      documentMap.put("contactPolicyID from JR",jr.get("contactPolicyID"));
      documentMap.put("id from JR",jr.get("id"));
      
      JSONObject journeyObjectiveJSON = journeyObjectiveService.generateResponseJSON(journeyObjective, true, now);
      documentMap.put("rj", journeyObjectiveJSON.toString());
      
      documentMap.put("getJourneyObjectiveID", journeyObjective.getJourneyObjectiveID());
      documentMap.put("getJourneyObjectiveName", journeyObjective.getJourneyObjectiveName());
      documentMap.put("getParentJourneyObjectiveID", journeyObjective.getParentJourneyObjectiveID());
      documentMap.put("getTargetingLimitMaxSimultaneous", journeyObjective.getTargetingLimitMaxSimultaneous());
      documentMap.put("getTargetingLimitWaitingPeriodDuration", journeyObjective.getTargetingLimitWaitingPeriodDuration());
      documentMap.put("getTargetingLimitWaitingPeriodTimeUnit", journeyObjective.getTargetingLimitWaitingPeriodTimeUnit());
      documentMap.put("getTargetingLimitMaxOccurrence", journeyObjective.getTargetingLimitMaxOccurrence());
      documentMap.put("getTargetingLimitSlidingWindowDuration", journeyObjective.getTargetingLimitSlidingWindowDuration());
      documentMap.put("getTargetingLimitSlidingWindowTimeUnit", journeyObjective.getTargetingLimitSlidingWindowTimeUnit());
      documentMap.put("getContactPolicyID", journeyObjective.getContactPolicyID());
      documentMap.put("getCatalogCharacteristics", journeyObjective.getCatalogCharacteristics());
      documentMap.put("getEffectiveTargetingLimitMaxSimultaneous", journeyObjective.getEffectiveTargetingLimitMaxSimultaneous());
      documentMap.put("getEffectiveWaitingPeriodEndDate", journeyObjective.getEffectiveWaitingPeriodEndDate(now));
      documentMap.put("getEffectiveTargetingLimitMaxOccurrence", journeyObjective.getEffectiveTargetingLimitMaxOccurrence());
      documentMap.put("getEffectiveSlidingWindowStartDate", journeyObjective.getEffectiveSlidingWindowStartDate(now));

      documentMap.put("getGUIManagedObjectID", journeyObjective.getGUIManagedObjectID());
      documentMap.put("getGUIManagedObjectName", journeyObjective.getGUIManagedObjectName());
      documentMap.put("getGUIManagedObjectDisplay", journeyObjective.getGUIManagedObjectDisplay());
      documentMap.put("getGUIManagedObjectType", journeyObjective.getGUIManagedObjectType());
      documentMap.put("getEpoch", journeyObjective.getEpoch());
      documentMap.put("getEffectiveStartDate", journeyObjective.getEffectiveStartDate());
      documentMap.put("getEffectiveEndDate", journeyObjective.getEffectiveEndDate());
      documentMap.put("getUserID", journeyObjective.getUserID());
      documentMap.put("getUserName", journeyObjective.getUserName());
      documentMap.put("getCreatedDate", journeyObjective.getCreatedDate());
      documentMap.put("getUpdatedDate", journeyObjective.getUpdatedDate());
      
      return documentMap;
    }
  }
}

