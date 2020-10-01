package com.evolving.nglm.evolution;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import com.evolving.nglm.core.SimpleESSinkConnector;
import com.evolving.nglm.core.StreamESSinkTask;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.PurchaseFulfillmentManager.PurchaseFulfillmentRequest;
import com.evolving.nglm.evolution.datacubes.DatacubeGenerator;

public class JourneyObjectiveESSinkConnector extends SimpleESSinkConnector
{
  private static DynamicCriterionFieldService dynamicCriterionFieldService;
  private static ContactPolicyService contactPolicyService;
  
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
      return JourneyObjective.unpack(new SchemaAndValue(journeyObjectiveValueSchema, ((Struct) journeyObjectiveValue).get("journey_objective"))); 
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
      return documentMap;
    }
  }
}

