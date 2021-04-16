package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ChangeLogESSinkTask;
import com.evolving.nglm.core.Deployment;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SimpleESSinkConnector;
import com.evolving.nglm.core.SystemTime;

@Deprecated
public class SegmentationDimensionESSinkConnector extends SimpleESSinkConnector
{
  private static DynamicCriterionFieldService dynamicCriterionFieldService;

  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  @Override public Class<SegmentationDimensionESSinkConnectorTask> taskClass()
  {
    return SegmentationDimensionESSinkConnectorTask.class;
  }

  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  @Deprecated
  public static class SegmentationDimensionESSinkConnectorTask extends ChangeLogESSinkTask<SegmentationDimension>
  {

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
   
      dynamicCriterionFieldService = new DynamicCriterionFieldService(Deployment.getBrokerServers(), "segmentationdimensionsinkconnector-dynamiccriterionfieldservice-" + getTaskNumber(), Deployment.getDynamicCriterionFieldTopic(), false);
      CriterionContext.initialize(dynamicCriterionFieldService);
      dynamicCriterionFieldService.start();      
    }

    /*****************************************
    *
    *  stop
    *
    *****************************************/

    @Override public void stop()
    {
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
    
    @Override public SegmentationDimension unpackRecord(SinkRecord sinkRecord) 
    {
      Object guiManagedObjectValue = sinkRecord.value();
      Schema guiManagedObjectValueSchema = sinkRecord.valueSchema();
      GUIManagedObject guiManagedObject = GUIManagedObject.commonSerde().unpack(new SchemaAndValue(guiManagedObjectValueSchema, guiManagedObjectValue));
      if (guiManagedObject instanceof SegmentationDimensionEligibility)
        {
          return (SegmentationDimensionEligibility) guiManagedObject;
        }
      else if (guiManagedObject instanceof SegmentationDimensionFileImport)
        {
          return (SegmentationDimensionFileImport) guiManagedObject;
        }
      else if (guiManagedObject instanceof SegmentationDimensionRanges)
        {
          return (SegmentationDimensionRanges) guiManagedObject;
        }
      else
        {
          return null;
        }
    }
    
    /*****************************************
    *
    *  getDocumentMap
    *
    *****************************************/
    
    @Override
    public Map<String, Object> getDocumentMap(SegmentationDimension segmentationDimension)
    {
      Map<String,Object> documentMap = new HashMap<String,Object>();
      
      // We read all data from JSONRepresentation()
      // because native data in object is sometimes not correct @rl ? - THIS NEEDS EXPLAINATION
      
      JSONObject jr = segmentationDimension.getJSONRepresentation();
      if (jr != null)
        {
          documentMap.put("id",            jr.get("id"));
          documentMap.put("display",       jr.get("display"));
          documentMap.put("targetingType", jr.get("targetingType"));
          documentMap.put("active",        jr.get("active"));
          documentMap.put("createdDate",   RLMDateUtils.formatDateForElasticsearchDefault(GUIManagedObject.parseDateField((String) jr.get("createdDate")))); // @rl - Ugly
          JSONArray segmentsJSON = (JSONArray) jr.get("segments");
          List<Map<String,String>> segments = new ArrayList<>();
          if (segmentsJSON != null)
            {
              for (int i = 0; i < segmentsJSON.size(); i++)
                {
                  JSONObject segmentJSON = (JSONObject) segmentsJSON.get(i);
                  Map<String,String> segmentMap = new HashMap<>();
                  segmentMap.put("id", (String) segmentJSON.get("id"));
                  segmentMap.put("name", (String) segmentJSON.get("name"));
                  segments.add(segmentMap);
                }
            }
          documentMap.put("segments",  segments);
          documentMap.put("timestamp", RLMDateUtils.formatDateForElasticsearchDefault(SystemTime.getCurrentTime()));
        }
      return documentMap;
    }

    @Override
    public String getDocumentID(SegmentationDimension segmentationDimension)
    {
      return segmentationDimension.getSegmentationDimensionID();
    }
  }
}

