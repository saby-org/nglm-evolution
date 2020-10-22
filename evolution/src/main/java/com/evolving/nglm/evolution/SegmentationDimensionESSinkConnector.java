package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import com.evolving.nglm.core.ChangeLogESSinkTask;
import com.evolving.nglm.core.RLMDateUtils;
import com.evolving.nglm.core.SimpleESSinkConnector;
import com.evolving.nglm.core.StreamESSinkTask;
import com.evolving.nglm.core.SystemTime;
import com.evolving.nglm.evolution.datacubes.DatacubeGenerator;

public class SegmentationDimensionESSinkConnector extends SimpleESSinkConnector
{
  
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
      SegmentationDimension result = null;
      Object segmentationDimensionValue = sinkRecord.value();
      Schema segmentationDimensionValueSchema = sinkRecord.valueSchema();
      Struct struct = (Struct) ((Struct) segmentationDimensionValue).get("segmentation_dimension_eligibility");
      if (struct != null)
        {
          result = SegmentationDimensionEligibility.unpack(new SchemaAndValue(SegmentationDimensionEligibility.schema(), struct));
        }
      else {
        struct = (Struct) ((Struct) segmentationDimensionValue).get("segmentation_dimension_file_import");
        if (struct != null)
          {
            result = SegmentationDimensionFileImport.unpack(new SchemaAndValue(SegmentationDimensionFileImport.schema(), struct));
          }
        else {
          struct = (Struct) ((Struct) segmentationDimensionValue).get("segmentation_dimension_ranges");
          if (struct != null)
            {
              result = SegmentationDimensionRanges.unpack(new SchemaAndValue(SegmentationDimensionRanges.schema(), struct));
            }
        }
      }
      return result;
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
      // because native data in object is sometimes not correct
      
      JSONObject jr = segmentationDimension.getJSONRepresentation();
      documentMap.put("id",            jr.get("id"));
      documentMap.put("display",       jr.get("display"));
      documentMap.put("targetingType", jr.get("targetingType"));
      documentMap.put("active",        jr.get("active"));
      documentMap.put("createdDate",   GUIManagedObject.parseDateField((String) jr.get("createdDate")));
      JSONArray segmentsJSON = (JSONArray) jr.get("segments");
      List<Map<String,String>> segments = new ArrayList<>();
      for (int i = 0; i < segmentsJSON.size(); i++)
        {
          JSONObject segmentJSON = (JSONObject) segmentsJSON.get(i);
          Map<String,String> segmentMap = new HashMap<>();
          segmentMap.put("id", (String) segmentJSON.get("id"));
          segmentMap.put("name", (String) segmentJSON.get("name"));
          segments.add(segmentMap);
        }
      documentMap.put("segments",  segments);
      documentMap.put("timestamp", RLMDateUtils.printTimestamp(SystemTime.getCurrentTime()));
      return documentMap;
    }

    @Override
    public String getDocumentID(SegmentationDimension segmentationDimension)
    {
      return segmentationDimension.getSegmentationDimensionID();
    }
  }
}

