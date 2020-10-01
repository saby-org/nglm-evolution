package com.evolving.nglm.evolution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

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
  
  public static class SegmentationDimensionESSinkConnectorTask extends StreamESSinkTask<SegmentationDimension>
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
      documentMap.put("id", segmentationDimension.getGUIManagedObjectID());
      documentMap.put("display", segmentationDimension.getGUIManagedObjectDisplay());
      documentMap.put("targetingType", segmentationDimension.getTargetingType().name());
      documentMap.put("createdDate", segmentationDimension.getCreatedDate());
      documentMap.put("active", segmentationDimension.getActive());
      List<Map<String,String>> segments = new ArrayList<>();
      for (Segment segment : segmentationDimension.getSegments())
        {
          Map<String,String> segmentMap = new HashMap<>();
          segmentMap.put("id", segment.getID());
          segmentMap.put("name", segment.getName());
          segments.add(segmentMap);
        }
      documentMap.put("segments", segments);
      documentMap.put("timestamp", DatacubeGenerator.TIMESTAMP_FORMAT.format(SystemTime.getCurrentTime()));
      return documentMap;
    }
  }
}

