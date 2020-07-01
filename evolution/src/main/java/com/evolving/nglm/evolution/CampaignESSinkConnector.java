/****************************************************************************
*
*  CampaignESSinkConnector.java
*
****************************************************************************/

package com.evolving.nglm.evolution;

import com.evolving.nglm.core.SimpleESSinkConnector;
import com.evolving.nglm.core.StreamESSinkTask;
import com.evolving.nglm.evolution.GUIManagedObject.GUIManagedObjectType;
   
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.HashMap;
import java.util.Map;

public class CampaignESSinkConnector extends SimpleESSinkConnector
{
  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  @Override public Class<? extends Task> taskClass()
  {
    return CampaignESSinkTask.class;
  }

  /****************************************
  *
  *  taskClass
  *
  ****************************************/
  
  public static class CampaignESSinkTask extends StreamESSinkTask<Journey>
  {
    @Override public Journey unpackRecord(SinkRecord sinkRecord) 
    {
      Object GUIManagedObjectValue = sinkRecord.value();
      Schema GUIManagedObjectValueSchema = sinkRecord.valueSchema();
      GUIManagedObject guiManagedObject = GUIManagedObject.commonSerde().unpack(new SchemaAndValue(GUIManagedObjectValueSchema, GUIManagedObjectValue));

      // Filter by type
      GUIManagedObjectType guiManagedObjectType = guiManagedObject.getGUIManagedObjectType();
      if(guiManagedObjectType != GUIManagedObjectType.Campaign || !(guiManagedObject.getAccepted())) {
        return null;
      }
      
      return (Journey) guiManagedObject;
    }
    
    @Override public Map<String,Object> getDocumentMap(Journey journey)
    {
      Map<String,Object> documentMap = new HashMap<String,Object>();
      documentMap.put("campaignName", journey.getJourneyName());
      documentMap.put("campaignID", journey.getJourneyID());
      documentMap.put("startDate", journey.getEffectiveStartDate());
      documentMap.put("endDate", journey.getEffectiveEndDate());

      return documentMap;
    }    
  }
}
